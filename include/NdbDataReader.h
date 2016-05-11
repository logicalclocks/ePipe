/*
 * Copyright (C) 2016 Hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */


/* 
 * File:   NdbDataReader.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef NDBDATAREADER_H
#define NDBDATAREADER_H

#include "FsMutationsTableTailer.h"
#include "ConcurrentQueue.h"
#include "vector"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include <boost/network.hpp>

typedef boost::network::http::client httpclient;

using namespace Utils;

template<typename Data>
class NdbDataReader {
public:
    NdbDataReader(Ndb** connections, const int num_readers, string elastic_addr);
    void start();
    void processBatch(Data data_batch);
    virtual ~NdbDataReader();

protected:
    virtual void readData(Ndb* connection, Data data_batch) = 0;
    string bulkUpdateElasticSearch(string json);
        
private:
    Ndb** mNdbConnections;
    const int mNumReaders;
    const string mElasticAddr;
    string mElasticBulkUrl;
    
    bool mStarted;
    std::vector<boost::thread* > mThreads;
    
    ConcurrentQueue<Data>* mBatchedQueue;
    
    void readerThread(int connectionId);
};


template<typename Data>
NdbDataReader<Data>::NdbDataReader(Ndb** connections, const int num_readers, 
        string elastic_ip) : mNdbConnections(connections), mNumReaders(num_readers), mElasticAddr(elastic_ip){
    mStarted = false;
    mElasticBulkUrl = "http://" + mElasticAddr + "/_bulk";
    mBatchedQueue = new ConcurrentQueue<Data>();
}

template<typename Data>
string NdbDataReader<Data>::bulkUpdateElasticSearch(string json){
    
    httpclient::request request_(mElasticBulkUrl);
    request_ << boost::network::header("Connection", "close");
    request_ << boost::network::header("Content-Type", "application/json");
    
    char body_str_len[8];
    sprintf(body_str_len, "%lu", json.length());

    request_ << boost::network::header("Content-Length", body_str_len);
    request_ << boost::network::body(json);
    
    httpclient client_;
    httpclient::response response_ = client_.post(request_);
    std::string body_ = boost::network::http::body(response_);
    return body_;
}

template<typename Data>
void NdbDataReader<Data>::start() {
    if (mStarted) {
        return;
    }
    
    for(int i=0; i< mNumReaders; i++){
        boost::thread* th = new boost::thread(&NdbDataReader::readerThread, this, i);
        LOG_DEBUG() << " Reader Thread [" << th->get_id() << "] created"; 
        mThreads.push_back(th);
    }
    mStarted = true;
}

template<typename Data>
void NdbDataReader<Data>::readerThread(int connIndex) {
    while(true){
        Data curr;
        mBatchedQueue->wait_and_pop(curr);
        LOG_DEBUG() << " Process Batch ";
        boost::posix_time::ptime t1 = boost::posix_time::microsec_clock::local_time();
        readData(mNdbConnections[connIndex], curr);
        boost::posix_time::ptime t2 = boost::posix_time::microsec_clock::local_time();
        boost::posix_time::time_duration elaspsed = t2 - t1;
        LOG_DEBUG() << " Process Batch took " << elaspsed.total_milliseconds() << " msec ";
    }
}

template<typename Data>
void NdbDataReader<Data>::processBatch(Data data_batch) {
    mBatchedQueue->push(data_batch);
}

template<typename Data>
NdbDataReader<Data>::~NdbDataReader() {
    for(int i=0; i< mNumReaders; i++){
        delete mNdbConnections[i];
    }
    delete[] mNdbConnections;
    delete mBatchedQueue;
}

#endif /* NDBDATAREADER_H */

