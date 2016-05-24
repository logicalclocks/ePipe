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

#include "ConcurrentQueue.h"
#include "Cache.h"
#include "Utils.h"
#include "ProjectDatasetINodeCache.h"

using namespace Utils;
using namespace Utils::ElasticSearch;
using namespace Utils::NdbC;

struct ReadTimes{
    float mNdbReadTime;
    float mJSONCreationTime;
    float mElasticSearchTime;
};

struct MConn{
    Ndb* inodeConnection;
    Ndb* metadataConnection;
};

template<typename Data, typename Conn>
class NdbDataReader {
public:
    NdbDataReader(Conn* connections, const int num_readers, string elastic_addr,
            const bool hopsworks, const string elastic_index, const string elastic_inode_type,
            ProjectDatasetINodeCache* cache);
    void start();
    void processBatch(vector<Data>* data_batch);
    virtual ~NdbDataReader();

private:
    const string mElasticAddr;
    string mElasticBulkUrl;
    
    bool mStarted;
    std::vector<boost::thread* > mThreads;
    
    ConcurrentQueue<vector<Data>*>* mBatchedQueue;
    
    void readerThread(int connectionId);
    
protected:
    const int mNumReaders;
    Conn* mNdbConnections;
    const bool mHopsworksEnalbed;
    const string mElasticIndex;
    const string mElasticInodeType;
    ProjectDatasetINodeCache* mPDICache;
    
    virtual ReadTimes readData(Conn connection, vector<Data>* data_batch) = 0;
    string bulkUpdateElasticSearch(string json);
};


template<typename Data, typename Conn>
NdbDataReader<Data, Conn>::NdbDataReader(Conn* connections, const int num_readers, 
        string elastic_ip, const bool hopsworks, const string elastic_index, 
        const string elastic_inode_type, ProjectDatasetINodeCache* cache) 
        :  mElasticAddr(elastic_ip), mNumReaders(num_readers), mNdbConnections(connections), 
        mHopsworksEnalbed(hopsworks), mElasticIndex(elastic_index), mElasticInodeType(elastic_inode_type),
        mPDICache(cache){
    mStarted = false;
    mElasticBulkUrl = getElasticSearchBulkUrl(mElasticAddr);
    mBatchedQueue = new ConcurrentQueue<vector<Data>*>();
}

template<typename Data, typename Conn>
string NdbDataReader<Data, Conn>::bulkUpdateElasticSearch(string json) {
    //TODO: handle elasticsearch failures
    return elasticSearchPOST(mElasticBulkUrl, json);
}

template<typename Data, typename Conn>
void NdbDataReader<Data, Conn>::start() {
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

template<typename Data, typename Conn>
void NdbDataReader<Data, Conn>::readerThread(int connIndex) {
    while(true){
        vector<Data>* curr;
        mBatchedQueue->wait_and_pop(curr);
        LOG_DEBUG() << " Process Batch ";
        ReadTimes rt = readData(mNdbConnections[connIndex], curr);
        LOG_DEBUG() << " Process Batch time taken [ Ndb = " << rt.mNdbReadTime 
                << " msec, JSON = " << rt.mJSONCreationTime << " msec, ES = " << rt.mElasticSearchTime << " msec ]";
        delete curr;
    }
}

template<typename Data, typename Conn>
void NdbDataReader<Data, Conn>::processBatch(vector<Data>* data_batch) {
    mBatchedQueue->push(data_batch);
}

template<typename Data, typename Conn>
NdbDataReader<Data, Conn>::~NdbDataReader() {
    delete[] mNdbConnections;
    delete mBatchedQueue;
}

#endif /* NDBDATAREADER_H */

