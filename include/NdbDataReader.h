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

#include "Cache.h"
#include "Utils.h"
#include "ProjectDatasetINodeCache.h"
#include "ElasticSearchBase.h"

using namespace Utils;
using namespace Utils::NdbC;

struct BatchStats{
    float mNdbReadTime;
    float mJSONCreationTime;
    float mElasticSearchTime;
    
    string str(){
        stringstream out;
        out << "[Ndb=" << mNdbReadTime << " msec, JSON=" << mJSONCreationTime << " msec, ES=" << mElasticSearchTime << " msec]";
        return out.str();
    }
};

template<typename Data, typename Conn, typename Keys>
class NdbDataReader {
public:
    NdbDataReader(Conn* connections, const int num_readers, const bool hopsworks, 
            ElasticSearchBase<Keys>* elastic, ProjectDatasetINodeCache* cache);
    void start();
    void processBatch(vector<Data>* data_batch);
    virtual ~NdbDataReader();

private:
    bool mStarted;
    std::vector<boost::thread* > mThreads;
    
    ConcurrentQueue<vector<Data>*>* mBatchedQueue;
    
    void readerThread(int connectionId);
    void readData(int connIndex, Conn connection, vector<Data>* data_batch);

protected:
    const int mNumReaders;
    Conn* mNdbConnections;
    const bool mHopsworksEnalbed;
    ElasticSearchBase<Keys>* mElasticSearch;
    ProjectDatasetINodeCache* mPDICache;
    
    virtual void processAddedandDeleted(Conn conn, vector<Data>* data_batch, Bulk<Keys>& bulk) = 0;
};


template<typename Data, typename Conn, typename Keys>
NdbDataReader<Data, Conn, Keys>::NdbDataReader(Conn* connections, const int num_readers, 
        const bool hopsworks,ElasticSearchBase<Keys>* elastic, ProjectDatasetINodeCache* cache) 
        :  mNumReaders(num_readers), mNdbConnections(connections), 
        mHopsworksEnalbed(hopsworks), mElasticSearch(elastic), mPDICache(cache){
    mStarted = false;
    mBatchedQueue = new ConcurrentQueue<vector<Data>*>();
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReader<Data, Conn, Keys>::start() {
    if (mStarted) {
        return;
    }
    
    for(int i=0; i< mNumReaders; i++){
        boost::thread* th = new boost::thread(&NdbDataReader::readerThread, this, i);
        LOG_DEBUG(" Reader Thread [" << th->get_id() << "] created"); 
        mThreads.push_back(th);
    }
    mStarted = true;
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReader<Data, Conn, Keys>::readerThread(int connIndex) {
    while(true){
        vector<Data>* curr;
        mBatchedQueue->wait_and_pop(curr);
        LOG_DEBUG(" Process Batch ");
        readData(connIndex, mNdbConnections[connIndex], curr);
        
        delete curr;
    }
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReader<Data, Conn, Keys>::readData(int connIndex, Conn connection, vector<Data>* data_batch) {
    Bulk<Keys> bulk;
    bulk.mStartProcessing = getCurrentTime();
    
    if (!data_batch->empty()) {
        processAddedandDeleted(connection, data_batch, bulk);
    }
    
    bulk.mEndProcessing = getCurrentTime();
    
    if (!bulk.mJSON.empty()) {
        sort(bulk.mArrivalTimes.begin(), bulk.mArrivalTimes.end());
        mElasticSearch->addData(bulk);
    }
    
    LOG_INFO("[thread-" << connIndex << "] processing batch of size [" << data_batch->size() << "] took " 
            << getTimeDiffInMilliseconds(bulk.mStartProcessing, bulk.mEndProcessing) << " msec");
}


template<typename Data, typename Conn, typename Keys>
void NdbDataReader<Data, Conn, Keys>::processBatch(vector<Data>* data_batch) {
    mBatchedQueue->push(data_batch);
}

template<typename Data, typename Conn, typename Keys>
NdbDataReader<Data, Conn, Keys>::~NdbDataReader() {
    delete[] mNdbConnections;
    delete mBatchedQueue;
}

#endif /* NDBDATAREADER_H */

