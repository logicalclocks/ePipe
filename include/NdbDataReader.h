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

typedef boost::unordered_set<int> UISet;
typedef vector<NdbRecAttr*> Row;
typedef boost::unordered_map<int, Row> UIRowMap;

using namespace Utils;

struct ReadTimes{
    float mNdbReadTime;
    float mJSONCreationTime;
    float mElasticSearchTime;
};

template<typename Data>
class NdbDataReader {
public:
    NdbDataReader(Ndb** connections, const int num_readers, string elastic_addr,
            const bool hopsworks, const string elastic_index, const string elastic_inode_type);
    void start();
    void processBatch(Data data_batch);
    virtual ~NdbDataReader();

private:
    Ndb** mNdbConnections;
    const int mNumReaders;
    const string mElasticAddr;
    string mElasticBulkUrl;
    
    bool mStarted;
    std::vector<boost::thread* > mThreads;
    
    ConcurrentQueue<Data>* mBatchedQueue;
    
    void readerThread(int connectionId);
    
protected:
    virtual ReadTimes readData(Ndb* connection, Data data_batch) = 0;
    string bulkUpdateElasticSearch(string json);
    UIRowMap readTableWithIntPK(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, const char* table_name, 
            UISet ids, const char** columns_to_read, const int columns_count, const int column_pk_index);
    const bool mHopsworksEnalbed;
    const string mElasticIndex;
    const string mElasticInodeType;
    
};


template<typename Data>
NdbDataReader<Data>::NdbDataReader(Ndb** connections, const int num_readers, 
        string elastic_ip, const bool hopsworks, const string elastic_index, 
        const string elastic_inode_type) : mNdbConnections(connections), mNumReaders(num_readers), 
        mElasticAddr(elastic_ip), mHopsworksEnalbed(hopsworks), mElasticIndex(elastic_index), mElasticInodeType(elastic_inode_type){
    mStarted = false;
    mElasticBulkUrl = getElasticSearchBulkUrl(mElasticAddr);
    mBatchedQueue = new ConcurrentQueue<Data>();
}

template<typename Data>
string NdbDataReader<Data>::bulkUpdateElasticSearch(string json) {
    return elasticSearchPOST(mElasticBulkUrl, json);
}

template<typename Data>
UIRowMap NdbDataReader<Data>::readTableWithIntPK(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, 
        const char* table_name,  UISet ids, const char** columns_to_read, const int columns_count, const int column_pk_index) {
    
    UIRowMap res;
    const NdbDictionary::Table* table = getTable(database, table_name);
    
    for(UISet::iterator it = ids.begin(); it != ids.end(); ++it){
        NdbOperation* op = getNdbOperation(transaction, table);
        op->readTuple(NdbOperation::LM_CommittedRead);
        op->equal(columns_to_read[column_pk_index], *it);
        
        for(int c=0; c<columns_count; c++){
            NdbRecAttr* col =getNdbOperationValue(op, columns_to_read[c]);
            res[*it].push_back(col);
        }
        LOG_TRACE() << " Read " << table_name << " row for [" << *it<< "]"; 
    }
    return res;
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
        ReadTimes rt = readData(mNdbConnections[connIndex], curr);
        LOG_DEBUG() << " Process Batch time taken [ Ndb = " << rt.mNdbReadTime 
                << " msec, JSON = " << rt.mJSONCreationTime << " msec, ES = " << rt.mElasticSearchTime << " msec ]";
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

