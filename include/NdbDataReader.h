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

template<typename Data>
class NdbDataReader {
public:
    NdbDataReader(Ndb** connections, const int num_readers);
    void start();
    void processBatch(Data data_batch);
    virtual ~NdbDataReader();

protected:
    virtual void readData(Ndb* connection, Data data_batch) = 0;
    
private:
    Ndb** mNdbConnections;
    const int mNumReaders;

    bool mStarted;
    std::vector<boost::thread* > mThreads;
    
    ConcurrentQueue<Data>* mBatchedQueue;
    
    void readerThread(int connectionId);
};


template<typename Data>
NdbDataReader<Data>::NdbDataReader(Ndb** connections, const int num_readers) : mNdbConnections(connections), mNumReaders(num_readers){
    mStarted = false;
    mBatchedQueue = new ConcurrentQueue<Data>();
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
        readData(mNdbConnections[connIndex], curr);
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

