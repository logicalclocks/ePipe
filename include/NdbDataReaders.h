/*
 * Copyright (C) 2018 Hops.io
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
 * File:   NdbDataReaders.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef NDBDATAREADERS_H
#define NDBDATAREADERS_H

#include "NdbDataReader.h"
#include "ConcurrentPriorityQueue.h"
#include <boost/atomic.hpp>

typedef boost::atomic<Uint64> AtomicLong;

template<typename Keys>
struct BulkIndexComparator {
  
  bool operator()(const Bulk<Keys> &r1, const Bulk<Keys> &r2) const {
    return r1.mProcessingIndex > r2.mProcessingIndex;
  }
};

template<typename Data, typename Conn, typename Keys> 
class NdbDataReaders : public DataReaderOutHandler<Keys>{
public:
  NdbDataReaders(ElasticSearchBase<Keys>* elastic);
  void start();
  void processBatch(vector<Data>* data_batch);
  void writeOutput(Bulk<Keys> out);
  virtual ~NdbDataReaders();
  
private:
  ElasticSearchBase<Keys>* mElasticSearch;
  bool mStarted;
  boost::thread mThread;
  
  ConcurrentQueue<vector<Data>*>* mBatchedQueue;
  ConcurrentPriorityQueue<Bulk<Keys>, BulkIndexComparator<Keys> >* mWaitingOutQueue;
  
  AtomicLong mLastSent;
  AtomicLong mCurrIndex;
  int mRoundRobinDrIndex;
  
  void run();
  void processWaiting();
  
protected:
  vector<NdbDataReader<Data, Conn, Keys>* > mDataReaders;
};

template<typename Data, typename Conn, typename Keys>
NdbDataReaders<Data, Conn, Keys>::NdbDataReaders(ElasticSearchBase<Keys>* elastic) : mElasticSearch(elastic) {
  mStarted = false;
  mBatchedQueue = new ConcurrentQueue<vector<Data>*>();
  mWaitingOutQueue = new ConcurrentPriorityQueue<Bulk<Keys>, BulkIndexComparator<Keys> >();
  mLastSent = 0; 
  mCurrIndex = 0;
  mRoundRobinDrIndex = -1;
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReaders<Data, Conn, Keys>::start() {
  if (mStarted) {
    return;
  }
  
  mThread = boost::thread(&NdbDataReaders::run, this);
  mStarted = true;
}


template<typename Data, typename Conn, typename Keys>
void NdbDataReaders<Data, Conn, Keys>::run() {
  while (true) {
    vector<Data>* curr;
    mBatchedQueue->wait_and_pop(curr);
    
    if(mRoundRobinDrIndex < mDataReaders.size()){
      mRoundRobinDrIndex++;
    }
    
    if(mRoundRobinDrIndex >= mDataReaders.size()){
      mRoundRobinDrIndex = 0;
    }
    
    mDataReaders[mRoundRobinDrIndex]->processBatch(++mCurrIndex, curr);
  }
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReaders<Data, Conn, Keys>::processBatch(vector<Data>* data_batch) {
  mBatchedQueue->push(data_batch);
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReaders<Data, Conn, Keys>::writeOutput(Bulk<Keys> out) {
  mWaitingOutQueue->push(out);
  processWaiting();
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReaders<Data, Conn, Keys>::processWaiting() {
  while(!mWaitingOutQueue->empty()){
    Bulk<Keys> out;
    mWaitingOutQueue->pop(out);
    if(out.mProcessingIndex == mLastSent + 1){
      LOG_INFO("publish enriched events with index ["  << out.mProcessingIndex << "] to Elastic");
      mElasticSearch->addData(out);
      mLastSent++;
    }else{
      mWaitingOutQueue->push(out);
      break;
    }
  }
}

template<typename Data, typename Conn, typename Keys>
NdbDataReaders<Data, Conn, Keys>::~NdbDataReaders() {
  delete mBatchedQueue;
}

#endif /* NDBDATAREADERS_H */

