/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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

#ifndef NDBDATAREADERS_H
#define NDBDATAREADERS_H

#include "NdbDataReader.h"
#include "ConcurrentPriorityQueue.h"
#include <boost/atomic.hpp>

typedef boost::atomic<Uint64> AtomicLong;

struct BulkIndexComparator {
  
  bool operator()(const eBulk &r1, const eBulk &r2) const {
    return r1.mProcessingIndex > r2.mProcessingIndex;
  }
};

template<typename Data, typename Conn>
class NdbDataReaders : public DataReaderOutHandler{
public:
  typedef std::vector<NdbDataReader<Data, Conn>* > DataReadersVec;
  typedef typename DataReadersVec::size_type drvec_size_type;
  NdbDataReaders(TimedRestBatcher* elastic);
  void start();
  void processBatch(std::vector<Data>* data_batch);
  void writeOutput(eBulk out);
  virtual ~NdbDataReaders();
  
private:
  TimedRestBatcher* timedRestBatcher;
  bool mStarted;
  boost::thread mThread;
  
  ConcurrentQueue<std::vector<Data>*>* mBatchedQueue;
  ConcurrentPriorityQueue<eBulk, BulkIndexComparator >* mWaitingOutQueue;
  
  AtomicLong mLastSent;
  AtomicLong mCurrIndex;
  drvec_size_type mRoundRobinDrIndex;
  
  void run();
  void processWaiting();
  
protected:
  std::vector<NdbDataReader<Data, Conn>* > mDataReaders;
};

template<typename Data, typename Conn>
NdbDataReaders<Data, Conn>::NdbDataReaders(TimedRestBatcher* batcher) : timedRestBatcher(batcher) {
  mStarted = false;
  mBatchedQueue = new ConcurrentQueue<std::vector<Data>*>();
  mWaitingOutQueue = new ConcurrentPriorityQueue<eBulk, BulkIndexComparator>();
  mLastSent = 0; 
  mCurrIndex = 0;
  mRoundRobinDrIndex = -1;
}

template<typename Data, typename Conn>
void NdbDataReaders<Data, Conn>::start() {
  if (mStarted) {
    return;
  }
  
  mThread = boost::thread(&NdbDataReaders::run, this);
  mStarted = true;
}


template<typename Data, typename Conn>
void NdbDataReaders<Data, Conn>::run() {
  while (true) {
    std::vector<Data>* curr;
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

template<typename Data, typename Conn>
void NdbDataReaders<Data, Conn>::processBatch(std::vector<Data>* data_batch) {
  mBatchedQueue->push(data_batch);
}

template<typename Data, typename Conn>
void NdbDataReaders<Data, Conn>::writeOutput(eBulk out) {
  mWaitingOutQueue->push(out);
  processWaiting();
}

template<typename Data, typename Conn>
void NdbDataReaders<Data, Conn>::processWaiting() {
  while (!mWaitingOutQueue->empty()) {
    boost::optional<eBulk> out_ptr = mWaitingOutQueue->pop();
    if (out_ptr) {
      eBulk out = out_ptr.get();
      if (out.mProcessingIndex == mLastSent + 1) {
        LOG_INFO("publish enriched events with index [" << out.mProcessingIndex << "] to Elastic");
        timedRestBatcher->addData(out);
        mLastSent++;
      } else {
        mWaitingOutQueue->push(out);
        break;
      }
    } else {
      break;
    }
  }
}

template<typename Data, typename Conn>
NdbDataReaders<Data, Conn>::~NdbDataReaders() {
  delete mBatchedQueue;
}

#endif /* NDBDATAREADERS_H */

