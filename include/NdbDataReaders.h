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

template<typename Data, typename Conn, typename Keys> 
class NdbDataReaders {
public:
  NdbDataReaders();
  void start();
  void processBatch(vector<Data>* data_batch);
  virtual ~NdbDataReaders();

private:
  bool mStarted;
  vector<boost::thread* > mThreads;
  ConcurrentQueue<vector<Data>*>* mBatchedQueue;
  
  void readerThread(int connectionId);
  
protected:
  vector<NdbDataReader<Data, Conn, Keys>* > mDataReaders;
};

template<typename Data, typename Conn, typename Keys>
NdbDataReaders<Data, Conn, Keys>::NdbDataReaders(){
  mStarted = false;
  mBatchedQueue = new ConcurrentQueue<vector<Data>*>();
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReaders<Data, Conn, Keys>::start() {
  if (mStarted) {
    return;
  }

  for (int i = 0; i < mDataReaders.size(); i++) {
    boost::thread* th = new boost::thread(&NdbDataReaders::readerThread, this, i);
    LOG_DEBUG(" Reader Thread [" << th->get_id() << "] created");
    mThreads.push_back(th);
  }
  mStarted = true;
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReaders<Data, Conn, Keys>::readerThread(int connIndex) {
  while (true) {
    vector<Data>* curr;
    mBatchedQueue->wait_and_pop(curr);
    LOG_DEBUG(" Process Batch ");
    mDataReaders[connIndex]->processBatch(curr);
    delete curr;
  }
}


template<typename Data, typename Conn, typename Keys>
void NdbDataReaders<Data, Conn, Keys>::processBatch(vector<Data>* data_batch) {
  mBatchedQueue->push(data_batch);
}

template<typename Data, typename Conn, typename Keys>
NdbDataReaders<Data, Conn, Keys>::~NdbDataReaders() {
  delete mBatchedQueue;
}

#endif /* NDBDATAREADERS_H */

