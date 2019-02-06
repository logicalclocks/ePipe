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
#include "TimedRestBatcher.h"

using namespace Utils;

template<typename Keys>
class DataReaderOutHandler{
  public:
    virtual void writeOutput(Bulk<Keys> out) = 0;
};

template<typename Data>
struct IndexedDataBatch{
  vector<Data>* mDataBatch;
  Uint64 mIndex;
  IndexedDataBatch(){
    
  }
  IndexedDataBatch(Uint64 index, vector<Data>* data){
   mIndex = index;
   mDataBatch = data;
  }
};

template<typename Data, typename Conn, typename Keys>
class NdbDataReader {
public:
  NdbDataReader(Conn connection, const bool hopsworks);
  void start(int readerId, DataReaderOutHandler<Keys>* outHandler);
  void processBatch(Uint64 index, vector<Data>* data_batch);
  virtual ~NdbDataReader();
  
protected:
  boost::thread mThread;
  Conn mNdbConnection;
  const bool mHopsworksEnalbed;
  virtual void processAddedandDeleted(vector<Data>* data_batch, Bulk<Keys>& bulk) = 0;
  
 private:
  int mReaderId;
  DataReaderOutHandler<Keys>* mOutHandler;
  ConcurrentQueue<IndexedDataBatch<Data> >* mBatchedQueue;
  void run();
};

template<typename Data, typename Conn, typename Keys>
NdbDataReader<Data, Conn, Keys>::NdbDataReader(Conn connection, const bool hopsworks)
: mNdbConnection(connection), mHopsworksEnalbed(hopsworks) {
  mBatchedQueue = new ConcurrentQueue<IndexedDataBatch<Data> >();
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReader<Data, Conn, Keys>::start(int readerId, DataReaderOutHandler<Keys>* outHandler) {
  mOutHandler = outHandler;
  mThread = boost::thread(&NdbDataReader::run, this);
  mReaderId = readerId;
  LOG_DEBUG("Reader-" << readerId << " created with thread "  << mThread.get_id());
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReader<Data, Conn, Keys>::run() {
  while (true) {
    IndexedDataBatch<Data> batch;
    mBatchedQueue->wait_and_pop(batch);

    if (!batch.mDataBatch->empty()) {
      Bulk<Keys> bulk;

      bulk.mProcessingIndex = batch.mIndex;
      
      bulk.mStartProcessing = getCurrentTime();

      processAddedandDeleted(batch.mDataBatch, bulk);
      
      bulk.mEndProcessing = getCurrentTime();

      sort(bulk.mArrivalTimes.begin(), bulk.mArrivalTimes.end());
      
      mOutHandler->writeOutput(bulk);
      
      LOG_DEBUG("Reader-" << mReaderId << " processing batch " << batch.mIndex << " of size [" << batch.mDataBatch->size() << "] took "
          << getTimeDiffInMilliseconds(bulk.mStartProcessing, bulk.mEndProcessing) << " msec");
    }

  }
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReader<Data, Conn, Keys>::processBatch(Uint64 index, vector<Data>* data_batch) {
  mBatchedQueue->push(IndexedDataBatch<Data>(index, data_batch));
  LOG_DEBUG("Reader-" << mReaderId << ": Process batch " << index);
}

template<typename Data, typename Conn, typename Keys>
NdbDataReader<Data, Conn, Keys>::~NdbDataReader() {

}

#endif /* NDBDATAREADER_H */

