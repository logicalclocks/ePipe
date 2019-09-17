/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * ePipe is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * ePipe is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef NDBDATAREADER_H
#define NDBDATAREADER_H

#include "Cache.h"
#include "Utils.h"
#include "TimedRestBatcher.h"

using namespace Utils;

class DataReaderOutHandler{
  public:
    virtual void writeOutput(eBulk out) = 0;
};

template<typename Data>
struct IndexedDataBatch{
  std::vector<Data>* mDataBatch;
  Uint64 mIndex;
  IndexedDataBatch(){
    
  }
  IndexedDataBatch(Uint64 index, std::vector<Data>* data){
   mIndex = index;
   mDataBatch = data;
  }
};

template<typename Data, typename Conn>
class NdbDataReader {
public:
  NdbDataReader(Conn connection, const bool hopsworks);
  void start(int readerId, DataReaderOutHandler* outHandler);
  void processBatch(Uint64 index, std::vector<Data>* data_batch);
  virtual ~NdbDataReader();
  
protected:
  boost::thread mThread;
  Conn mNdbConnection;
  const bool mHopsworksEnalbed;
  virtual void processAddedandDeleted(std::vector<Data>* data_batch,
      eBulk& bulk) = 0;
  
 private:
  int mReaderId;
  DataReaderOutHandler* mOutHandler;
  ConcurrentQueue<IndexedDataBatch<Data> >* mBatchedQueue;
  void run();
};

template<typename Data, typename Conn>
NdbDataReader<Data, Conn>::NdbDataReader(Conn connection, const bool hopsworks)
: mNdbConnection(connection), mHopsworksEnalbed(hopsworks) {
  mBatchedQueue = new ConcurrentQueue<IndexedDataBatch<Data> >();
}

template<typename Data, typename Conn>
void NdbDataReader<Data, Conn>::start(int readerId, DataReaderOutHandler* outHandler) {
  mOutHandler = outHandler;
  mThread = boost::thread(&NdbDataReader::run, this);
  mReaderId = readerId;
  LOG_DEBUG("Reader-" << readerId << " created with thread "  << mThread.get_id());
}

template<typename Data, typename Conn>
void NdbDataReader<Data, Conn>::run() {
  while (true) {
    IndexedDataBatch<Data> batch;
    mBatchedQueue->wait_and_pop(batch);

    if (!batch.mDataBatch->empty()) {
      eBulk bulk;

      bulk.mProcessingIndex = batch.mIndex;
      
      bulk.mStartProcessing = getCurrentTime();

      processAddedandDeleted(batch.mDataBatch, bulk);
      
      bulk.mEndProcessing = getCurrentTime();

      bulk.sortArrivalTimes();

      mOutHandler->writeOutput(bulk);
      
      LOG_DEBUG("Reader-" << mReaderId << " processing batch " << batch.mIndex << " of size [" << batch.mDataBatch->size() << "] took "
          << getTimeDiffInMilliseconds(bulk.mStartProcessing, bulk.mEndProcessing) << " msec");
    }

  }
}

template<typename Data, typename Conn>
void NdbDataReader<Data, Conn>::processBatch(Uint64 index, std::vector<Data>* data_batch) {
  mBatchedQueue->push(IndexedDataBatch<Data>(index, data_batch));
  LOG_DEBUG("Reader-" << mReaderId << ": Process batch " << index);
}

template<typename Data, typename Conn>
NdbDataReader<Data, Conn>::~NdbDataReader() {

}

#endif /* NDBDATAREADER_H */

