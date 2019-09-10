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

#ifndef RCBATCHER_H
#define RCBATCHER_H

#include "Batcher.h"
#include "RCTableTailer.h"
#include "NdbDataReaders.h"

template<typename DataRow, typename Conn, typename Keys>
class RCBatcher : public Batcher {
public:
  RCBatcher(RCTableTailer<DataRow>* table_tailer, NdbDataReaders<DataRow, Conn, Keys>* ndb_data_readers,
          const int time_before_issuing_ndb_reqs, const int batch_size);

  RCBatcher(RCTableTailer<DataRow>* table_tailer, NdbDataReaders<DataRow, Conn, Keys>* ndb_data_readers,
          const int time_before_issuing_ndb_reqs, const int batch_size, const int queue_id);
private:

  RCTableTailer<DataRow>* mTableTailer;
  NdbDataReaders<DataRow, Conn, Keys>* mNdbDataReaders;
  const int mQueueId;

  int mCurrentCount;
  boost::mutex mLock;
  std::vector<DataRow>* mOperations;
  virtual void run();
  virtual void processBatch();
};

template<typename DataRow, typename Conn, typename Keys>
RCBatcher<DataRow, Conn, Keys>::RCBatcher(RCTableTailer<DataRow>* table_tailer, NdbDataReaders<DataRow, Conn, Keys>* ndb_data_readers,
        const int time_before_issuing_ndb_reqs, const int batch_size)
: Batcher(time_before_issuing_ndb_reqs, batch_size), mTableTailer(table_tailer), mNdbDataReaders(ndb_data_readers), mQueueId(SINGLE_QUEUE) {
  mCurrentCount = 0;
  mOperations = new std::vector<DataRow>();
}

template<typename DataRow, typename Conn, typename Keys>
RCBatcher<DataRow, Conn, Keys>::RCBatcher(RCTableTailer<DataRow>* table_tailer, NdbDataReaders<DataRow, Conn, Keys>* ndb_data_readers,
        const int time_before_issuing_ndb_reqs, const int batch_size, const int queue_id)
: Batcher(time_before_issuing_ndb_reqs, batch_size), mTableTailer(table_tailer), mNdbDataReaders(ndb_data_readers), mQueueId(queue_id) {
  mCurrentCount = 0;
  mOperations = new std::vector<DataRow>();
}

template<typename DataRow, typename Conn, typename Keys>
void RCBatcher<DataRow, Conn, Keys>::run() {
  while (true) {
    DataRow row = mTableTailer->consumeMultiQueue(mQueueId);

    mLock.lock();
    mOperations->push_back(row);
    mCurrentCount++;
    mLock.unlock();

    if (mCurrentCount == mBatchSize && !mTimerProcessing) {
      resetTimer();
      processBatch();
    }
  }
}

template<typename DataRow, typename Conn, typename Keys>
void RCBatcher<DataRow, Conn, Keys>::processBatch() {
  if (mCurrentCount > 0) {
    LOG_DEBUG("process batch");

    mLock.lock();
    std::vector<DataRow>* added_deleted_batch = mOperations;
    mOperations = new std::vector<DataRow>();
    mCurrentCount = 0;
    mLock.unlock();

    mNdbDataReaders->processBatch(added_deleted_batch);
  }
}
#endif /* RCBATCHER_H */

