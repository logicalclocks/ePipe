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
 * File:   RCBatcher.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef RCBATCHER_H
#define RCBATCHER_H

#include "Batcher.h"
#include "RCTableTailer.h"
#include "NdbDataReader.h"

template<typename DataRow, typename Conn>
class RCBatcher : public Batcher {
public:
    RCBatcher(RCTableTailer<DataRow>* table_tailer, NdbDataReader<DataRow, Conn>* ndb_data_reader,
            const int time_before_issuing_ndb_reqs, const int batch_size);
    
    RCBatcher(RCTableTailer<DataRow>* table_tailer, NdbDataReader<DataRow, Conn>* ndb_data_reader,
            const int time_before_issuing_ndb_reqs, const int batch_size, const int queue_id);
private:

    RCTableTailer<DataRow>* mTableTailer;
    NdbDataReader<DataRow, Conn>* mNdbDataReader;
    const int mQueueId;
    
    int mCurrentCount;
    boost::mutex mLock;
    vector<DataRow>* mOperations;
    virtual void run();
    virtual void processBatch();
};

template<typename DataRow, typename Conn>
RCBatcher<DataRow, Conn>::RCBatcher(RCTableTailer<DataRow>* table_tailer, NdbDataReader<DataRow, Conn>* ndb_data_reader,
        const int time_before_issuing_ndb_reqs, const int batch_size)
: Batcher(time_before_issuing_ndb_reqs, batch_size), mTableTailer(table_tailer), mNdbDataReader(ndb_data_reader), mQueueId(SINGLE_QUEUE) {
    mCurrentCount = 0;
    mOperations = new vector<DataRow>();
}

template<typename DataRow, typename Conn>
RCBatcher<DataRow, Conn>::RCBatcher(RCTableTailer<DataRow>* table_tailer, NdbDataReader<DataRow, Conn>* ndb_data_reader,
        const int time_before_issuing_ndb_reqs, const int batch_size, const int queue_id)
: Batcher(time_before_issuing_ndb_reqs, batch_size), mTableTailer(table_tailer), mNdbDataReader(ndb_data_reader), mQueueId(queue_id) {
    mCurrentCount = 0;
    mOperations = new vector<DataRow>();
}

template<typename DataRow, typename Conn>
void RCBatcher<DataRow, Conn>::run() {
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

template<typename DataRow, typename Conn>
void RCBatcher<DataRow, Conn>::processBatch() {
    if (mCurrentCount > 0) {
        LOG_DEBUG("process batch");

        mLock.lock();
        vector<DataRow>* added_deleted_batch = mOperations;
        mOperations = new vector<DataRow>();
        mCurrentCount = 0;
        mLock.unlock();

        mNdbDataReader->processBatch(added_deleted_batch);
    }
}
#endif /* RCBATCHER_H */

