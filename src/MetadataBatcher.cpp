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
 * File:   MetadataBatcher.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "MetadataBatcher.h"

MetadataBatcher::MetadataBatcher(MetadataTableTailer* table_tailer, MetadataReader* data_reader, 
        const int time_before_issuing_ndb_reqs, const int batch_size) : Batcher(time_before_issuing_ndb_reqs, batch_size) {
    mTableTailer = table_tailer;
    mNdbDataReader = data_reader;
    
    mAddOperations = new Mq();
    mDeleteOperations = new Mq();
}

void MetadataBatcher::run() {
  while (true) {
        MetadataRow row = mTableTailer->consume();

        LOG_TRACE() << "-------------------------";
        LOG_TRACE() << "Id = " << row.mId;
        LOG_TRACE() << "FieldId = " << row.mFieldId;
        LOG_TRACE() << "TupleId = " << row.mTupleId;
        LOG_TRACE() << "Data = " << row.mMetadata;
        LOG_TRACE() << "Operation = " << row.mOperation;
        LOG_TRACE() << "-------------------------";
        
        if (row.mOperation == DELETE) {
            mLock.lock();
            mDeleteOperations->push(row);
            mLock.unlock();
        } else if (row.mOperation == ADD) {
            mLock.lock();
            mAddOperations->push(row);
            mLock.unlock();
        } else {

            LOG_ERROR() << "Unknown Operation code " << row.mOperation;
        }
        
        if(mAddOperations->size() == (unsigned) mBatchSize && !mTimerProcessing){
            processBatch();
        }
    }
}

void MetadataBatcher::processBatch() {
     if (mDeleteOperations->size() > 0 || mAddOperations->size() > 0) {
        LOG_DEBUG() << "process batch";
        
        mLock.lock();
        Mq_Mq added_deleted_batch;
        added_deleted_batch.deleted = mDeleteOperations;
        mDeleteOperations = new Mq();
        added_deleted_batch.added = mAddOperations;
        mAddOperations = new Mq();
        mLock.unlock();
        
        mNdbDataReader->processBatch(added_deleted_batch);
    }
}

MetadataBatcher::~MetadataBatcher() {
    delete mDeleteOperations;
    delete mAddOperations;
}

