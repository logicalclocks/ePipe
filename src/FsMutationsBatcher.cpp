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
 * File:   FsMutationsBatcher.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "FsMutationsBatcher.h"

FsMutationsBatcher::FsMutationsBatcher(FsMutationsTableTailer* table_tailer, FsMutationsDataReader* data_reader, 
        const int time_before_issuing_ndb_reqs, const int batch_size) : Batcher(time_before_issuing_ndb_reqs, batch_size) {
    
    mTableTailer = table_tailer;
    mNdbDataReader = data_reader;
    
    mAddOperations = new Cus();
    mDeleteOperations = new Cus();
}

void FsMutationsBatcher::run() {
    while (true) {
        FsMutationRow row = mTableTailer->consume();

        LOG_TRACE() << "-------------------------";
        LOG_TRACE() << "DatasetId = " << row.mDatasetId;
        LOG_TRACE() << "InodeId = " << row.mInodeId;
        LOG_TRACE() << "ParentId = " << row.mParentId;
        LOG_TRACE() << "InodeName = " << row.mInodeName;
        LOG_TRACE() << "Timestamp = " << row.mTimestamp;
        LOG_TRACE() << "Operation = " << row.mOperation;
        LOG_TRACE() << "-------------------------";
        
        if (row.mOperation == DELETE) {
            mLock.lock();
            mDeleteOperations->unsynchronized_add(row);
            mLock.unlock();
        } else if (row.mOperation == ADD) {
            mLock.lock();
            mAddOperations->unsynchronized_add(row);
            mLock.unlock();
        } else {

            LOG_ERROR() << "Unknown Operation code " << row.mOperation;
        }
        
        if(mAddOperations->size() == mBatchSize && !mTimerProcessing){
            processBatch();
        }
    }

}

void FsMutationsBatcher::processBatch() {
    if (mDeleteOperations->size() > 0 || mAddOperations->size() > 0) {
        LOG_DEBUG() << "process batch";
        
        mLock.lock();
        Cus_Cus added_deleted_batch;
        added_deleted_batch.deleted = mDeleteOperations;
        mDeleteOperations = new Cus();
        added_deleted_batch.added = mAddOperations;
        mAddOperations = new Cus();
        mLock.unlock();
        
        mNdbDataReader->processBatch(added_deleted_batch);
    }
}

FsMutationsBatcher::~FsMutationsBatcher() {
    delete mDeleteOperations;
    delete mAddOperations;
}

