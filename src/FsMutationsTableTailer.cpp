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
 * File:   MutationsTableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "FsMutationsTableTailer.h"
#include "Utils.h"
#include <boost/unordered_map.hpp>

using namespace Utils::NdbC;

typedef boost::unordered_map<Uint64, FSpq*> HashMap;

const string _mutation_table= "hdfs_metadata_log";
const int _mutation_noCols= 7;
const string _mutation_cols[_mutation_noCols]=
    {"dataset_id",
     "inode_id",
     "logical_time",
     "inode_partition_id",
     "inode_parent_id",
     "inode_name",
     "operation"
    };

const int _mutation_noEvents = 1; 
const NdbDictionary::Event::TableEvent _mutation_events[_mutation_noEvents] = { NdbDictionary::Event::TE_INSERT };

const WatchTable FsMutationsTableTailer::TABLE = {_mutation_table, _mutation_cols, _mutation_noCols , _mutation_events, _mutation_noEvents, _mutation_cols[2]};

//const static ptime EPOCH_TIME(boost::gregorian::date(1970,1,1)); 

FsMutationsTableTailer::FsMutationsTableTailer(Ndb* ndb, const int poll_maxTimeToWait, const Barrier barrier,
        ProjectDatasetINodeCache* cache) : RCTableTailer(ndb, TABLE, poll_maxTimeToWait, barrier), mPDICache(cache) {
    mQueue = new CFSq();
    mCurrentPriorityQueue = new FSpq();
//    mTimeTakenForEventsToArrive = 0;
//    mNumOfEvents = 0;
//    mPrintEveryNEvents = 0;
}

void FsMutationsTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
    FsMutationRow row;
    readRowFromNdbRecAttr(row, value);
    if (row.mOperation == Add || row.mOperation == Delete) {        
        mLock.lock();
        mCurrentPriorityQueue->push(row);
        int size = mCurrentPriorityQueue->size();
        mLock.unlock();
        
        LOG_TRACE(" push inode [" << row.mInodeId << "] to queue[" << size << "], Op [" << row.mOperation << "]");

        if (row.mOperation == Add) {
            mPDICache->addINodeToDataset(row.mInodeId, row.mDatasetId);
        } else if (row.mOperation == Delete) {
            mPDICache->removeINode(row.mInodeId);
        }
    } else {
       LOG_ERROR( "Unknown Operation [" << row.mOperation << "] for " << " INode [" << row.mInodeId << "]");
    }
    
//    ptime t = EPOCH_TIME + boost::posix_time::milliseconds(row.mTimestamp);
//    mTimeTakenForEventsToArrive += Utils::getTimeDiffInMilliseconds(t, row.mEventCreationTime);
//    mNumOfEvents++;
//    mPrintEveryNEvents++;
//    if(mPrintEveryNEvents>=10000){
//        double avgArrival = mTimeTakenForEventsToArrive / mNumOfEvents;
//        LOG_INFO("Average Arrival Time=" << avgArrival << " msec");
//        mPrintEveryNEvents = 0;
//    }
}

void FsMutationsTableTailer::barrierChanged() {
    FSpq* pq = NULL;
    mLock.lock();
    if(!mCurrentPriorityQueue->empty()){
        pq = mCurrentPriorityQueue;
        mCurrentPriorityQueue = new FSpq();
    }
    mLock.unlock();

    if (pq != NULL) {
        LOG_TRACE("--------------------------------------NEW BARRIER (" << pq->size() << " events )------------------- ");
        pushToQueue(pq);
    }
}

FsMutationRow FsMutationsTableTailer::consume(){
    FsMutationRow row;
    mQueue->wait_and_pop(row);
    LOG_TRACE(" pop inode [" << row.mInodeId << "] from queue \n" << row.to_string());
    return row;
}

void FsMutationsTableTailer::removeLogs(Ndb* conn, FPK& pks) {
     const NdbDictionary::Dictionary* database = getDatabase(conn);
    NdbTransaction* transaction = startNdbTransaction(conn);
    const NdbDictionary::Table* log_table = getTable(database, TABLE.mTableName);
    for(FPK::iterator it=pks.begin(); it != pks.end() ; ++it){
        FsMutationPK pk = *it;
        NdbOperation* op = getNdbOperation(transaction, log_table);
        
        op->deleteTuple();
        op->equal(_mutation_cols[0].c_str(), pk.mDatasetId);
        op->equal(_mutation_cols[1].c_str(), pk.mInodeId);
        op->equal(_mutation_cols[2].c_str(), pk.mLogicalTime);
        
        LOG_TRACE("Delete log row: Dataset[" << pk.mDatasetId << "], INode[" 
                << pk.mInodeId << "], Timestamp[" << pk.mLogicalTime << "]");
    }
    executeTransaction(transaction, NdbTransaction::Commit);
    conn->closeTransaction(transaction);
}

void FsMutationsTableTailer::recover(){
    ptime start = Utils::getCurrentTime();
    const NdbDictionary::Dictionary* database = getDatabase(mNdbConnection);
    const NdbDictionary::Table* table = getTable(database, TABLE.mTableName);
    
    NdbTransaction* transaction = startNdbTransaction(mNdbConnection);
    NdbScanOperation* scanOp = getNdbScanOperation(transaction, table);
        
    scanOp->readTuples(NdbOperation::LM_CommittedRead);
            
    NdbRecAttr * cols[TABLE.mNoColumns];
    NdbRecAttr* gciCol = scanOp->getValue(NdbDictionary::Column::ROW_GCI);

    for (int i = 0; i < TABLE.mNoColumns; i++) {
        cols[i] = scanOp->getValue(TABLE.mColumnNames[i].c_str());
    }

    executeTransaction(transaction, NdbTransaction::Commit);
    
    
    HashMap* rowsByGCI = new HashMap();
    
    while (scanOp->nextResult(true) == 0) {
        FsMutationRow row;
        readRowFromNdbRecAttr(row, cols);
        Uint64 gci = gciCol->u_64_value();
        HashMap::iterator curr = rowsByGCI->find(gci);
        FSpq* currRows;
        if(curr == rowsByGCI->end()){
            currRows = new FSpq();
            rowsByGCI->emplace(gci, currRows);
        }else{
            currRows = curr->second;
        }
        currRows->push(row);
    }
    transaction->close();

    vector<Uint64>* gcis = new vector<Uint64>();

    for (HashMap::iterator it = rowsByGCI->begin(); it != rowsByGCI->end(); it++) {
        gcis->push_back(it->first);
    }

    std::sort(gcis->begin(), gcis->end());
    
    
    LOG_INFO("ePipe done reading/sorting for recovery in " << Utils::getTimeDiffInMilliseconds(start, Utils::getCurrentTime()) << " msec");
    
    for(vector<Uint64>::iterator it=gcis->begin(); it != gcis->end(); it++){
        Uint64 gci = *it;
        HashMap::iterator rit = rowsByGCI->find(gci);
        pushToQueue(rit->second);
    }
    
}

void FsMutationsTableTailer::readRowFromNdbRecAttr(FsMutationRow &row, NdbRecAttr* value[]){
    row.mEventCreationTime = Utils::getCurrentTime();
    row.mDatasetId = value[0]->int32_value();
    row.mInodeId =  value[1]->int32_value();
    row.mLogicalTime = value[2]->int32_value();
    row.mPartitionId = value[3]->int32_value();
    row.mParentId = value[4]->int32_value();
    row.mInodeName = get_string(value[5]);
    row.mOperation = static_cast<OperationType>(value[6]->int8_value());
}

void FsMutationsTableTailer::pushToQueue(FSpq* curr) {
    while (!curr->empty()) {
        mQueue->push(curr->top());
        curr->pop();
    }
    delete curr;
}

FsMutationsTableTailer::~FsMutationsTableTailer() {
    delete mQueue;
}

