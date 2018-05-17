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
 * File:   ProvenanceTableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#include "ProvenanceTableTailer.h"
#include <boost/unordered_map.hpp>

typedef boost::unordered_map<Uint64, PRpq*> HashMap;

using namespace Utils::NdbC;

const string _provenance_table= "hdfs_provenance_log";
const int _provenance_noCols= 14;
const string _provenance_cols[_provenance_noCols]=
        {"inode_id",
         "user_id",
         "app_id",
         "logical_time",
         "partition_id",
         "parent_id",
         "project_name",
         "dataset_name",
         "inode_name",
         "user_name",
         "logical_time_batch",
         "timestamp",
         "timestamp_batch",
         "operation"
        };

const int _provenance_noEvents = 1;
const NdbDictionary::Event::TableEvent _provenance_events[_provenance_noEvents] = { NdbDictionary::Event::TE_INSERT };

const WatchTable ProvenanceTableTailer::TABLE = {_provenance_table, _provenance_cols, _provenance_noCols , _provenance_events, _provenance_noEvents, _provenance_cols[3]};


ProvenanceTableTailer::ProvenanceTableTailer(Ndb *ndb, const int poll_maxTimeToWait, const Barrier barrier)
        : RCTableTailer(ndb, TABLE, poll_maxTimeToWait, barrier) {
    mQueue = new CPRq();
    mCurrentPriorityQueue = new PRpq();
}

void ProvenanceTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[],
                                        NdbRecAttr* value[]) {
    ProvenanceRow row;
    readRowFromNdbRecAttr(row, value);

    mLock.lock();
    mCurrentPriorityQueue->push(row);
    int size = mCurrentPriorityQueue->size();
    mLock.unlock();

    LOG_TRACE("push provenance log for [" << row.mInodeName << "] to queue[" << size << "], Op [" << row.mOperation << "]");

}

void ProvenanceTableTailer::barrierChanged() {
    PRpq* pq = NULL;
    mLock.lock();
    if(!mCurrentPriorityQueue->empty()){
        pq = mCurrentPriorityQueue;
        mCurrentPriorityQueue = new PRpq();
    }
    mLock.unlock();

    if (pq != NULL) {
        LOG_TRACE("--------------------------------------NEW BARRIER (" << pq->size() << " events )------------------- ");
        pushToQueue(pq);
    }
}

ProvenanceRow ProvenanceTableTailer::consume() {
    ProvenanceRow row;
    mQueue->wait_and_pop(row);
    LOG_TRACE(" pop inode [" << row.mInodeId << "] from queue \n" << row.to_string());
    return row;
}

void ProvenanceTableTailer::readRowFromNdbRecAttr(ProvenanceRow &row, NdbRecAttr* value[]) {
    row.mEventCreationTime = Utils::getCurrentTime();
    row.mInodeId = value[0]->int32_value();
    row.mUserId = value[1]->int32_value();
    row.mAppId = get_string(value[2]);
    row.mLogicalTime = value[3]->int32_value();
    row.mPartitionId = value[4]->int32_value();
    row.mParentId = value[5]->int32_value();
    row.mProjectName = get_string(value[6]);
    row.mDatasetName = get_string(value[7]);
    row.mInodeName = get_string(value[8]);
    row.mUserName = get_string(value[9]);
    row.mLogicalTimeBatch = value[10]->int32_value();
    row.mTimestamp = value[11]->int64_value();
    row.mTimestampBatch = value[12]->int64_value();
    row.mOperation = value[13]->int8_value();
}

void ProvenanceTableTailer::pushToQueue(PRpq *curr) {
    while (!curr->empty()) {
        mQueue->push(curr->top());
        curr->pop();
    }
    delete curr;
}

void ProvenanceTableTailer::removeLogs(Ndb* conn, PKeys& pks) {
    const NdbDictionary::Dictionary* database = getDatabase(conn);
    NdbTransaction* transaction = startNdbTransaction(conn);
    const NdbDictionary::Table* log_table = getTable(database, TABLE.mTableName);
    NdbDictionary::Column::ArrayType app_id_array_type = 
            log_table->getColumn(_provenance_cols[2].c_str())->getArrayType();

    for(PKeys::iterator it=pks.begin(); it != pks.end() ; ++it){
        ProvenancePK pk = *it;
        NdbOperation* op = getNdbOperation(transaction, log_table);
        
        op->deleteTuple();
        op->equal(_provenance_cols[0].c_str(), pk.mInodeId);
        op->equal(_provenance_cols[1].c_str(), pk.mUserId);
        op->equal(_provenance_cols[2].c_str(), get_ndb_varchar(pk.mAppId, app_id_array_type).c_str());
        op->equal(_provenance_cols[3].c_str(), pk.mLogicalTime);

        
        LOG_DEBUG("Delete log row: App[" << pk.mAppId << "], INode[" 
                << pk.mInodeId << "], User[" << pk.mUserId << "], Timestamp["
                << pk.mLogicalTime << "]");
    }
    executeTransaction(transaction, NdbTransaction::Commit);
    conn->closeTransaction(transaction);
}

void ProvenanceTableTailer::recover() {
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
        ProvenanceRow row;
        readRowFromNdbRecAttr(row, cols);
        Uint64 gci = gciCol->u_64_value();
        HashMap::iterator curr = rowsByGCI->find(gci);
        PRpq* currRows;
        if(curr == rowsByGCI->end()){
            currRows = new PRpq();
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
    
    
    LOG_INFO("ePipe done reading/sorting for " << _provenance_table << " recovery in " << Utils::getTimeDiffInMilliseconds(start, Utils::getCurrentTime()) << " msec");
    
    for(vector<Uint64>::iterator it=gcis->begin(); it != gcis->end(); it++){
        Uint64 gci = *it;
        HashMap::iterator rit = rowsByGCI->find(gci);
        pushToQueue(rit->second);
    }
    
}

ProvenanceTableTailer::~ProvenanceTableTailer() {
    delete mQueue;
}