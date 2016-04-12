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
 * File:   NdbDataReader.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "NdbDataReader.h"
#include "Utils.h"

NdbDataReader::NdbDataReader(Ndb** connections, const int num_readers) : mNdbConnections(connections), mNumReaders(num_readers){
    mStarted = false;
    mBatchedQueue = new ConcurrentQueue<Cus_Cus>();
}

void NdbDataReader::start() {
    if (mStarted) {
        return;
    }
    
    for(int i=0; i< mNumReaders; i++){
        boost::thread* th = new boost::thread(&NdbDataReader::readerThread, this, i);
        LOG_DEBUG() << " Reader Thread [" << th->get_id() << "] created"; 
        mThreads.push_back(th);
    }
    mStarted = true;
}

void NdbDataReader::readerThread(int connIndex) {
    while(true){
        Cus_Cus curr;
        mBatchedQueue->wait_and_pop(curr);
        LOG_DEBUG() << " Process Batch Add [" << curr.added->unsynchronized_size() << "] Delete [" << curr.deleted->unsynchronized_size() << "] ";
        readData(mNdbConnections[connIndex], curr.added);
    }
}

void NdbDataReader::readData(Ndb* connection, Cus* added) {

    const NdbDictionary::Dictionary* database = connection->getDictionary();
    if (!database) LOG_NDB_API_ERROR(connection->getNdbError());

    const NdbDictionary::Table* inodes_table = database->getTable("hdfs_inodes");
    if (!inodes_table) LOG_NDB_API_ERROR(database->getNdbError());

    NdbTransaction* ts = connection->startTransaction();
    if (ts == NULL) LOG_NDB_API_ERROR(connection->getNdbError());
    
    NdbDictionary::Column::ArrayType name_array_type = inodes_table->getColumn("name")->getArrayType();
    
    const int num_inodes_columns = 2;
    const char* inodes_columns_to_read[] = {"id", "size"};

    int batch_size = added->unsynchronized_size();
    
    std::vector<FsMutationRow> pending;
    std::vector<NdbRecAttr*> cols[batch_size];
    
    int i = 0;
    while (i < batch_size) {
        FsMutationRow row = added->remove();

        NdbOperation* op = ts->getNdbOperation(inodes_table);
        if (op == NULL) LOG_NDB_API_ERROR(ts->getNdbError());
        
        op->readTuple(NdbOperation::LM_CommittedRead);
        
        op->equal("parent_id", row.mParentId);
        op->equal("name", Utils::get_ndb_varchar(row.mInodeName, name_array_type));
        
        for(int c=0; c<num_inodes_columns; c++){
            NdbRecAttr* col = op->getValue(inodes_columns_to_read[c]);
            if (col == NULL) LOG_NDB_API_ERROR(ts->getNdbError());
            cols[i].push_back(col);
        }
        
        LOG_TRACE() << " Read INode row for [" << row.mParentId << " , "  << row.mInodeName << "]";  
        pending.push_back(row);
        i++;
    }                    
    
    if(ts->execute(NdbTransaction::Commit) == -1){
        LOG_NDB_API_ERROR(ts->getNdbError());
    }
    
    for (int i = 0; i < batch_size; i++) {
          for(int c=0; c<num_inodes_columns; c++){
              NdbRecAttr* col = cols[i][c];
              if(c == 0 && pending[i].mInodeId != col->int32_value()){
                   LOG_INFO() << " Data for " << pending[i].mParentId << ", " << pending[i].mInodeName << " not found";
                  break;
              }
            LOG_INFO() << "Got values for INode " << col->getColumn()->getName() << " = "<< col->int64_value(); 
          }
    }

    connection->closeTransaction(ts);
    
}

void NdbDataReader::process_batch(Cus_Cus added_deleted_batch) {
    mBatchedQueue->push(added_deleted_batch);
}

NdbDataReader::~NdbDataReader() {
    for(int i=0; i< mNumReaders; i++){
        delete mNdbConnections[i];
    }
    delete mBatchedQueue;
}

