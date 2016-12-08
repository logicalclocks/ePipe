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
 * File:   Recovery.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "Recovery.h"
#include "SchemabasedMetadataReader.h"

using namespace Utils::NdbC;

const int ER_PROJECT_ID = 1;
const int ER_DATASET_ID = 2;
const int ER_METADATA_ID = 3;
const int ER_SCHEMALESS_METADATA_ID = 4;

const char* ER_TABLE = "epipe_recovery";
const char* ER_TABLE_ID = "table";
const char* ER_LAST_USED_ID = "last_used_id";

const int ER_MODES_COUNT = 4;
const int MODES_ARR[ER_MODES_COUNT] = {ER_PROJECT_ID, ER_DATASET_ID, ER_METADATA_ID, ER_SCHEMALESS_METADATA_ID};


RecoveryIndeces Recovery::getRecoveryIndeces(Ndb* connection) {
    RecoveryIndeces ri;
    const NdbDictionary::Dictionary* database = getDatabase(connection);
    const NdbDictionary::Table* recovery_table = getTable(database, ER_TABLE);

    NdbTransaction* transaction = startNdbTransaction(connection);
    
    UIRowMap rows;
    for(int i=0; i<ER_MODES_COUNT; i++){
        NdbOperation* op = getNdbOperation(transaction, recovery_table);
        op->readTuple(NdbOperation::LM_Read);
        int id = MODES_ARR[i];
        op->equal(ER_TABLE_ID, id);
        
        NdbRecAttr* id_col = getNdbOperationValue(op, ER_TABLE_ID);
        rows[id].push_back(id_col);
        
        NdbRecAttr* last_used_id_col = getNdbOperationValue(op, ER_LAST_USED_ID);
        rows[id].push_back(last_used_id_col);
    }
    
    executeTransaction(transaction, NdbTransaction::Commit);
    
    for(UIRowMap::iterator it = rows.begin(); it != rows.end(); ++it){
        int last_used_id = 0;
        if(it->first == it->second[0]->int8_value()){
            last_used_id = it->second[1]->int32_value();
        }
        
        if(it->first == ER_PROJECT_ID){
            ri.mProjectIndex = last_used_id;
        }else if(it->first == ER_DATASET_ID){
            ri.mDatasetIndex = last_used_id;
        }else if(it->first == ER_METADATA_ID){
            ri.mMetadataIndex = last_used_id;
        }else if(it->first == ER_SCHEMALESS_METADATA_ID){
            ri.mSchemalessMetadataIndex = last_used_id;
        }
    }
    ri.mMutationsIndex = 0;
    transaction->close();
    
    return ri;
}

void Recovery::checkpointProject(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, int projectId) {
    checkpoint(database, transaction, ER_PROJECT_ID, projectId);
}

void Recovery::checkpointDataset(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, int datasetId) {
    checkpoint(database, transaction, ER_DATASET_ID, datasetId);
}

void Recovery::checkpointMetadata(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, int metadataId) {
    checkpoint(database, transaction, ER_METADATA_ID, metadataId);
}

void Recovery::checkpointSchemalessMetadata(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, int metadataId){
    checkpoint(database, transaction, ER_SCHEMALESS_METADATA_ID, metadataId);
}

void Recovery::checkpoint(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, int colId, int value) {
    ptime t1 = Utils::getCurrentTime();
    const NdbDictionary::Table* recovery_table = getTable(database, ER_TABLE);
    NdbOperation* op = getNdbOperation(transaction, recovery_table);
    op->writeTuple();
    op->equal(ER_TABLE_ID, colId);
    op->setValue(ER_LAST_USED_ID, value);
    executeTransaction(transaction, NdbTransaction::Commit);
    ptime t2 = Utils::getCurrentTime();
    LOG_TRACE("Checkpoint: [" << colId << "] --> (" << value << ") in " << Utils::getTimeDiffInMilliseconds(t1, t2) << " msec");
}



