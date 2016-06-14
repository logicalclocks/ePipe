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

using namespace Utils::NdbC;

const int ER_PROJECT_ID = 1;
const int ER_DATASET_ID = 2;
const int ER_METADATA_ID = 3;

const char* ER_TABLE = "epipe_recovery";
const char* ER_TABLE_ID = "table";
const char* ER_LAST_USED_ID = "last_used_id";

const int ER_MODES_COUNT = 3;
const int MODES_ARR[ER_MODES_COUNT] = {ER_PROJECT_ID, ER_DATASET_ID, ER_METADATA_ID};


RecoveryIndeces Recovery::getRecoveryIndeces(Ndb* connection) {
    RecoveryIndeces ri;
    const NdbDictionary::Dictionary* database = getDatabase(connection);

    NdbTransaction* transaction = startNdbTransaction(connection);
    const NdbDictionary::Table* recovery_table = getTable(database, ER_TABLE);
    
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
        }
    }
    transaction->close();
    
    return ri;
}

