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
 * File:   MetadataReader.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "MetadataReader.h"

const int NUM_FIELDS_COLS = 3;
const char* FIELDS_COLS_TO_READ[] = {"name", "searchable", "tableid"};

MetadataReader::MetadataReader(Ndb** connections, const int num_readers,  std::string elastic_ip) : NdbDataReader(connections, num_readers, elastic_ip) {

}

void MetadataReader::readData(Ndb* connection, Mq_Mq data_batch) {
    Mq* added = data_batch.added;
    
    const NdbDictionary::Dictionary* database = connection->getDictionary();
    if (!database) LOG_NDB_API_ERROR(connection->getNdbError());

    // TODO check in the fields cache before going to the database
    
    const NdbDictionary::Table* fields_table = database->getTable("meta_fields");
    if (!fields_table) LOG_NDB_API_ERROR(database->getNdbError());
    
    NdbTransaction* ts = connection->startTransaction();
    if (ts == NULL) LOG_NDB_API_ERROR(connection->getNdbError());
    
    int batch_size = added->size();
    std::vector<NdbRecAttr*> cols[batch_size]; 
    for(int i=0; i<batch_size; i++){
        MetadataRow row = added->front();
        added->pop();
        
        NdbOperation* op = ts->getNdbOperation(fields_table);
        op->readTuple(NdbOperation::LM_CommittedRead);
        op->equal("fieldid", row.mFieldId);
        
        for(int c=0; c<NUM_FIELDS_COLS; c++){
            NdbRecAttr* col = op->getValue(FIELDS_COLS_TO_READ[c]);
            if (col == NULL) LOG_NDB_API_ERROR(ts->getNdbError());
            cols[i].push_back(col);
        }
        
        LOG_TRACE() << " Read meta_field row for [" << row.mFieldId << "]"; 
    }
    
    if(ts->execute(NdbTransaction::NoCommit) == -1){
        LOG_NDB_API_ERROR(ts->getNdbError());
    }
}


MetadataReader::~MetadataReader() {
}

