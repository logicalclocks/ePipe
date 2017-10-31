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
 * File:   MetadataLogTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "MetadataLogTailer.h"

using namespace Utils::NdbC;

const string _metalog_table= "meta_log";
const int _metalog_noCols= 6;
const string _metalog_cols[_metalog_noCols]=
    {"id",
     "meta_pk1",
     "meta_pk2",
     "meta_pk3",
     "meta_type",
     "meta_op_type"
    };

const int _metalog_noEvents = 1; 
const NdbDictionary::Event::TableEvent _metalog_events[_metalog_noEvents] = { NdbDictionary::Event::TE_INSERT };

const WatchTable MetadataLogTailer::TABLE = {_metalog_table, _metalog_cols, _metalog_noCols , _metalog_events, _metalog_noEvents, "PRIMARY"};


//Common
const int METADATA_NO_COLS = 4;

//SchemaBased Metadata Table
const char* SB_METADATA = "meta_data";
const char* SB_METADATA_COLS[METADATA_NO_COLS]=
    {"id",
     "fieldid",
     "tupleid",
     "data"
    };

//Schemaless Metadata Table
const char* NS_METADATA = "meta_data_schemaless";
const char* NS_METADATA_COLS[METADATA_NO_COLS]=
    {"id",
     "inode_id",
     "inode_parent_id",
     "data"
    };

//Indeces
const int METADATA_PK1 = 0;
const int METADATA_PK2 = 1;
const int METADATA_PK3 = 2;
const int METADATA_DATA = 3;


MetadataLogTailer::MetadataLogTailer(Ndb* ndb, const int poll_maxTimeToWait, const Barrier barrier)
    : RCTableTailer<MetadataLogEntry> (ndb, TABLE, poll_maxTimeToWait, barrier) {
   mSchemaBasedQueue = new CMetaQ();
   mSchemalessQueue = new CMetaQ();
}

void MetadataLogTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
    MetadataLogEntry entry;
    entry.mEventCreationTime = Utils::getCurrentTime();
    entry.mId = value[0]->int32_value();
    int PK1 = value[1]->int32_value();
    int PK2 = value[2]->int32_value();
    int PK3 = value[3]->int32_value();
    entry.mMetaPK = MetadataKey(PK1, PK2, PK3);
    entry.mMetaType = static_cast<MetadataType>(value[4]->int8_value());
    entry.mMetaOpType = static_cast<OperationType>(value[5]->int8_value());

    if (entry.mMetaType == Schemabased) {
        mSchemaBasedQueue->push(entry);
    } else if (entry.mMetaType == Schemaless) {
        mSchemalessQueue->push(entry);
    }else{
        LOG_FATAL("Unkown MetadataType " << entry.mMetaType);
        return;
    }

    LOG_TRACE(" push metalog " << entry.mMetaPK.to_string() << " to queue, Op [" << Utils::OperationTypeToStr(entry.mMetaOpType) << "]");
}

MetadataLogEntry MetadataLogTailer::consumeMultiQueue(int queue_id) {
    MetadataLogEntry res;
    if(queue_id == Schemabased){
        mSchemaBasedQueue->wait_and_pop(res);
    }else if(queue_id == Schemaless){
        mSchemalessQueue->wait_and_pop(res);
    }else{
        LOG_FATAL("Unkown Queue Id, It should be either " << Schemabased << " or " << Schemaless << " but got " << queue_id << " instead");
        return res;
    }
    
    LOG_TRACE(" pop metalog [" << res.mId  << "] \n" << res.to_string());
    return res;
}

MetadataLogEntry MetadataLogTailer::consume() {
    MetadataLogEntry res;
    LOG_FATAL("consume shouldn't be called");
    return res;
}

void MetadataLogTailer::removeLogs(Ndb* conn, UISet& pks) {
    const NdbDictionary::Dictionary* database = getDatabase(conn);
    NdbTransaction* transaction = startNdbTransaction(conn);
    const NdbDictionary::Table* log_table = getTable(database, TABLE.mTableName);
    for(UISet::iterator it=pks.begin(); it != pks.end() ; ++it){
        int id = *it;
        NdbOperation* op = getNdbOperation(transaction, log_table);
        
        op->deleteTuple();
        op->equal(_metalog_cols[0].c_str(), id);
        
        LOG_TRACE("Delete log row: " << id);
    }
    executeTransaction(transaction, NdbTransaction::Commit);
    conn->closeTransaction(transaction);
}

SchemabasedMq* MetadataLogTailer::readSchemaBasedMetadataRows(const NdbDictionary::Dictionary* database, 
        NdbTransaction* transaction, MetaQ* batch, UISet& primaryKeys) {
    UMetadataKeyRowMap rows = readMetadataRows(database, transaction, SB_METADATA, batch, 
            SB_METADATA_COLS, METADATA_NO_COLS, METADATA_PK1, METADATA_PK2, METADATA_PK3);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);
    
    SchemabasedMq* res = new SchemabasedMq();
    for (MetaQ::iterator it = batch->begin(); it != batch->end(); ++it) {
        MetadataLogEntry ml = *it;
        primaryKeys.insert(ml.mId);
        if(ml.mMetaOpType == Delete){
            res->push_back(SchemabasedMetadataEntry(ml));
            continue;
        }
                
        Row row = rows[ml.mMetaPK];
        if(row[METADATA_PK1]->int32_value() == ml.mMetaPK.mPK1 
                && row[METADATA_PK2]->int32_value() == ml.mMetaPK.mPK2 
                && row[METADATA_PK3]->int32_value() == ml.mMetaPK.mPK3){
            SchemabasedMetadataEntry entry = SchemabasedMetadataEntry(ml);
            entry.mMetadata = get_string(row[METADATA_DATA]);
            res->push_back(entry);
        }else{
            LOG_WARN("Ignore " << ml.mMetaPK.to_string() << " since it seems to be deleted");
        }
    }
    return res;
}

SchemalessMq* MetadataLogTailer::readSchemalessMetadataRows(const NdbDictionary::Dictionary* database, 
        NdbTransaction* transaction, MetaQ* batch, UISet& primaryKeys) {
    UMetadataKeyRowMap rows = readMetadataRows(database, transaction, NS_METADATA, batch, 
            NS_METADATA_COLS, METADATA_NO_COLS, METADATA_PK1, METADATA_PK2, METADATA_PK3);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);

    SchemalessMq* res = new SchemalessMq();
    for (MetaQ::iterator it = batch->begin(); it != batch->end(); ++it) {
        MetadataLogEntry ml = *it;
        primaryKeys.insert(ml.mId);
        if(ml.mMetaOpType == Delete){
            res->push_back(SchemalessMetadataEntry(ml));
            continue;
        }
                        
        Row row = rows[ml.mMetaPK];
        
        if(row[METADATA_PK1]->int32_value() == ml.mMetaPK.mPK1 
                && row[METADATA_PK2]->int32_value() == ml.mMetaPK.mPK2 
                && row[METADATA_PK3]->int32_value() == ml.mMetaPK.mPK3){
            SchemalessMetadataEntry entry = SchemalessMetadataEntry(ml);
            entry.mJSONData = get_string(row[METADATA_DATA]);
            res->push_back(entry);
        }else{
            LOG_WARN("Ignore " << ml.mMetaPK.to_string() << " since it seems to be deleted");
        }
    }
    return res;
}

UMetadataKeyRowMap MetadataLogTailer::readMetadataRows(const NdbDictionary::Dictionary* database, 
        NdbTransaction* transaction, const char* table_name, MetaQ* batch, const char** columns_to_read, 
        const int columns_count, const int column_pk1, const int column_pk2, const int column_pk3) {

    UMetadataKeyRowMap res;
    const NdbDictionary::Table* table = getTable(database, table_name);

    for (MetaQ::iterator it = batch->begin(); it != batch->end(); ++it) {
        MetadataLogEntry ml = *it;
        if(ml.mMetaOpType == Delete){
            continue;
        }
        
        NdbOperation* op = getNdbOperation(transaction, table);
        op->readTuple(NdbOperation::LM_CommittedRead);
        
        op->equal(columns_to_read[column_pk1], ml.mMetaPK.mPK1);
        op->equal(columns_to_read[column_pk2], ml.mMetaPK.mPK2);
        op->equal(columns_to_read[column_pk3], ml.mMetaPK.mPK3);
        
        for (int c = 0; c < columns_count; c++) {
            NdbRecAttr* col = getNdbOperationValue(op, columns_to_read[c]);
            res[ml.mMetaPK].push_back(col);
        }
        LOG_TRACE("Read " << table_name << " row for [" << ml.mMetaPK.mPK1 << "," << ml.mMetaPK.mPK2 << "," << ml.mMetaPK.mPK3 << "]");
    }
    return res;

}

MetadataLogTailer::~MetadataLogTailer() {
    delete mSchemaBasedQueue;
    delete mSchemalessQueue;
}