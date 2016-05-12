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

const char* META_FIELDS = "meta_fields";
const int NUM_FIELDS_COLS = 5;
const char* FIELDS_COLS_TO_READ[] = {"fieldid", "name", "tableid", "searchable", "type"};
const int FIELD_ID_COL = 0;
const int FIELD_NAME_COL = 1;
const int FIELD_TABLE_ID_COL = 2;
const int FIELD_SEARCHABLE_COL = 3;
const int FIELD_TYPE_COL = 4;

const char* META_TABLES = "meta_tables";
const int NUM_TABLES_COLS = 3;
const char* TABLES_COLS_TO_READ[] = {"tableid", "name", "templateid"};
const int TABLE_ID_COL = 0;
const int TABLE_NAME_COL = 1;
const int TABLE_TEMPLATE_ID_COL = 2;

const char* META_TEMPLATES = "meta_templates";
const int NUM_TEMPLATE_COLS = 2;
const char* TEMPLATE_COLS_TO_READ[] = {"templateid", "name"};
const int TEMPLATE_ID_COL = 0;
const int TEMPLATE_NAME_COL = 1;


const char* META_TUPLE_TO_FILE = "meta_tuple_to_file";
const int NUM_TUPLES_COLS = 4;
const char* TUPLES_COLS_TO_READ[] = {"tupleid", "inodeid", "inode_pid", "inode_name"};
const int TUPLE_ID_COL = 0;
const int TUPLE_INODE_ID_COL = 1;
const int TUPLE_INODE_PID_COL = 2;
const int TUPLE_INODE_NAME_COL = 3;

MetadataReader::MetadataReader(Ndb** connections, const int num_readers,  std::string elastic_ip) : NdbDataReader(connections, num_readers, elastic_ip) {

}

ReadTimes MetadataReader::readData(Ndb* connection, Mq_Mq data_batch) {
    Mq* added = data_batch.added;
    
    const NdbDictionary::Dictionary* database = getDatabase(connection);
    NdbTransaction* ts = startNdbTransaction(connection);
    
    UIRowMap tuples = readMetadataColumns(database, ts, added);
    
    string data = createJSON(tuples, added);
    
    LOG_INFO() << " Out :: " << endl << data << endl;
    
    connection->closeTransaction(ts);
   
}

UIRowMap MetadataReader::readMetadataColumns(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, Mq* added) { 
    
    UISet fields_ids;
    UISet tuple_ids;
    
    for(int i=0; i<added->size(); i++){
        //TODO: check in the cache
        fields_ids.insert(added->at(i).mFieldId);
        tuple_ids.insert(added->at(i).mTupleId);
    }
    
    
    UISet tables_to_read = readFields(database, transaction, fields_ids);
    
    UISet templates_to_read = readTables(database, transaction, tables_to_read);
    
    readTemplates(database, transaction, templates_to_read);
    
    // Read the tuples
    UIRowMap tuples = readTableWithIntPK(database, transaction, META_TUPLE_TO_FILE, 
            tuple_ids, TUPLES_COLS_TO_READ, NUM_TUPLES_COLS, TUPLE_ID_COL);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);
    
    return tuples;
}

UISet MetadataReader::readFields(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet fields_ids) {
    
    UISet tables_to_read;
    
    if(fields_ids.empty())
        return tables_to_read;
    
    UIRowMap fields = readTableWithIntPK(database, transaction, META_FIELDS, 
            fields_ids, FIELDS_COLS_TO_READ, NUM_FIELDS_COLS, FIELD_ID_COL);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);
    
    for(UIRowMap::iterator it=fields.begin(); it != fields.end(); ++it){
        if(it->first != it->second[FIELD_ID_COL]->int32_value()){
            // TODO: update elastic?!
            LOG_DEBUG() << " Field " << it->first << " doesn't exists";
            continue;
        }
        
        Field field;
        field.mName = get_string(it->second[FIELD_NAME_COL]);
        field.mSearchable = it->second[FIELD_SEARCHABLE_COL]->u_char_value() == 1;
        field.mTableId = it->second[FIELD_TABLE_ID_COL]->int32_value();
        
        mFieldsCache.put(it->first, field);
        
        //TODO: check in the cache
        if(field.mSearchable){
            tables_to_read.insert(field.mTableId);
        }
    }
    
    return tables_to_read;
}

UISet MetadataReader::readTables(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet tables_ids) {
    
    UISet templates_to_read;
    
    if(tables_ids.empty())
        templates_to_read;
        
    UIRowMap tables = readTableWithIntPK(database, transaction, META_TABLES, 
            tables_ids,TABLES_COLS_TO_READ, NUM_TABLES_COLS, TABLE_ID_COL);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);

    for(UIRowMap::iterator it=tables.begin(); it != tables.end(); ++it){
        if(it->first != it->second[TABLE_ID_COL]->int32_value()){
            //TODO: update elastic?!
            LOG_DEBUG() << " TABLE " << it->first << " doesn't exists";
            continue;
        }
        
        Table table;
        table.mName = get_string(it->second[TABLE_NAME_COL]);
        table.mTemplateId = it->second[TABLE_TEMPLATE_ID_COL]->int32_value();
        
        mTablesCache.put(it->first, table);
        
        //TODO: check in the cache
        templates_to_read.insert(table.mTemplateId);
    }
    
    return templates_to_read;
}

void MetadataReader::readTemplates(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet templates_ids) {

    if(templates_ids.empty())
        return;
    
    UIRowMap templates = readTableWithIntPK(database, transaction, META_TEMPLATES, 
            templates_ids, TEMPLATE_COLS_TO_READ, NUM_TEMPLATE_COLS, TEMPLATE_ID_COL);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);
    
     for(UIRowMap::iterator it=templates.begin(); it != templates.end(); ++it){
        if(it->first != it->second[TEMPLATE_ID_COL]->int32_value()){
            //TODO: update elastic?!
            LOG_DEBUG() << " TEMPLATE " << it->first << " doesn't exists";
            continue;
        }
        
        string templateName = get_string(it->second[TEMPLATE_NAME_COL]);
        
        //TODO: check in the cache
        mTemplatesCache.put(it->first, templateName);
    }
}

string MetadataReader::createJSON(UIRowMap tuples, Mq* added) {
    
    stringstream out;
   
    for(Mq::iterator it= added->begin(); it != added->end(); ++it){
        MetadataRow row = *it;
        Row tuple = tuples[row.mTupleId];
        if(tuple[TUPLE_ID_COL]->int32_value() != row.mTupleId){
             LOG_INFO() << " Data for " << row.mTupleId << ", " << row.mFieldId << " not found";
             continue;
        }
        
        if(!mFieldsCache.contains(row.mFieldId))
            continue;
            
        Field field = mFieldsCache.get(row.mFieldId);
        
        if(!mTablesCache.contains(field.mTableId))
            continue;
        
        Table table = mTablesCache.get(field.mTableId);
        
        if(!mTemplatesCache.contains(table.mTemplateId))
            continue;
        
        string templateName = mTemplatesCache.get(table.mTemplateId);
        
    }
}

MetadataReader::~MetadataReader() {
}

