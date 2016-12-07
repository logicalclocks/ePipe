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
#include "HopsworksOpsLogTailer.h"

const char* META_FIELDS = "meta_fields";
const int NUM_FIELDS_COLS = 5;
const char* FIELDS_COLS_TO_READ[] = {"fieldid", "name", "tableid", "searchable", "ftype"};
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
const int NUM_TUPLES_COLS = 2;
const char* TUPLES_COLS_TO_READ[] = {"tupleid", "inodeid"};
const int TUPLE_ID_COL = 0;
const int TUPLE_INODE_ID_COL = 1;

const int DONT_EXIST_INT = -1;
const char* DONT_EXIST_STR = "-1";

MetadataReader::MetadataReader(MConn* connections, const int num_readers, const bool hopsworks, 
        ElasticSearch* elastic, ProjectDatasetINodeCache* cache, const int lru_cap) 
            : NdbDataReader<MetadataEntry, MConn>(connections, num_readers, hopsworks, elastic, cache), 
        mFieldsCache(lru_cap, "Field"), mTablesCache(lru_cap, "Table"), mTemplatesCache(lru_cap, "Template"){

}


void MetadataReader::processAddedandDeleted(MConn connection, Mq* data_batch, Bulk& bulk) {
        
    Ndb* metaConn = connection.metadataConnection;
    
    const NdbDictionary::Dictionary* metaDatabase = getDatabase(metaConn);
    NdbTransaction* metaTransaction = startNdbTransaction(metaConn);
    
    UIRowMap tuples = readMetadataColumns(metaDatabase, metaTransaction, data_batch);
    
    if(mHopsworksEnalbed){
        //read inodes to datasets from inodes datatbase to enable
        //parent/child relationship
        refreshProjectDatasetINodeCache(connection.inodeConnection, tuples,
                metaDatabase, metaTransaction);
    }
    
    int last_used_id = data_batch->at(data_batch->size() - 1).mId;
    Recovery::checkpointMetadata(metaDatabase, metaTransaction, last_used_id);
        
    createJSON(tuples, data_batch, bulk);
            
    metaConn->closeTransaction(metaTransaction);
}

UIRowMap MetadataReader::readMetadataColumns(const NdbDictionary::Dictionary* database,
        NdbTransaction* transaction, Mq* data_batch) { 
    
    UISet fields_ids;
    UISet tuple_ids;
    
    for (Mq::iterator it = data_batch->begin(); it != data_batch->end(); ++it) {
       MetadataEntry entry = *it;
       
       if(!mFieldsCache.contains(entry.mFieldId)){
          fields_ids.insert(entry.mFieldId);
       }
       
       tuple_ids.insert(entry.mTupleId);
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
        int fieldId = it->second[FIELD_ID_COL]->int32_value();
        if(it->first != fieldId){
            // TODO: update elastic?!            
            LOG_ERROR("Field " << it->first << " doesn't exist, got fieldId " 
                    << fieldId << " was expecting " << it->first);
            continue;
        }
        
        Field field;
        field.mFieldId = it->second[FIELD_ID_COL]->int32_value();
        field.mName = get_string(it->second[FIELD_NAME_COL]);
        field.mSearchable = it->second[FIELD_SEARCHABLE_COL]->int8_value() == 1;
        field.mTableId = it->second[FIELD_TABLE_ID_COL]->int32_value();
        field.mType = (FieldType) it->second[FIELD_TYPE_COL]->short_value();
        mFieldsCache.put(it->first, field);
        
        if(field.mSearchable && !mTablesCache.contains(field.mTableId)){
            tables_to_read.insert(field.mTableId);
        }
    }
    
    return tables_to_read;
}

UISet MetadataReader::readTables(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet tables_ids) {
    
    UISet templates_to_read;
    
    if(tables_ids.empty())
        return templates_to_read;
        
    UIRowMap tables = readTableWithIntPK(database, transaction, META_TABLES, 
            tables_ids,TABLES_COLS_TO_READ, NUM_TABLES_COLS, TABLE_ID_COL);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);

    for(UIRowMap::iterator it=tables.begin(); it != tables.end(); ++it){
        int tableId = it->second[TABLE_ID_COL]->int32_value();
        if(it->first != tableId){
            //TODO: update elastic?!
            LOG_ERROR("Table " << it->first << " doesn't exist, got tableId " 
                    << tableId << " was expecting " << it->first);
            continue;
        }
        
        Table table;
        table.mName = get_string(it->second[TABLE_NAME_COL]);
        table.mTemplateId = it->second[TABLE_TEMPLATE_ID_COL]->int32_value();
        
        mTablesCache.put(it->first, table);
        
        if(!mTemplatesCache.contains(table.mTemplateId)){
            templates_to_read.insert(table.mTemplateId);
        }
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
        int templateId = it->second[TEMPLATE_ID_COL]->int32_value(); 
        if(it->first != templateId){
            //TODO: update elastic?!
            LOG_ERROR("Template " << it->first << " doesn't exist, got templateId " 
                    << templateId << " was expecting " << it->first);
            continue;
        }
        
        string templateName = get_string(it->second[TEMPLATE_NAME_COL]);
        
        mTemplatesCache.put(it->first, templateName);
    }
}

void MetadataReader::refreshProjectDatasetINodeCache(SConn inode_connection, UIRowMap tuples, 
        const NdbDictionary::Dictionary* metaDatabase, NdbTransaction* metaTransaction) {

    UISet inodes_ids;
    
    for (UIRowMap::iterator it = tuples.begin(); it != tuples.end(); ++it) {
        int tupleId = it->second[TUPLE_ID_COL]->int32_value();
        if (it->first != tupleId) {
            //TODO: update elastic?!
            LOG_ERROR("Tuple " << it->first << " doesn't exist, got tupleId " 
                    << tupleId << " was expecting " << it->first);
            continue;
        }

        int inodeId = it->second[TUPLE_INODE_ID_COL]->int32_value();
        inodes_ids.insert(inodeId);
    }

    UISet dataset_ids = HopsworksOpsLogTailer::refreshDatasetIds(inode_connection, inodes_ids, mPDICache);
    if (!dataset_ids.empty()) {
        HopsworksOpsLogTailer::refreshProjectIds(metaDatabase, metaTransaction, dataset_ids, mPDICache);
    }
}

void MetadataReader::createJSON(UIRowMap tuples, Mq* data_batch, Bulk& bulk) {

    vector<ptime> arrivalTimes(data_batch->size());
    stringstream out;
    int i=0;
    for(Mq::iterator it=data_batch->begin(); it != data_batch->end(); ++it, i++){
        MetadataEntry entry = *it;
        arrivalTimes[i] = entry.mEventCreationTime;
        
        boost::optional<Field> _fres = mFieldsCache.get(entry.mFieldId);
        
        if(!_fres){
            LOG_ERROR(" Field " << entry.mFieldId << " is not in the cache");
            continue;
        }
        
        Field field = *_fres;
        boost::optional<Table> _tres = mTablesCache.get(field.mTableId);
        
        if (!_tres) {
            LOG_ERROR(" Table " << field.mTableId << " is not in the cache");
            continue;
        }

        Table table = *_tres;
        
        boost::optional<string> _ttres = mTemplatesCache.get(table.mTemplateId);

        if (!_ttres) {
            LOG_ERROR(" Template " << table.mTemplateId << " is not in the cache");
            continue;
        }
        
        string templateName = *_ttres;
        
        int inodeId = tuples[entry.mTupleId][TUPLE_INODE_ID_COL]->int32_value();

         // INode Operation
        rapidjson::StringBuffer sbOp;
        rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

        opWriter.StartObject();

        opWriter.String("update");
        opWriter.StartObject();


        if(mHopsworksEnalbed){
            int datasetId = mPDICache->getDatasetId(inodeId);
            // set project (rounting) and dataset (parent) ids 
            opWriter.String("_parent");
            opWriter.Int(datasetId);

            opWriter.String("_routing");
            opWriter.Int(mPDICache->getProjectId(datasetId));  
            
            if(datasetId == inodeId){
                opWriter.String("_type");
                opWriter.String(mElasticSearch->getDatasetType());
            }
            
        }

        opWriter.String("_id");
        opWriter.Int(inodeId);
        
        opWriter.EndObject();
        
        out << sbOp.GetString() << endl;
        
        rapidjson::StringBuffer sbDoc;
        rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);

        docWriter.StartObject();
        docWriter.String("doc");
        docWriter.StartObject();

        docWriter.String("xattr");
        docWriter.StartObject();
        
        docWriter.String(templateName.c_str());
        docWriter.StartObject();
        
        docWriter.String(table.mName.c_str());
        docWriter.StartObject();
        
        docWriter.String(field.mName.c_str());

        switch (field.mType) {
            case BOOL:
            {
                bool boolVal = entry.mMetadata == "true" || entry.mMetadata == "1";
                docWriter.Bool(boolVal);
                break;
            }
            case INT:
            {
                try {
                    int intVal = entry.mOperation == Delete ? DONT_EXIST_INT : boost::lexical_cast<int>(entry.mMetadata);
                    docWriter.Int(intVal);
                } catch (boost::bad_lexical_cast &e) {
                    LOG_ERROR("Error while casting [" << entry.mMetadata << "] to int" << e.what());
                }

                break;
            }
            case DOUBLE:
            {
                try {
                    double doubleVal = entry.mOperation == Delete ? DONT_EXIST_INT : boost::lexical_cast<double>(entry.mMetadata);
                    docWriter.Double(doubleVal);
                } catch (boost::bad_lexical_cast &e) {
                    LOG_ERROR("Error while casting [" << entry.mMetadata << "] to double" << e.what());
                }

                break;
            }
            case TEXT:
            {
                docWriter.String(entry.mOperation == Delete ? DONT_EXIST_STR : entry.mMetadata.c_str());
                break;
            }
        }
        
        docWriter.EndObject();
         
        docWriter.EndObject();
        
        docWriter.EndObject();
        docWriter.EndObject();

        docWriter.String("doc_as_upsert");
        docWriter.Bool(true);

        docWriter.EndObject();

        out << sbDoc.GetString() << endl;
    }
    
    bulk.mArrivalTimes = arrivalTimes;
    bulk.mJSON = out.str();
}

MetadataReader::~MetadataReader() {
    for(int i=0; i< mNumReaders; i++){
        delete mNdbConnections[i].inodeConnection;
        delete mNdbConnections[i].metadataConnection;
    }
}

