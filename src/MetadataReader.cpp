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
const int NUM_TUPLES_COLS = 2;
const char* TUPLES_COLS_TO_READ[] = {"tupleid", "inodeid"};
const int TUPLE_ID_COL = 0;
const int TUPLE_INODE_ID_COL = 1;

const char* INODE_DATASET_LOOKUP = "hdfs_inode_dataset_lookup";
const int NUM_INODE_DATASET_COLS = 2;
const char* INODE_DATASET_LOOKUP_COLS_TO_READ[] = {"inode_id", "dataset_id"};
const int INODE_DATASET_LOOKUP_INODE_ID_COL = 0;
const int INODE_DATASET_LOOKUO_DATASET_ID_COL = 1;

MetadataReader::MetadataReader(MConn* connections, const int num_readers,  string elastic_ip, 
        const bool hopsworks, const string elastic_index, const string elastic_inode_type, DatasetProjectCache* cache) 
        : NdbDataReader<Mq_Mq, MConn>(connections, num_readers, elastic_ip, hopsworks, elastic_index, elastic_inode_type, cache) {

}

ReadTimes MetadataReader::readData(MConn connection, Mq_Mq data_batch) {
    Mq* added = data_batch.added;
    Ndb* metaConn = connection.metadataConnection;
    
    ptime t1 = getCurrentTime();
    
    const NdbDictionary::Dictionary* metaDatabase = getDatabase(metaConn);
    NdbTransaction* metaTransaction = startNdbTransaction(metaConn);
    
    UTupleIdToMetadataEntries tupleToEntries;
    UInodesToTemplates inodesToTemplates = readMetadataColumns(metaDatabase, metaTransaction, added, tupleToEntries);
    
    if(mHopsworksEnalbed){
        //read inodes to datasets from inodes datatbase to enable
        //parent/child relationship
        Ndb* inodeConn = connection.inodeConnection;
        
        const NdbDictionary::Dictionary* inodeDatabase = getDatabase(inodeConn);
        NdbTransaction* inodeTransaction = startNdbTransaction(inodeConn);
        
        readINodeToDatasetLookup(inodeDatabase, inodeTransaction, inodesToTemplates);
        
        executeTransaction(inodeTransaction, NdbTransaction::NoCommit);
        inodeConn->closeTransaction(inodeTransaction);
    }
    
    ptime t2 = getCurrentTime();
    
    string data = createJSON(inodesToTemplates, tupleToEntries);
    
    ptime t3 = getCurrentTime();
    
    LOG_INFO() << " Out :: " << endl << data << endl;
    
    metaConn->closeTransaction(metaTransaction);
   
    string resp = bulkUpdateElasticSearch(data);
    
    ptime t4 = getCurrentTime();
    
    LOG_INFO() << " RESP " << resp;
    
    ReadTimes rt;
    rt.mNdbReadTime = getTimeDiffInMilliseconds(t1, t2);
    rt.mJSONCreationTime = getTimeDiffInMilliseconds(t2, t3);
    rt.mElasticSearchTime = getTimeDiffInMilliseconds(t3, t4);
    
    return rt;
}

UInodesToTemplates MetadataReader::readMetadataColumns(const NdbDictionary::Dictionary* database,
        NdbTransaction* transaction, Mq* added, UTupleIdToMetadataEntries &tupleToEntries) { 
    
    UISet fields_ids;
    UISet tuple_ids;
    
    for (Mq::iterator it = added->begin(); it != added->end(); ++it) {
        MetadataEntry entry = *it;
        if (tupleToEntries.find(entry.mTupleId) == tupleToEntries.end()) {
            tupleToEntries[entry.mTupleId] = UFieldIdToMetadataEntry();
        }
        tupleToEntries[entry.mTupleId][entry.mFieldId] = entry;
        
        //TODO: check in the cache
        fields_ids.insert(entry.mFieldId);
        tuple_ids.insert(entry.mTupleId);
    }
    
    UISet tables_to_read = readFields(database, transaction, fields_ids);
    
    UISet templates_to_read = readTables(database, transaction, tables_to_read);
    
    readTemplates(database, transaction, templates_to_read);
    
    // Read the tuples
    UIRowMap tuples = readTableWithIntPK(database, transaction, META_TUPLE_TO_FILE, 
            tuple_ids, TUPLES_COLS_TO_READ, NUM_TUPLES_COLS, TUPLE_ID_COL);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);
    
    UInodesToTemplates inodesToTemplates = getInodesToTemplates(tuples, tupleToEntries);
    
    return inodesToTemplates;
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
        field.mFieldId = it->second[FIELD_ID_COL]->int32_value();
        field.mName = get_string(it->second[FIELD_NAME_COL]);
        field.mSearchable = it->second[FIELD_SEARCHABLE_COL]->int8_value() == 1;
        field.mTableId = it->second[FIELD_TABLE_ID_COL]->int32_value();
        field.mType = (FieldType) it->second[FIELD_TYPE_COL]->short_value();
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
        return templates_to_read;
        
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
        
        mTemplatesCache.put(it->first, templateName);
    }
}

void MetadataReader::readINodeToDatasetLookup(const NdbDictionary::Dictionary* inodesDatabase, 
        NdbTransaction* inodesTransaction, UInodesToTemplates inodesToTemplates) {
    
    //read inodes to dataset ids
    UISet inodes_ids;
    for(UInodesToTemplates::iterator it=inodesToTemplates.begin(); it != inodesToTemplates.end(); ++it){
        //TODO: check if is in the cache
        inodes_ids.insert(it->first);
    }
    
    UIRowMap inodesToDatasets = readTableWithIntPK(inodesDatabase, inodesTransaction,
            INODE_DATASET_LOOKUP, inodes_ids, INODE_DATASET_LOOKUP_COLS_TO_READ,
            NUM_INODE_DATASET_COLS, INODE_DATASET_LOOKUP_INODE_ID_COL);

    executeTransaction(inodesTransaction, NdbTransaction::NoCommit);

    for (UIRowMap::iterator it = inodesToDatasets.begin(); it != inodesToDatasets.end(); ++it) {
        if (it->first != it->second[INODE_DATASET_LOOKUP_INODE_ID_COL]->int32_value()) {
            //TODO: update elastic?!
            LOG_DEBUG() << " TEMPLATE " << it->first << " doesn't exists";
            continue;
        }

        int datasetId = it->second[INODE_DATASET_LOOKUO_DATASET_ID_COL]->int32_value();

        mInodesToDataset.put(it->first, datasetId);
    }
}

string MetadataReader::createJSON(UInodesToTemplates inodesToTemplates, UTupleIdToMetadataEntries tupleToEntries) {
    
    stringstream out;

    for (UInodesToTemplates::iterator it = inodesToTemplates.begin(); it != inodesToTemplates.end(); ++it) {
        int inodeId = it->first;
        UTemplateToTables templatesToTables = it->second;

        // INode Operation
        rapidjson::StringBuffer sbOp;
        rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

        opWriter.StartObject();

        opWriter.String("update");
        opWriter.StartObject();

        opWriter.String("_index");
        opWriter.String(mElasticIndex.c_str());

        opWriter.String("_type");
        opWriter.String(mElasticInodeType.c_str());

        if(mHopsworksEnalbed){
            if(mInodesToDataset.contains(inodeId)){
                int datasetId = mInodesToDataset.get(inodeId);
                // set project (rounting) and dataset (parent) ids 
                opWriter.String("_parent");
                opWriter.Int(datasetId);

                opWriter.String("_routing");
                opWriter.Int(mDatasetProjectCache->getProjectId(datasetId));
                
            }else{
                LOG_ERROR() << "Something went wrong: DatasetId for InodeId[" << inodeId<< "] is not in the cache";
            }
        }

        opWriter.String("_id");
        opWriter.Int(inodeId);

        opWriter.EndObject();


        rapidjson::StringBuffer sbDoc;
        rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);

        docWriter.StartObject();
        docWriter.String("doc");
        docWriter.StartObject();

        docWriter.String("xattr");
        docWriter.StartObject();

        for (UTemplateToTables::iterator templateIt = templatesToTables.begin(); templateIt != templatesToTables.end(); ++templateIt) {
            string templateName = templateIt->first;
            UTableToTuples tablesToTuples = templateIt->second;

            docWriter.String(templateName.c_str());
            docWriter.StartObject();

            for (UTableToTuples::iterator tableIt = tablesToTuples.begin(); tableIt != tablesToTuples.end(); ++tableIt) {
                string tableName = tableIt->first;
                UTupleToFields tupleToFields = tableIt->second;

                docWriter.String(tableName.c_str());
                docWriter.StartObject();

                for (UTupleToFields::iterator tupleIt = tupleToFields.begin(); tupleIt != tupleToFields.end(); ++tupleIt) {
                    int tupleId = tupleIt->first;
                    vector<Field> fields = tupleIt->second;

                    for (vector<Field>::iterator fieldIt = fields.begin(); fieldIt != fields.end(); ++fieldIt) {
                        Field field = *fieldIt;
                        string metadata = tupleToEntries[tupleId][field.mFieldId].mMetadata;

                        if (!field.mSearchable)
                            continue;

                        docWriter.String(field.mName.c_str());

                        switch (field.mType) {
                            case BOOL:
                            {
                                bool boolVal = metadata == "true" || metadata == "1";
                                docWriter.Bool(boolVal);
                                break;
                            }
                            case INT:
                            {
                                try {
                                    int intVal = boost::lexical_cast<int>(metadata);
                                    docWriter.Int(intVal);
                                } catch (boost::bad_lexical_cast &e) {
                                    LOG_ERROR() << "Error while casting [" << metadata << "] to int" << e.what();
                                }

                                break;
                            }
                            case DOUBLE:
                            {
                                try {
                                    double doubleVal = boost::lexical_cast<double>(metadata);
                                    docWriter.Double(doubleVal);
                                } catch (boost::bad_lexical_cast &e) {
                                    LOG_ERROR() << "Error while casting [" << metadata << "] to double" << e.what();
                                }

                                break;
                            }
                            case TEXT:
                            {
                                docWriter.String(metadata.c_str());
                                break;
                            }
                        }

                    }

                }

                docWriter.EndObject();
            }

            docWriter.EndObject();

        }


        docWriter.EndObject();
        docWriter.EndObject();

        docWriter.String("doc_as_upsert");
        docWriter.Bool(true);

        docWriter.EndObject();

        out << sbOp.GetString() << endl << sbDoc.GetString() << endl;
    }

    return out.str();
}

UInodesToTemplates MetadataReader::getInodesToTemplates(UIRowMap tuples, UTupleIdToMetadataEntries tupleToEntries) {

    UInodesToTemplates inodesToTemplates;

    for (UIRowMap::iterator it = tuples.begin(); it != tuples.end(); ++it) {
        int tupleId = it->first;
        int inodeId = it->second[TUPLE_INODE_ID_COL]->int32_value();
          
        UFieldIdToMetadataEntry fields = tupleToEntries[tupleId];

        for (UFieldIdToMetadataEntry::iterator fieldIt = fields.begin(); fieldIt != fields.end(); ++fieldIt) {
            MetadataEntry row = fieldIt->second;
            if (!mFieldsCache.contains(row.mFieldId)) {
                LOG_ERROR() << " Field " << row.mFieldId << " is not in the cache";
                continue;
            }

            Field field = mFieldsCache.get(row.mFieldId);

            if (!mTablesCache.contains(field.mTableId)) {
                LOG_ERROR() << " Table " << field.mTableId << " is not in the cache";
                continue;
            }


            Table table = mTablesCache.get(field.mTableId);

            if (!mTemplatesCache.contains(table.mTemplateId)) {
                LOG_ERROR() << " Template " << table.mTemplateId << " is not in the cache";
                continue;
            }

            string templateName = mTemplatesCache.get(table.mTemplateId);


            if (inodesToTemplates.find(inodeId) == inodesToTemplates.end()) {
                inodesToTemplates[inodeId] = UTemplateToTables();
            }

            if (inodesToTemplates[inodeId].find(templateName) == inodesToTemplates[inodeId].end()) {
                inodesToTemplates[inodeId][templateName] = UTableToTuples();
            }

            if (inodesToTemplates[inodeId][templateName].find(table.mName) == inodesToTemplates[inodeId][templateName].end()) {
                inodesToTemplates[inodeId][templateName][table.mName] = UTupleToFields();
            }

            if (inodesToTemplates[inodeId][templateName][table.mName].find(tupleId) == inodesToTemplates[inodeId][templateName][table.mName].end()) {
                inodesToTemplates[inodeId][templateName][table.mName][tupleId] = vector<Field>();
            }

            inodesToTemplates[inodeId][templateName][table.mName][tupleId].push_back(field);

        }
    }

    return inodesToTemplates;
}


MetadataReader::~MetadataReader() {
    for(int i=0; i< mNumReaders; i++){
        delete mNdbConnections[i].inodeConnection;
        delete mNdbConnections[i].metadataConnection;
    }
}

