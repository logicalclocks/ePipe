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
 * File:   DatasetTableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "DatasetTableTailer.h"

using namespace Utils::NdbC;

const string _dataset_table= "dataset";
const int _dataset_noCols= 9;
const string _dataset_cols[_dataset_noCols]=
    {"inode_id",
     "inode_pid",
     "inode_name",
     "projectId",
     "description",
     "public_ds",
     "searchable",
     "shared",
     "id"
    };

const int _dataset_noEvents = 3; 
const NdbDictionary::Event::TableEvent _dataset_events[_dataset_noEvents] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE, 
      NdbDictionary::Event::TE_DELETE
    };

const WatchTable DatasetTableTailer::TABLE = {_dataset_table, _dataset_cols, _dataset_noCols , _dataset_events, _dataset_noEvents, "inode_id", "inode_id"};

const int DS_INODE_ID = 0;
const int DS_INODE_PID = 1;
const int DS_INODE_NAME = 2;
const int DS_PROJ_ID = 3;
const int DS_DESC = 4;
const int DS_PUBLIC = 5;
const int DS_SEARCH = 6;
const int DS_SHARED = 7;
const int DS_ID_PK = 8;


//INode->Dataset Lookup Table
const char* INODE_DATASET_LOOKUP = "hdfs_inode_dataset_lookup";
const int NUM_INODE_DATASET_COLS = 2;
const char* INODE_DATASET_LOOKUP_COLS_TO_READ[] = {"inode_id", "dataset_id"};
const int INODE_DATASET_LOOKUP_INODE_ID_COL = 0;
const int INODE_DATASET_LOOKUO_DATASET_ID_COL = 1;


DatasetTableTailer::DatasetTableTailer(Ndb* ndb, const int poll_maxTimeToWait, 
        ElasticSearch* elastic,ProjectDatasetINodeCache* cache) 
    : TableTailer(ndb, TABLE, poll_maxTimeToWait), mElasticSearch(elastic), mPDICache(cache){
}

void DatasetTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
    int datasetId = value[DS_INODE_ID]->int32_value();
    int projectId = value[DS_PROJ_ID]->int32_value();
    bool originalDS = value[DS_SHARED]->int8_value() == 0;

    if (!originalDS && value[DS_SHARED]->isNULL() != -1) {
        LOG_DEBUG("Ignore shared Dataset [" << datasetId << "] in Project [" << projectId << "]");
        return;
    }

    switch (eventType) {
        case NdbDictionary::Event::TE_INSERT:
            handleAdd(datasetId, projectId, value);
            break;
        case NdbDictionary::Event::TE_DELETE:
            handleDelete(datasetId, projectId);
            break;
        case NdbDictionary::Event::TE_UPDATE:
            handleUpdate(value);
            break;
    }
}

void DatasetTableTailer::handleAdd(int datasetId, int projectId, NdbRecAttr* value[]) {
    mPDICache->addDatasetToProject(datasetId, projectId);
    
    string data = createJSONUpSert(projectId, value);
    if (mElasticSearch->addDataset(projectId, datasetId, data)) {
        LOG_INFO("Add Dataset[" << datasetId << "]: Succeeded");
    }

    const NdbDictionary::Dictionary* database = getDatabase(mNdbConnection);
    NdbTransaction* transaction = startNdbTransaction(mNdbConnection);
    Recovery::checkpointDataset(database, transaction, datasetId);
    transaction->close();
}

void DatasetTableTailer::handleDelete(int datasetId, int projectId) {
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();
    docWriter.String("query");

    docWriter.StartObject();

    docWriter.String("match");
    docWriter.StartObject();
    docWriter.String("dataset_id");
    docWriter.Int(datasetId);
    docWriter.EndObject();

    docWriter.EndObject();

    docWriter.EndObject();

    //TODO: handle failures in elastic search
    if (mElasticSearch->deleteDatasetChildren(projectId, datasetId, string(sbDoc.GetString()))) {
        LOG_INFO("Delete Dataset[" << datasetId << "] children inodes: Succeeded");
    }

    if (mElasticSearch->deleteDataset(projectId, datasetId)) {
        LOG_INFO("Delete Dataset[" << datasetId << "]: Succeeded");
    }

    mPDICache->removeDataset(datasetId);
}

void DatasetTableTailer::handleUpdate(NdbRecAttr* value[]) {
    int datasetPK = value[DS_ID_PK]->int32_value();
    int datasetId = -1;
    int projectId = -1;
    if(value[DS_INODE_ID]->isNULL() == -1){
        const NdbDictionary::Dictionary* database = getDatabase(mNdbConnection);
        const NdbDictionary::Table* table = getTable(database, TABLE.mTableName);
        NdbTransaction* transaction = startNdbTransaction(mNdbConnection);
        NdbOperation* op = getNdbOperation(transaction, table);
        
        op->readTuple(NdbOperation::LM_CommittedRead);
        
        op->equal(_dataset_cols[8].c_str(), datasetPK);
        
        NdbRecAttr* datasetIdCol = getNdbOperationValue(op, _dataset_cols[DS_INODE_ID]);
        NdbRecAttr* projectIdCol = getNdbOperationValue(op, _dataset_cols[DS_PROJ_ID]);
        
        executeTransaction(transaction, NdbTransaction::Commit);        
        datasetId = datasetIdCol->int32_value();
        projectId = projectIdCol->int32_value();
        
        transaction->close();
    }
    
    if(datasetId == -1 || projectId == -1){
        LOG_ERROR("Couldn't resolve projectId[" << projectId << "] or datasetId[" << datasetId << "]");
        return;
    }
    
     
    string data = createJSONUpSert(projectId, value);
    if (mElasticSearch->addDataset(projectId, datasetId, data)) {
        LOG_INFO("Update Dataset[" << datasetId << "]: Succeeded");
    }
}

string DatasetTableTailer::createJSONUpSert(int porjectId, NdbRecAttr* value[]) {
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();

    docWriter.String("doc");
    docWriter.StartObject();

    if (value[DS_INODE_PID]->isNULL() != -1) {
        docWriter.String("parent_id");
        docWriter.Int(value[DS_INODE_PID]->int32_value());
    }

    if (value[DS_INODE_NAME]->isNULL() != -1) {
        docWriter.String("name");
        docWriter.String(get_string(value[DS_INODE_NAME]).c_str());
    }

    docWriter.String("project_id");
    docWriter.Int(porjectId);

    if (value[DS_DESC]->isNULL() != -1) {
        docWriter.String("description");
        docWriter.String(get_string(value[DS_DESC]).c_str());
    }

    if (value[DS_PUBLIC]->isNULL() != -1) {
        bool public_ds = value[DS_PUBLIC]->int8_value() == 1;
        docWriter.String("public_ds");
        docWriter.Bool(public_ds);
    }

    if (value[DS_SEARCH]->isNULL() != -1) {
        bool searchable = value[DS_SEARCH]->int8_value() == 1;
        docWriter.String("searchable");
        docWriter.Bool(searchable);
    }

    docWriter.EndObject();
    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);
    docWriter.EndObject();
    return string(sbDoc.GetString());
}

void DatasetTableTailer::refreshCache(MConn connection, UISet inodes, ProjectDatasetINodeCache* cache){
    UISet datasets_ids = refreshDatasetIds(connection.inodeConnection, inodes, cache);
    if(!datasets_ids.empty()){
        refreshProjectIds(connection.metadataConnection, datasets_ids, cache);
    }
}

UISet DatasetTableTailer::refreshDatasetIds(SConn inode_connection, UISet inodes, ProjectDatasetINodeCache* cache) {
    UISet inodes_ids_to_read;
    UISet dataset_ids;

    for (UISet::iterator it = inodes.begin(); it != inodes.end(); ++it) {

        int inodeId = *it;

        int datasetId = cache->getDatasetId(inodeId);
        if (datasetId == -1) {
            inodes_ids_to_read.insert(inodeId);
            continue;
        }

        int projectId = cache->getProjectId(datasetId);
        if (projectId == -1) {
            dataset_ids.insert(datasetId);
        }
    }


    if (!inodes_ids_to_read.empty()) {
        const NdbDictionary::Dictionary* inodeDatabase = getDatabase(inode_connection);
        NdbTransaction* inodeTransaction = startNdbTransaction(inode_connection);

        readINodeToDatasetLookup(inodeDatabase, inodeTransaction,
                inodes_ids_to_read, dataset_ids, cache);

        executeTransaction(inodeTransaction, NdbTransaction::NoCommit);
        inode_connection->closeTransaction(inodeTransaction);
    }
    
    return dataset_ids;
}

void DatasetTableTailer::readINodeToDatasetLookup(const NdbDictionary::Dictionary* inodesDatabase,
        NdbTransaction* inodesTransaction, UISet inodes_ids, UISet& datasets_to_read, ProjectDatasetINodeCache* cache) {

    UIRowMap inodesToDatasets = readTableWithIntPK(inodesDatabase, inodesTransaction,
            INODE_DATASET_LOOKUP, inodes_ids, INODE_DATASET_LOOKUP_COLS_TO_READ,
            NUM_INODE_DATASET_COLS, INODE_DATASET_LOOKUP_INODE_ID_COL);

    executeTransaction(inodesTransaction, NdbTransaction::NoCommit);

    for (UIRowMap::iterator it = inodesToDatasets.begin(); it != inodesToDatasets.end(); ++it) {
        int inodeId = it->second[INODE_DATASET_LOOKUP_INODE_ID_COL]->int32_value();
        if (it->first != inodeId) {
            //TODO: update elastic?!
            LOG_ERROR("INodeToDataset " << it->first << " doesn't exist, got inodeId "
                    << inodeId << " was expecting " << it->first);
            continue;
        }

        int datasetId = it->second[INODE_DATASET_LOOKUO_DATASET_ID_COL]->int32_value();

        cache->addINodeToDataset(it->first, datasetId);
        if (!cache->containsDataset(datasetId)) {
            datasets_to_read.insert(datasetId);
        }
    }

}

void DatasetTableTailer::refreshProjectIds(SConn connection, UISet datasets, ProjectDatasetINodeCache* cache) {
    const NdbDictionary::Dictionary* metaDatabase = getDatabase(connection);
    NdbTransaction* metaTransaction = startNdbTransaction(connection);
    refreshProjectIds(metaDatabase, metaTransaction, datasets, cache);
    connection->closeTransaction(metaTransaction);
}

void DatasetTableTailer::refreshProjectIds(const NdbDictionary::Dictionary* database,
        NdbTransaction* transaction, UISet dataset_ids, ProjectDatasetINodeCache* cache) {    

    const NdbDictionary::Index * index= getIndex(database, TABLE.mTableName, _dataset_cols[0]);
    
    vector<NdbIndexScanOperation*> indexScanOps;
    UIRowMap rows;
    for (UISet::iterator it = dataset_ids.begin(); it != dataset_ids.end(); ++it) {
        NdbIndexScanOperation* op = getNdbIndexScanOperation(transaction, index);
        op->readTuples(NdbOperation::LM_CommittedRead);
       
        op->equal(_dataset_cols[DS_INODE_ID].c_str(), *it);
        
        NdbRecAttr* id_col = getNdbOperationValue(op, _dataset_cols[DS_INODE_ID]);
        NdbRecAttr* proj_id_col = getNdbOperationValue(op, _dataset_cols[DS_PROJ_ID]);
        NdbRecAttr* shared_col = getNdbOperationValue(op, _dataset_cols[DS_SHARED]);
        rows[*it].push_back(id_col);
        rows[*it].push_back(proj_id_col);
        rows[*it].push_back(shared_col);
        indexScanOps.push_back(op);
    }

    executeTransaction(transaction, NdbTransaction::NoCommit);

    int i = 0;
    for (UIRowMap::iterator it = rows.begin(); it != rows.end(); ++it, i++) {
        
        stringstream projs;
        int res=0;
        while (indexScanOps[i]->nextResult(true) == 0) {
            if (it->first != it->second[0]->int32_value()) {
                LOG_ERROR("Dataset [" << it->first << "] doesn't exists");
                continue;
            }
            
            bool originalDs = it->second[2]->int8_value() == 0;
            
            if(!originalDs){
                continue;
            }
            
            int projectId = it->second[1]->int32_value();
            if(res == 0){
                cache->addDatasetToProject(it->first, projectId);
            }
            projs << projectId << ",";
            res++;
        }
        
        if(res > 1){
            LOG_FATAL("Got " << res << " rows of the original Dataset [" << it->first << "] in projects [" << projs.str() << "], only one was expected");
        }
    }
}

DatasetTableTailer::~DatasetTableTailer() {
}

