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
 * File:   HopsworksOpsLogTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "HopsworksOpsLogTailer.h"
using namespace Utils;
using namespace Utils::NdbC;

const string _opslog_table= "ops_log";
const int _opslog_noCols= 6;
const string _opslog_cols[_opslog_noCols]=
    {
     "id",
     "op_id",
     "op_on",
     "op_type",
     "project_id",
     "inode_id"
    };

const int _opslog_noEvents = 1; 
const NdbDictionary::Event::TableEvent _opslog_events[_opslog_noEvents] = { NdbDictionary::Event::TE_INSERT};

const WatchTable HopsworksOpsLogTailer::TABLE = {_opslog_table, _opslog_cols, _opslog_noCols , _opslog_events, _opslog_noEvents, "PRIMARY", "id"};

const int OPS_ID_PK = 0;
const int OPS_OP_ID = 1;
const int OPS_OP_ON = 2;
const int OPS_OP_TYPE = 3;
const int OPS_PROJECT_ID = 4;
const int OPS_INODE_ID = 5;

//Dataset Table
const char* DS = "dataset";
const int NUM_DS_COLS = 7;
const char* DS_COLS_TO_READ[] =    
   { "id",
     "inode_id",
     "inode_pid",
     "inode_name",
     "projectId",
     "description",
     "public_ds"
    };

const int DS_PK = 0;
const int DS_INODE_ID = 1;
const int DS_INODE_PARENT_ID = 2;
const int DS_INODE_NAME = 3;
const int DS_PROJECT_ID = 4;
const int DS_DESC = 5;
const int DS_PUBLIC = 6;


//Project Table
const char* PR = "project";
const int NUM_PR_COLS = 5;
const char* PR_COLS_TO_READ[] =    
   { "id",
     "inode_pid",
     "inode_name",
     "username",
     "description"
    };

const int PR_PK= 0;
const int PR_INODE_PID = 1;
const int PR_INODE_NAME = 2;
const int PR_USER = 3;
const int PR_DESC = 4;


//INode->Dataset Lookup Table
const char* INODE_DATASET_LOOKUP = "hdfs_inode_dataset_lookup";
const int NUM_INODE_DATASET_COLS = 2;
const char* INODE_DATASET_LOOKUP_COLS_TO_READ[] = {"inode_id", "dataset_id"};
const int INODE_DATASET_LOOKUP_INODE_ID_COL = 0;
const int INODE_DATASET_LOOKUO_DATASET_ID_COL = 1;



HopsworksOpsLogTailer::HopsworksOpsLogTailer(Ndb* ndb, const int poll_maxTimeToWait, 
        ElasticSearch* elastic, ProjectDatasetINodeCache* cache) 
    : TableTailer(ndb, TABLE, poll_maxTimeToWait), mElasticSearch(elastic), mPDICache(cache){
    
}

void HopsworksOpsLogTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {

    int id = value[OPS_ID_PK]->int32_value();
    int op_id = value[OPS_OP_ID]->int32_value();
    OpsLogOn op_on = static_cast<OpsLogOn>(value[OPS_OP_ON]->int8_value());
    OperationType op_type = static_cast<OperationType>(value[OPS_OP_TYPE]->int8_value());
    int project_id = value[OPS_PROJECT_ID]->int32_value();
    int inode_id = value[OPS_INODE_ID]->int32_value();
    LOG_DEBUG(endl << "ID = " << id << endl << "OpID = " << op_id << endl 
            << "OpOn = " << OpsLogOnToStr(op_on) << endl
            << "OpType = " << OperationTypeToStr(op_type) << endl
            << "ProjectID = " << project_id << endl
            << "INodeID = " << inode_id);
    
    switch(op_on){
        case Dataset:
            handleDataset(op_id, op_type, inode_id, project_id);
            break;
        case Project:
            handleProject(op_id, op_type);
            break;
    }
    
    removeLog(id);
}

void HopsworksOpsLogTailer::handleDataset(int opId, OperationType opType, 
        int datasetId, int projectId) {
    if(opType == Delete){
       handleDeleteDataset(datasetId, projectId);
    }else{
       handleUpsertDataset(opId, opType, datasetId, projectId);
    }
}

void HopsworksOpsLogTailer::handleUpsertDataset(int opId, OperationType opType, int datasetId, int projectId){
    mPDICache->addDatasetToProject(datasetId, projectId);

    const NdbDictionary::Dictionary* database = getDatabase(mNdbConnection);
    const NdbDictionary::Table* table = getTable(database, DS);
    NdbTransaction* transaction = startNdbTransaction(mNdbConnection);
    
    NdbOperation* op = getNdbOperation(transaction, table);

    op->readTuple(NdbOperation::LM_CommittedRead);

    op->equal(DS_COLS_TO_READ[DS_PK], opId);
    NdbRecAttr** row = new NdbRecAttr*[NUM_DS_COLS];
    for(int i=0; i<NUM_DS_COLS; i++){
        row[i] = getNdbOperationValue(op, DS_COLS_TO_READ[i]);
    }
    
    executeTransaction(transaction, NdbTransaction::Commit);
    
    string data = createDatasetJSONUpSert(projectId, row);
    if (mElasticSearch->addDataset(projectId, datasetId, data)) {
        switch(opType){
            case Add:
                LOG_INFO("Add Dataset[" << datasetId << "]: Succeeded");
                break;
            case Update:
                LOG_INFO("Update Dataset[" << datasetId << "]: Succeeded");
                break;
        }
    }
    
    transaction->close();
}

string HopsworksOpsLogTailer::createDatasetJSONUpSert(int porjectId, NdbRecAttr* value[]) {
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();

    docWriter.String("doc");
    docWriter.StartObject();

    if (value[DS_INODE_PARENT_ID]->isNULL() != -1) {
        docWriter.String("parent_id");
        docWriter.Int(value[DS_INODE_PARENT_ID]->int32_value());
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

    docWriter.EndObject();
    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);
    docWriter.EndObject();
    return string(sbDoc.GetString());
}

void HopsworksOpsLogTailer::handleDeleteDataset(int datasetId, int projectId) {
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

void HopsworksOpsLogTailer::handleProject(int projectId, OperationType opType) {
    if (opType == Delete) {
        handleDeleteProject(projectId);
    } else {
        handleUpsertProject(projectId, opType);
    }
}

void HopsworksOpsLogTailer::handleDeleteProject(int projectId) {
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();
    docWriter.String("query");

    docWriter.StartObject();

    docWriter.String("match");
    docWriter.StartObject();
    docWriter.String("project_id");
    docWriter.Int(projectId);
    docWriter.EndObject();

    docWriter.EndObject();

    docWriter.EndObject();

    //TODO: handle failures in elastic search
    if (mElasticSearch->deleteProjectChildren(projectId, string(sbDoc.GetString()))) {
        LOG_INFO("Delete Project[" << projectId << "] children inodes and datasets : Succeeded");
    }

    if (mElasticSearch->deleteProject(projectId)) {
        LOG_INFO("Delete Project[" << projectId << "]: Succeeded");
    }

    mPDICache->removeProject(projectId);
}

void HopsworksOpsLogTailer::handleUpsertProject(int projectId, OperationType opType){

    const NdbDictionary::Dictionary* database = getDatabase(mNdbConnection);
    const NdbDictionary::Table* table = getTable(database, PR);
    NdbTransaction* transaction = startNdbTransaction(mNdbConnection);
    
    NdbOperation* op = getNdbOperation(transaction, table);

    op->readTuple(NdbOperation::LM_CommittedRead);

    op->equal(PR_COLS_TO_READ[PR_PK], projectId);
    NdbRecAttr** row = new NdbRecAttr*[NUM_PR_COLS];
    for(int i=0; i<NUM_PR_COLS; i++){
        row[i] = getNdbOperationValue(op, PR_COLS_TO_READ[i]);
    }
    
    executeTransaction(transaction, NdbTransaction::Commit);
    
    string data = createProjectJSONUpSert(row);
   if (mElasticSearch->addProject(projectId, data)) {
         switch(opType){
            case Add:
                LOG_INFO("Add Project[" << projectId << "]: Succeeded");
                break;
            case Update:
                LOG_INFO("Update Project[" << projectId << "]: Succeeded");
                break;
        }
    }
    
    transaction->close();
        
}

string HopsworksOpsLogTailer::createProjectJSONUpSert(NdbRecAttr* value[]) {
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();

    docWriter.String("doc");
    docWriter.StartObject();

    if (value[PR_INODE_PID]->isNULL() != -1) {
        docWriter.String("parent_id");
        docWriter.Int(value[PR_INODE_PID]->int32_value());
    }

    if (value[PR_INODE_NAME]->isNULL() != -1) {
        docWriter.String("name");
        docWriter.String(get_string(value[PR_INODE_NAME]).c_str());
    }

    if (value[PR_USER]->isNULL() != -1) {
        docWriter.String("user");
        docWriter.String(get_string(value[PR_USER]).c_str());
    }

    if (value[PR_DESC]->isNULL() != -1) {
        docWriter.String("description");
        docWriter.String(get_string(value[PR_DESC]).c_str());
    }

    docWriter.EndObject();
    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);
    docWriter.EndObject();

    return string(sbDoc.GetString());
}

void HopsworksOpsLogTailer::removeLog(int pk){
     const NdbDictionary::Dictionary* database = getDatabase(mNdbConnection);    
     const NdbDictionary::Table* log_table = getTable(database, TABLE.mTableName);
     NdbTransaction* transaction = startNdbTransaction(mNdbConnection);
     NdbOperation* op = getNdbOperation(transaction, log_table);  
     op->deleteTuple();
     op->equal(_opslog_cols[OPS_ID_PK].c_str(), pk);
     LOG_TRACE("Delete log row: [" << pk << "]");
     executeTransaction(transaction, NdbTransaction::Commit);
     mNdbConnection->closeTransaction(transaction);
}


void HopsworksOpsLogTailer::refreshCache(MConn connection, UISet inodes, ProjectDatasetINodeCache* cache){
    UISet datasets_ids = refreshDatasetIds(connection.inodeConnection, inodes, cache);
    if(!datasets_ids.empty()){
        refreshProjectIds(connection.metadataConnection, datasets_ids, cache);
    }
}

UISet HopsworksOpsLogTailer::refreshDatasetIds(SConn inode_connection, UISet inodes, ProjectDatasetINodeCache* cache) {
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

void HopsworksOpsLogTailer::readINodeToDatasetLookup(const NdbDictionary::Dictionary* inodesDatabase,
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

void HopsworksOpsLogTailer::refreshProjectIds(SConn connection, UISet datasets, ProjectDatasetINodeCache* cache) {
    const NdbDictionary::Dictionary* metaDatabase = getDatabase(connection);
    NdbTransaction* metaTransaction = startNdbTransaction(connection);
    refreshProjectIds(metaDatabase, metaTransaction, datasets, cache);
    connection->closeTransaction(metaTransaction);
}

void HopsworksOpsLogTailer::refreshProjectIds(const NdbDictionary::Dictionary* database,
        NdbTransaction* transaction, UISet dataset_ids, ProjectDatasetINodeCache* cache) {    

    const NdbDictionary::Index * index= getIndex(database, DS, DS_COLS_TO_READ[DS_INODE_ID]);
    
    vector<NdbIndexScanOperation*> indexScanOps;
    UIRowMap rows;
    for (UISet::iterator it = dataset_ids.begin(); it != dataset_ids.end(); ++it) {
        NdbIndexScanOperation* op = getNdbIndexScanOperation(transaction, index);
        op->readTuples(NdbOperation::LM_CommittedRead);
       
        op->equal(DS_COLS_TO_READ[DS_INODE_ID], *it);
        
        NdbRecAttr* id_col = getNdbOperationValue(op, DS_COLS_TO_READ[DS_INODE_ID]);
        NdbRecAttr* proj_id_col = getNdbOperationValue(op, DS_COLS_TO_READ[DS_PROJECT_ID]);
        NdbRecAttr* shared_col = getNdbOperationValue(op, "shared");
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

HopsworksOpsLogTailer::~HopsworksOpsLogTailer() {
    
}

