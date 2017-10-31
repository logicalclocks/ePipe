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
#include "Tables.h"

using namespace Utils;
using namespace Utils::NdbC;

const string _opslog_table= "ops_log";
const int _opslog_noCols= 7;
const string _opslog_cols[_opslog_noCols]=
    {
     "id",
     "op_id",
     "op_on",
     "op_type",
     "project_id",
     "dataset_id",
     "inode_id"
    };

const int _opslog_noEvents = 1; 
const NdbDictionary::Event::TableEvent _opslog_events[_opslog_noEvents] = { NdbDictionary::Event::TE_INSERT};

const WatchTable HopsworksOpsLogTailer::TABLE = {_opslog_table, _opslog_cols, _opslog_noCols , _opslog_events, _opslog_noEvents, "PRIMARY"};

const int OPS_ID_PK = 0;
const int OPS_OP_ID = 1;
const int OPS_OP_ON = 2;
const int OPS_OP_TYPE = 3;
const int OPS_PROJECT_ID = 4;
const int OPS_DATASET_ID = 5;
const int OPS_INODE_ID = 6;

HopsworksOpsLogTailer::HopsworksOpsLogTailer(Ndb* ndb, const int poll_maxTimeToWait, const Barrier barrier,
        ElasticSearch* elastic, ProjectDatasetINodeCache* cache, SchemaCache* schemaCache) 
    : TableTailer(ndb, TABLE, poll_maxTimeToWait, barrier), mElasticSearch(elastic), mPDICache(cache), mSchemaCache(schemaCache){
    
}

void HopsworksOpsLogTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {

    int id = value[OPS_ID_PK]->int32_value();
    int op_id = value[OPS_OP_ID]->int32_value();
    OpsLogOn op_on = static_cast<OpsLogOn>(value[OPS_OP_ON]->int8_value());
    OperationType op_type = static_cast<OperationType>(value[OPS_OP_TYPE]->int8_value());
    int project_id = value[OPS_PROJECT_ID]->int32_value();
    int dataset_id = value[OPS_DATASET_ID]->int32_value();
    int inode_id = value[OPS_INODE_ID]->int32_value();
    LOG_DEBUG(endl << "ID = " << id << endl << "OpID = " << op_id << endl 
            << "OpOn = " << OpsLogOnToStr(op_on) << endl
            << "OpType = " << OperationTypeToStr(op_type) << endl
            << "ProjectID = " << project_id << endl
            << "DatasetID = " << project_id << endl
            << "INodeID = " << inode_id);
    
    bool success = false;
    switch(op_on){
        case Dataset:
            success = handleDataset(op_id, op_type, inode_id, project_id);
            break;
        case Project:
            success = handleProject(op_id, op_type);
            break;
        case Schema:
            success = handleSchema(op_id, op_type, dataset_id, project_id, inode_id);
            break;
    }
    
    if(success){
        removeLog(id);
    }
}

bool HopsworksOpsLogTailer::handleDataset(int opId, OperationType opType, 
        int datasetId, int projectId) {
    if(opType == Delete){
       return handleDeleteDataset(datasetId, projectId);
    }else{
       return handleUpsertDataset(opId, opType, datasetId, projectId);
    }
}

bool HopsworksOpsLogTailer::handleUpsertDataset(int opId, OperationType opType, int datasetId, int projectId){
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
    bool success = mElasticSearch->addDataset(projectId, datasetId, data);
    if (success) {
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
    return success;
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

bool HopsworksOpsLogTailer::handleDeleteDataset(int datasetId, int projectId) {
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
    bool success1 = mElasticSearch->deleteDatasetChildren(projectId, datasetId, string(sbDoc.GetString()));
    if (success1) {
        LOG_INFO("Delete Dataset[" << datasetId << "] children inodes: Succeeded");
    }
    
    bool success2 = mElasticSearch->deleteDataset(projectId, datasetId);
    if (success2) {
        LOG_INFO("Delete Dataset[" << datasetId << "]: Succeeded");
    }

    mPDICache->removeDataset(datasetId);
    return (success1 && success2);
}

bool HopsworksOpsLogTailer::handleProject(int projectId, OperationType opType) {
    if (opType == Delete) {
        return handleDeleteProject(projectId);
    } else {
        return handleUpsertProject(projectId, opType);
    }
}

bool HopsworksOpsLogTailer::handleDeleteProject(int projectId) {
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
    bool success1 = mElasticSearch->deleteProjectChildren(projectId, string(sbDoc.GetString()));
    if (success1) {
        LOG_INFO("Delete Project[" << projectId << "] children inodes and datasets : Succeeded");
    }
    
    bool success2 = mElasticSearch->deleteProject(projectId);
    if (success2) {
        LOG_INFO("Delete Project[" << projectId << "]: Succeeded");
    }

    mPDICache->removeProject(projectId);
    return (success1 && success2);
}

bool HopsworksOpsLogTailer::handleUpsertProject(int projectId, OperationType opType){

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
    bool success = mElasticSearch->addProject(projectId, data);
    if (success) {
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
    return success;
        
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

bool HopsworksOpsLogTailer::handleSchema(int schemaId, OperationType opType, int datasetId, int projectId, int inodeId) {
    if(opType == Delete){
        return handleSchemaDelete(schemaId, datasetId, projectId, inodeId);
    }else{
        LOG_ERROR("Unsupported Schema Operation [" << Utils::OperationTypeToStr(opType) << "]. Only Delete is supported.");
        return true;
    }
}

bool HopsworksOpsLogTailer::handleSchemaDelete(int schemaId, int datasetId, int projectId, int inodeId) {
    const NdbDictionary::Dictionary* database = getDatabase(mNdbConnection);
    NdbTransaction* transaction = startNdbTransaction(mNdbConnection);
    mSchemaCache->refresTemplate(database, transaction, schemaId);
    boost::optional<string> schema_ptr = mSchemaCache->getTemplate(schemaId);
    if(schema_ptr){
        string schema = *schema_ptr;
        string json = createSchemaDeleteJSON(schema);
        bool success = mElasticSearch->deleteSchemaForINode(projectId, datasetId, inodeId, json);
        if (success) {
            LOG_INFO("Delete Schema/Template [" << schemaId << ", " << schema << "] for INode ["  << inodeId<< "] : Succeeded");
        }
        return success;
    }else{
        LOG_WARN("Schema/Template ["<< schemaId << "] does not exist");
        return true;
    }
}

string HopsworksOpsLogTailer::createSchemaDeleteJSON(string schema) {
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();

    docWriter.String("script");
    
    stringstream scriptStream;
    scriptStream << "ctx._source[\""<< XATTR_FIELD_NAME << "\"].remove(\"" << schema << "\")"; 
    docWriter.String(scriptStream.str().c_str());
    
    docWriter.EndObject();

    return string(sbDoc.GetString());
}

HopsworksOpsLogTailer::~HopsworksOpsLogTailer() {
    
}

