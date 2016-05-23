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
 * File:   FsMutationsDataReader.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "FsMutationsDataReader.h"
#include "DatasetTableTailer.h"

const char* INODES = "hdfs_inodes";
const int NUM_INODES_COLS = 4;
const char* INODES_COLS_TO_READ[] = {"id", "size", "user_id", "group_id"};
const int INODE_ID_COL = 0;
const int INODE_SIZE_COL = 1;
const int INODE_USER_ID_COL=2;
const int INODE_GROUP_ID_COL=3;

const char* USERS = "hdfs_users";
const char* GROUPS = "hdfs_groups";
const int NUM_UG_COLS = 2;
const char* UG_COLS_TO_READ[] = {"id", "name"};
const int UG_ID_COL = 0;
const int UG_NAME_COL = 1;

FsMutationsDataReader::FsMutationsDataReader(MConn* connections, const int num_readers, string elastic_ip,
        const bool hopsworks, const string elastic_index, const string elastic_inode_type, ProjectDatasetINodeCache* cache) 
    : NdbDataReader<FsMutationRow,MConn>(connections, num_readers, elastic_ip, hopsworks, elastic_index, elastic_inode_type, cache){

}

ReadTimes FsMutationsDataReader::readData(MConn connection, Fmq* data_batch) {
    ReadTimes rt;
    
    string json;
    if (!data_batch->empty()) {
        json = processAddedandDeleted(connection, data_batch, rt);
    }

    if (!json.empty()) {
        ptime t1 = getCurrentTime();
        string resp = bulkUpdateElasticSearch(json);
        ptime t2 = getCurrentTime();
        LOG_INFO() << " RESP " << resp;
        rt.mElasticSearchTime = getTimeDiffInMilliseconds(t1, t2);
    }
    return rt;
}

string FsMutationsDataReader::processAddedandDeleted(MConn conn, Fmq* data_batch, ReadTimes& rt) {
    ptime t1 = getCurrentTime();

    Ndb* inodeConnection = conn.inodeConnection;
    const NdbDictionary::Dictionary* database = getDatabase(inodeConnection);

    NdbTransaction* ts = startNdbTransaction(inodeConnection);

    Rows inodes(data_batch->size());
        
    readINodes(database, ts, data_batch, inodes);

    getUsersAndGroups(database, ts, inodes);

    FsMutationsTableTailer::removeLogs(database, ts, data_batch);
    
    executeTransaction(ts, NdbTransaction::Commit);
    
    if(mHopsworksEnalbed){
        updateProjectIds(conn.metadataConnection, data_batch);
    }
    
    ptime t2 = getCurrentTime();

    string data = createJSON(data_batch, inodes);

    ptime t3 = getCurrentTime();

    LOG_INFO() << " Out :: " << endl << data << endl;
    
    inodeConnection->closeTransaction(ts);
    
    rt.mNdbReadTime = getTimeDiffInMilliseconds(t1, t2);
    rt.mJSONCreationTime = getTimeDiffInMilliseconds(t2, t3);
    
    return data;
}

void FsMutationsDataReader::readINodes(const NdbDictionary::Dictionary* database, 
        NdbTransaction* transaction, Fmq* data_batch, Rows& inodes) {
    
    const NdbDictionary::Table* inodes_table = getTable(database, INODES);    
    NdbDictionary::Column::ArrayType name_array_type = inodes_table->getColumn("name")->getArrayType();
    
    int i = 0;
    for (Fmq::iterator it=data_batch->begin(); it != data_batch->end(); ++it, i++) {

        FsMutationRow row = *it;
        if(row.mOperation == DELETE){
            continue;
        }
        NdbOperation* op = getNdbOperation(transaction, inodes_table);
        
        op->readTuple(NdbOperation::LM_CommittedRead);
        
        op->equal("parent_id", row.mParentId);
        op->equal("name", get_ndb_varchar(row.mInodeName, name_array_type));
        
        inodes[i] = Row(NUM_INODES_COLS);
        for(int c=0; c<NUM_INODES_COLS; c++){
            NdbRecAttr* col = getNdbOperationValue(op, INODES_COLS_TO_READ[c]);
            inodes[i][c] = col;
        }
        
        LOG_TRACE() << " Read INode row for [" << row.mParentId << " , "  << row.mInodeName << "]";       
    }
    
    executeTransaction(transaction, NdbTransaction::NoCommit);
}

void FsMutationsDataReader::getUsersAndGroups(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, 
       Rows& inodes) {   
    UISet user_ids;
    UISet group_ids;
    
    for(vector<Row>::iterator it = inodes.begin(); it != inodes.end(); ++it){
        Row row = *it;
        if(row.empty()){
            continue;
        }
        int userId = row[INODE_USER_ID_COL]->int32_value();
        int groupId = row[INODE_GROUP_ID_COL]->int32_value();
        
        if(!mUsersCache.contains(userId)){
            user_ids.insert(userId);
        }
        
        if(!mGroupsCache.contains(groupId)){
            group_ids.insert(groupId);
        }
    }
    
    if(user_ids.empty() && group_ids.empty()){
        LOG_DEBUG() << "All required users and groups are already in the cache";
        return;
    }
    
    UIRowMap users = getUsersFromDB(database, transaction, user_ids);
    UIRowMap groups = getGroupsFromDB(database, transaction, group_ids);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);
    
    for(UIRowMap::iterator it = users.begin(); it != users.end(); ++it){
        if(it->first != it->second[UG_ID_COL]->int32_value()){
            LOG_DEBUG() << "User " << it->first << " doesn't exist";
            continue;
        }
        string userName = get_string(it->second[UG_NAME_COL]);
        LOG_DEBUG() << "ADD User [" << it->first << ", " << userName << "] to the Cache";
        mUsersCache.put(it->first, userName);
    }
    
     for(UIRowMap::iterator it = groups.begin(); it != groups.end(); ++it){
          if(it->first != it->second[UG_ID_COL]->int32_value()){
            LOG_DEBUG() << "Group " << it->first << " doesn't exist";
            continue;
        }
        string groupName = get_string(it->second[UG_NAME_COL]);
        LOG_DEBUG() << "ADD Group [" << it->first << ", " << groupName << "] to the Cache";
        mGroupsCache.put(it->first, groupName);
    }
}

UIRowMap FsMutationsDataReader::getUsersFromDB(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet ids) {
   return readTableWithIntPK(database, transaction, USERS, ids, UG_COLS_TO_READ, NUM_UG_COLS, UG_ID_COL);
}

UIRowMap FsMutationsDataReader::getGroupsFromDB(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet ids) {
   return readTableWithIntPK(database, transaction, GROUPS, ids, UG_COLS_TO_READ, NUM_UG_COLS, UG_ID_COL);
}

void FsMutationsDataReader::updateProjectIds(Ndb* metaConnection, Fmq* data_batch) {
    UISet dataset_ids;
    for (Fmq::iterator it = data_batch->begin(); it != data_batch->end(); ++it) {
        FsMutationRow row = *it;

        if (!mPDICache->containsDataset(row.mDatasetId)) {
            dataset_ids.insert(row.mDatasetId);
        }
    }

    if (!dataset_ids.empty()) {
        const NdbDictionary::Dictionary* metaDatabase = getDatabase(metaConnection);
        NdbTransaction* metaTransaction = startNdbTransaction(metaConnection);
        DatasetTableTailer::updateProjectIds(metaDatabase, metaTransaction, dataset_ids, mPDICache);
        metaConnection->closeTransaction(metaTransaction);
    }
}

string FsMutationsDataReader::createJSON(Fmq* pending, Rows& inodes) {
    
    stringstream out;
    int i=0;
    for (Fmq::iterator it = pending->begin(); it != pending->end(); ++it, i++) {
        FsMutationRow row = *it;
        
        if(row.mOperation == DELETE){
            //Handle the delete
            rapidjson::StringBuffer sbOp;
            rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

            opWriter.StartObject();
            
            // update could be used and set operation to delete instead of add
            opWriter.String("delete");
            opWriter.StartObject();

            opWriter.String("_index");
            opWriter.String(mElasticIndex.c_str());

            opWriter.String("_type");
            opWriter.String(mElasticInodeType.c_str());

            if (mHopsworksEnalbed) {
                // set project (rounting) and dataset (parent) ids 
                opWriter.String("_parent");
                opWriter.Int(row.mDatasetId);

                opWriter.String("_routing");
                opWriter.Int(mPDICache->getProjectId(row.mDatasetId));
            }

            opWriter.String("_id");
            opWriter.Int(row.mInodeId);

            opWriter.EndObject();

            opWriter.EndObject();

            out << sbOp.GetString() << endl;
            continue;
        }
        
        if (row.mInodeId != inodes[i][INODE_ID_COL]->int32_value()) {
            LOG_INFO() << " Data for " << row.mParentId << ", " << row.mInodeName << " not found";
            continue;
        }
        
        
        //Handle ADD
        int userId = inodes[i][INODE_USER_ID_COL]->int32_value();
        int groupId = inodes[i][INODE_GROUP_ID_COL]->int32_value();
        
        rapidjson::StringBuffer sbOp;
        rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

        opWriter.StartObject();
        
        opWriter.String("update");
        opWriter.StartObject();

        opWriter.String("_index");
        opWriter.String(mElasticIndex.c_str());
        
        opWriter.String("_type");
        opWriter.String(mElasticInodeType.c_str());
        
        int projectId = -1;
        if(mHopsworksEnalbed){
            // set project (rounting) and dataset (parent) ids 
            opWriter.String("_parent");
            opWriter.Int(row.mDatasetId);
            
            projectId = mPDICache->getProjectId(row.mDatasetId);
            opWriter.String("_routing");
            opWriter.Int(projectId);
        }
 
        opWriter.String("_id");
        opWriter.Int(row.mInodeId);

        opWriter.EndObject();
        
        opWriter.EndObject();
        
        out << sbOp.GetString() << endl;
        
        rapidjson::StringBuffer sbDoc;
        rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
        
        docWriter.StartObject();
        docWriter.String("doc");
        docWriter.StartObject();
        
        docWriter.String("parent_id");
        docWriter.Int(row.mParentId);
        
        if(mHopsworksEnalbed){
            docWriter.String("dataset_id");
            docWriter.Int(row.mDatasetId);
        
            docWriter.String("project_id");
            docWriter.Int(projectId);
        }
        
        docWriter.String("name");
        docWriter.String(row.mInodeName.c_str());
        
        docWriter.String("operation");
        docWriter.Int(row.mOperation);
        
        docWriter.String("timestamp");
        docWriter.Int64(row.mTimestamp);
        
        docWriter.String("size");
        docWriter.Int64(inodes[i][INODE_SIZE_COL]->int64_value());
        
        //FIXME: 
        docWriter.String("user");
        docWriter.String(mUsersCache.get(userId).get().c_str());
        
        //FIXME:
        docWriter.String("group");
        docWriter.String(mGroupsCache.get(groupId).get().c_str());
        
        docWriter.EndObject();
        
        docWriter.String("doc_as_upsert");
        docWriter.Bool(true);
        
        docWriter.EndObject();
        
        out << sbDoc.GetString() << endl;
                
    }

    return out.str();
}

FsMutationsDataReader::~FsMutationsDataReader() {
    for(int i=0; i< mNumReaders; i++){
        delete mNdbConnections[i].inodeConnection;
        delete mNdbConnections[i].metadataConnection;
    }
}
