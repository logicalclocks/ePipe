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

const int NUM_INODES_COLS = 4;
const char* INODES_COLS_TO_READ[] = {"id", "size", "user_id", "group_id"};
const int INODE_ID_COL = 0;
const int INODE_SIZE_COL = 1;
const int INODE_USER_ID_COL=2;
const int INODE_GROUP_ID_COL=3;

const int NUM_UG_COLS = 2;
const char* UG_COLS_TO_READ[] = {"id", "name"};
const int UG_ID_COL = 0;
const int UG_NAME_COL = 1;

FsMutationsDataReader::FsMutationsDataReader(Ndb** connections, const int num_readers, string elastic_ip,
        const bool hopsworks, const string elastic_index, const string elastic_inode_type, ProjectDatasetINodeCache* cache) 
    : NdbDataReader<Cus_Cus, Ndb*>(connections, num_readers, elastic_ip, hopsworks, elastic_index, elastic_inode_type, cache){

}

ReadTimes FsMutationsDataReader::readData(Ndb* connection, Cus_Cus data_batch) {
    ReadTimes rt;
    Cus* added = data_batch.added;

    string json;
    if (added->unsynchronized_size() > 0) {
        json = processAdded(connection, added, rt);
    }

    //TODO: handle deleted

    if (!json.empty()) {
        ptime t1 = getCurrentTime();
        string resp = bulkUpdateElasticSearch(json);
        ptime t2 = getCurrentTime();
        LOG_INFO() << " RESP " << resp;
        rt.mElasticSearchTime = getTimeDiffInMilliseconds(t1, t2);
    }
    return rt;
}

string FsMutationsDataReader::processAdded(Ndb* connection, Cus* added, ReadTimes& rt) {
    ptime t1 = getCurrentTime();

    const NdbDictionary::Dictionary* database = getDatabase(connection);

    NdbTransaction* ts = startNdbTransaction(connection);

    int batch_size = added->unsynchronized_size();
    FsMutationRow pending[batch_size];
    Row inodes[batch_size];

    readINodes(database, ts, added, inodes, pending);

    getUsersAndGroups(database, ts, inodes, batch_size);

    ptime t2 = getCurrentTime();

    string data = createJSON(pending, inodes, batch_size);

    ptime t3 = getCurrentTime();

    LOG_INFO() << " Out :: " << endl << data << endl;

    connection->closeTransaction(ts);
    
    rt.mNdbReadTime = getTimeDiffInMilliseconds(t1, t2);
    rt.mJSONCreationTime = getTimeDiffInMilliseconds(t2, t3);
    
    return data;
}

void FsMutationsDataReader::readINodes(const NdbDictionary::Dictionary* database, 
        NdbTransaction* transaction, Cus* added, Row* inodes, FsMutationRow* pending) {
    
    const NdbDictionary::Table* inodes_table = getTable(database, "hdfs_inodes");    
    NdbDictionary::Column::ArrayType name_array_type = inodes_table->getColumn("name")->getArrayType();
    
    int batch_size = added->unsynchronized_size();
        
    for (int i =0; i < batch_size; i++) {
        boost::optional<FsMutationRow> res = added->unsynchronized_remove();
        if(!res){
            break;
        }
        
        FsMutationRow row = *res;
        
        NdbOperation* op = getNdbOperation(transaction, inodes_table);
        
        op->readTuple(NdbOperation::LM_CommittedRead);
        
        op->equal("parent_id", row.mParentId);
        op->equal("name", Utils::get_ndb_varchar(row.mInodeName, name_array_type));
        
        for(int c=0; c<NUM_INODES_COLS; c++){
            NdbRecAttr* col = getNdbOperationValue(op, INODES_COLS_TO_READ[c]);
            inodes[i].push_back(col);
        }
        
        LOG_TRACE() << " Read INode row for [" << row.mParentId << " , "  << row.mInodeName << "]";  
        pending[i] = row;
    }
    
    executeTransaction(transaction, NdbTransaction::NoCommit);
}

void FsMutationsDataReader::getUsersAndGroups(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, 
        Row* inodes, int batchSize) {   
    UISet user_ids;
    UISet group_ids;
    
    for(int i=0; i< batchSize; i++){
        int userId = inodes[i][INODE_USER_ID_COL]->int32_value();
        int groupId = inodes[i][INODE_GROUP_ID_COL]->int32_value();
        
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
   return readTableWithIntPK(database, transaction, "hdfs_users", ids, UG_COLS_TO_READ, NUM_UG_COLS, UG_ID_COL);
}

UIRowMap FsMutationsDataReader::getGroupsFromDB(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet ids) {
   return readTableWithIntPK(database, transaction, "hdfs_groups", ids, UG_COLS_TO_READ, NUM_UG_COLS, UG_ID_COL);
}

string FsMutationsDataReader::createJSON(FsMutationRow* pending, Row* inodes, int batch_size) {
    
    stringstream out;
    for (int i = 0; i < batch_size; i++) {
        if (pending[i].mInodeId != inodes[i][INODE_ID_COL]->int32_value()) {
            LOG_INFO() << " Data for " << pending[i].mParentId << ", " << pending[i].mInodeName << " not found";
            continue;
        }
        
        int userId = inodes[i][INODE_USER_ID_COL]->int32_value();
        int groupId = inodes[i][INODE_GROUP_ID_COL]->int32_value();
        int datasetId = pending[i].mDatasetId;
        
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
            // set project (rounting) and dataset (parent) ids 
            opWriter.String("_parent");
            opWriter.Int(datasetId);
        
            opWriter.String("_routing");
            opWriter.Int(mPDICache->getProjectId(datasetId));
        }
 
        opWriter.String("_id");
        opWriter.Int(inodes[i][INODE_ID_COL]->int32_value());

        opWriter.EndObject();
        
        opWriter.EndObject();
        
        out << sbOp.GetString() << endl;
        
        rapidjson::StringBuffer sbDoc;
        rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
        
        docWriter.StartObject();
        docWriter.String("doc");
        docWriter.StartObject();
        
        docWriter.String("parent_id");
        docWriter.Int(pending[i].mParentId);
        
        docWriter.String("name");
        docWriter.String(pending[i].mInodeName.c_str());
        
        docWriter.String("operation");
        docWriter.Int(pending[i].mOperation);
        
        docWriter.String("timestamp");
        docWriter.Int64(pending[i].mTimestamp);
        
        docWriter.String("size");
        docWriter.Int64(inodes[i][INODE_SIZE_COL]->int64_value());
        
        docWriter.String("user");
        docWriter.String(mUsersCache.get(userId).c_str());
        
        docWriter.String("group");
        docWriter.String(mGroupsCache.get(groupId).c_str());
        
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
        delete mNdbConnections[i];
    }
}
