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
const int USER_ID_COL=2;
const int GROUP_ID_COL=3;

FsMutationsDataReader::FsMutationsDataReader(Ndb** connections, const int num_readers, string elastic_ip) : NdbDataReader<Cus_Cus>(connections, num_readers, elastic_ip){

}

void FsMutationsDataReader::readData(Ndb* connection, Cus_Cus data_batch) {
    Cus* added = data_batch.added;
    
    const NdbDictionary::Dictionary* database = getDatabase(connection);

    NdbTransaction* ts = startNdbTransaction(connection);
    
    
    int batch_size = added->unsynchronized_size();
    FsMutationRow pending[batch_size];
    Row inodes[batch_size];
    
    readINodes(database, ts, added, inodes, pending);
        
    getUsersAndGroups(database, ts, inodes, batch_size);
        
    string data = createJSON(pending, inodes, batch_size);
        
    LOG_INFO() << " Out :: " << endl << data << endl;

    connection->closeTransaction(ts);
    
    string resp = bulkUpdateElasticSearch(data);
    
    LOG_INFO() << " RESP " << resp;
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
    UGSet user_ids;
    UGSet group_ids;
    
    for(int i=0; i< batchSize; i++){
        int userId = inodes[i][USER_ID_COL]->int32_value();
        int groupId = inodes[i][GROUP_ID_COL]->int32_value();
        
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
    
    const NdbDictionary::Table* users_table = getTable(database, "hdfs_users");
    const NdbDictionary::Table* groups_table = getTable(database, "hdfs_groups");
    
    UGMap users = getUsersOrGroupsFromDB(users_table, transaction, user_ids);
    UGMap groups = getUsersOrGroupsFromDB(groups_table, transaction, group_ids);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);
    
    for(UGMap::iterator it = users.begin(); it != users.end(); ++it){
        string userName = get_string(it->second);
        LOG_DEBUG() << "ADD User [" << it->first << ", " << userName << "] to the Cache";
        mUsersCache.put(it->first, userName);
    }
    
     for(UGMap::iterator it = groups.begin(); it != groups.end(); ++it){
        string groupName = get_string(it->second);
        LOG_DEBUG() << "ADD Group [" << it->first << ", " << groupName << "] to the Cache";
        mGroupsCache.put(it->first, groupName);
    }
}


UGMap FsMutationsDataReader::getUsersOrGroupsFromDB(const NdbDictionary::Table* table, NdbTransaction* transaction, 
        UGSet ids) {
    UGMap res;
    for(UGSet::iterator it = ids.begin(); it != ids.end(); ++it){
        NdbOperation* op = getNdbOperation(transaction, table);
        op->readTuple(NdbOperation::LM_CommittedRead);
        int id = *it;
        op->equal("id", id);
        NdbRecAttr* name = getNdbOperationValue(op, "name");
        res[id] = name;
    }
    return res;
}

string FsMutationsDataReader::createJSON(FsMutationRow* pending, Row* inodes, int batch_size) {
    
    std::stringstream out;
    for (int i = 0; i < batch_size; i++) {
        if (pending[i].mInodeId != inodes[i][0]->int32_value()) {
            LOG_INFO() << " Data for " << pending[i].mParentId << ", " << pending[i].mInodeName << " not found";
            break;
        }
        
        int userId = inodes[i][USER_ID_COL]->int32_value();
        int groupId = inodes[i][GROUP_ID_COL]->int32_value();
        
        rapidjson::StringBuffer sbOp;
        rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

        opWriter.StartObject();
        
        opWriter.String("update");
        opWriter.StartObject();

        opWriter.String("_index");
        opWriter.String("projects");
        
        opWriter.String("_type");
        opWriter.String("inode");
        
        // set project (rounting) and dataset (parent) ids 
       // opWriter.String("_parent");
       // opWriter.Int(3);
        
       // opWriter.String("_routing");
       // opWriter.Int(2);
        
        opWriter.String("_id");
        opWriter.Int(inodes[i][0]->int32_value());

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
        docWriter.Int64(inodes[i][1]->int64_value());
        
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
    
}
