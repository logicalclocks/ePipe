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
    
    const NdbDictionary::Dictionary* database = connection->getDictionary();
    if (!database) LOG_NDB_API_ERROR(connection->getNdbError());

    const NdbDictionary::Table* inodes_table = database->getTable("hdfs_inodes");
    if (!inodes_table) LOG_NDB_API_ERROR(database->getNdbError());

    NdbTransaction* ts = connection->startTransaction();
    if (ts == NULL) LOG_NDB_API_ERROR(connection->getNdbError());
    
    NdbDictionary::Column::ArrayType name_array_type = inodes_table->getColumn("name")->getArrayType();
    
    int batch_size = added->unsynchronized_size();
    
    std::vector<FsMutationRow> pending;
    std::vector<NdbRecAttr*> cols[batch_size];
    
    int i = 0;
    while (i < batch_size) {
        boost::optional<FsMutationRow> res = added->unsynchronized_remove();
        if(!res){
            break;
        }
        
        FsMutationRow row = *res;
        
        NdbOperation* op = ts->getNdbOperation(inodes_table);
        if (op == NULL) LOG_NDB_API_ERROR(ts->getNdbError());
        
        op->readTuple(NdbOperation::LM_CommittedRead);
        
        op->equal("parent_id", row.mParentId);
        op->equal("name", Utils::get_ndb_varchar(row.mInodeName, name_array_type));
        
        for(int c=0; c<NUM_INODES_COLS; c++){
            NdbRecAttr* col = op->getValue(INODES_COLS_TO_READ[c]);
            if (col == NULL) LOG_NDB_API_ERROR(ts->getNdbError());
            cols[i].push_back(col);
        }
        
        LOG_TRACE() << " Read INode row for [" << row.mParentId << " , "  << row.mInodeName << "]";  
        pending.push_back(row);
        i++;
    }                    
    
    if(ts->execute(NdbTransaction::NoCommit) == -1){
        LOG_NDB_API_ERROR(ts->getNdbError());
    }

    //TODO: Check users and groups cache
        
    const NdbDictionary::Table* users_table = database->getTable("hdfs_users");
    if (!users_table) LOG_NDB_API_ERROR(database->getNdbError());
    
    const NdbDictionary::Table* groups_table = database->getTable("hdfs_groups");
    if (!users_table) LOG_NDB_API_ERROR(database->getNdbError());
    
    boost::unordered_set<int> user_ids;
    boost::unordered_set<int> group_ids;
    boost::unordered_map<int, NdbRecAttr*> users;
    boost::unordered_map<int, NdbRecAttr*> groups;

    for(int i=0; i< batch_size; i++){
        user_ids.insert(cols[i][USER_ID_COL]->int32_value());
        group_ids.insert(cols[i][GROUP_ID_COL]->int32_value());
    }
    
    for(boost::unordered_set<int>::iterator it = user_ids.begin(); 
            it != user_ids.end(); ++it){
         NdbOperation* user_op = ts->getNdbOperation(users_table);
        user_op->readTuple(NdbOperation::LM_CommittedRead);
        int user_id = *it;
        user_op->equal("id", user_id);
        NdbRecAttr* user_name = user_op->getValue("name");
        if (user_name == NULL) LOG_NDB_API_ERROR(ts->getNdbError());
        users[user_id] = user_name;
    }
    
     for(boost::unordered_set<int>::iterator it = group_ids.begin(); 
            it != group_ids.end(); ++it){
         NdbOperation* group_op = ts->getNdbOperation(groups_table);
        group_op->readTuple(NdbOperation::LM_CommittedRead);
        int group_id = *it;
        group_op->equal("id", group_id);
        NdbRecAttr* group_name = group_op->getValue("name");
        if (group_name == NULL) LOG_NDB_API_ERROR(ts->getNdbError());
        groups[group_id] = group_name;
    }
    
    if(ts->execute(NdbTransaction::Commit) == -1){
        LOG_NDB_API_ERROR(ts->getNdbError());
    }
    
    //TODO: update current cache
    
    std::string data = createJSON(pending, cols, users, groups);
        
    LOG_INFO() << " Out :: " << endl << data << endl;

    connection->closeTransaction(ts);
    
    string resp = bulkUpdateElasticSearch(data);
    
    LOG_INFO() << " RESP " << resp;
}

string FsMutationsDataReader::createJSON(std::vector<FsMutationRow> pending, std::vector<NdbRecAttr*> inodes[],
        boost::unordered_map<int, NdbRecAttr*> users, boost::unordered_map<int, NdbRecAttr*> groups) {
    
    std::stringstream out;
    for (unsigned int i = 0; i < pending.size(); i++) {
        if (pending[i].mInodeId != inodes[i][0]->int32_value()) {
            LOG_INFO() << " Data for " << pending[i].mParentId << ", " << pending[i].mInodeName << " not found";
            break;
        }

        rapidjson::StringBuffer sbOp;
        rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

        opWriter.StartObject();
        
        opWriter.String("update");
        opWriter.StartObject();

        opWriter.String("_index");
        opWriter.String("projects");
        
        opWriter.String("_type");
        opWriter.String("inode");
        
        opWriter.String("_parent");
        opWriter.Int(3);
        
        opWriter.String("_routing");
        opWriter.Int(2);
        
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
