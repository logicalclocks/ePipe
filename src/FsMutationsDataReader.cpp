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

using namespace boost::network;
using namespace boost::network::http;

FsMutationsDataReader::FsMutationsDataReader(Ndb** connections, const int num_readers, std::string elastic_ip) : NdbDataReader<Cus_Cus>(connections, num_readers, elastic_ip){

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
    
    const int num_inodes_columns = 2;
    const char* inodes_columns_to_read[] = {"id", "size"};

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
        
        for(int c=0; c<num_inodes_columns; c++){
            NdbRecAttr* col = op->getValue(inodes_columns_to_read[c]);
            if (col == NULL) LOG_NDB_API_ERROR(ts->getNdbError());
            cols[i].push_back(col);
        }
        
        LOG_TRACE() << " Read INode row for [" << row.mParentId << " , "  << row.mInodeName << "]";  
        pending.push_back(row);
        i++;
    }                    
    
    if(ts->execute(NdbTransaction::Commit) == -1){
        LOG_NDB_API_ERROR(ts->getNdbError());
    }


    std::stringstream out;

    for (int i = 0; i < batch_size; i++) {
        if (pending[i].mInodeId != cols[i][0]->int32_value()) {
            LOG_INFO() << " Data for " << pending[i].mParentId << ", " << pending[i].mInodeName << " not found";
            break;
        }

        for (int c = 0; c < num_inodes_columns; c++) {
            NdbRecAttr* col = cols[i][c];
            LOG_INFO() << "Got values for INode " << col->getColumn()->getName() << " = " << col->int64_value();
        }


        rapidjson::StringBuffer sbOp;
        rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

        opWriter.StartObject();
        
        opWriter.String("update");
        opWriter.StartObject();

        opWriter.String("_index");
        opWriter.String("test");
        
        opWriter.String("_type");
        opWriter.String("inodes");
        
        opWriter.String("_id");
        opWriter.Int(cols[i][0]->int32_value());

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
        docWriter.Int64(cols[i][1]->int64_value());
        
        docWriter.EndObject();
        
        docWriter.String("doc_as_upsert");
        docWriter.Bool(true);
        
        docWriter.EndObject();
        
        out << sbDoc.GetString() << endl;
                
    }

    std::string data = out.str();
        
    LOG_INFO() << " Out :: " << endl << data << endl;

    connection->closeTransaction(ts);
    
    client::request request_(mElasticBulkUrl);
    request_ << header("Connection", "close");
    request_ << header("Content-Type", "application/json");
    
    char body_str_len[8];
    sprintf(body_str_len, "%lu", data.length());

    request_ << header("Content-Length", body_str_len);
    request_ << body(data);
    
    client client_;
    client::response response_ = client_.post(request_);
    std::string body_ = body(response_);

    LOG_INFO() << " RESP " << body_;
}

FsMutationsDataReader::~FsMutationsDataReader() {
    
}
