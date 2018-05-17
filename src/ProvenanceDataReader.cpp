/*
 * Copyright (C) 2018 Hops.io
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
 * File:   ProvenanceDataReader.cpp
 * Author: Mahmoud Ismail <maism@kth.se>
 * 
 */

#include "ProvenanceDataReader.h"

ProvenanceDataReader::ProvenanceDataReader(SConn* connections, const int num_readers, 
        const bool hopsworks, ProvenanceElasticSearch* elastic, 
        ProjectDatasetINodeCache* cache) : NdbDataReader(connections, num_readers, hopsworks, elastic, cache) {
}

void ProvenanceDataReader::processAddedandDeleted(SConn conn, Pq* data_batch, PBulk& bulk){
    vector<ptime> arrivalTimes(data_batch->size());
    stringstream out;
    int i=0;
    for (Pq::iterator it = data_batch->begin(); it != data_batch->end(); ++it, i++) {
        ProvenanceRow row = *it;
        arrivalTimes[i] = row.mEventCreationTime;
        ProvenancePK rowPK = row.getPK();
        bulk.mPKs.push_back(rowPK);
        
        rapidjson::StringBuffer sbOp;
        rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

        opWriter.StartObject();
        
        opWriter.String("update");
        opWriter.StartObject();
        
        opWriter.String("_id");
        opWriter.String(rowPK.to_string().c_str());
        
        opWriter.EndObject();
        
        opWriter.EndObject();
        
        out << sbOp.GetString() << endl;
        
        rapidjson::StringBuffer sbDoc;
        rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
        
        docWriter.StartObject();
        docWriter.String("doc");
        docWriter.StartObject();
      
        docWriter.String("inode_id");
        docWriter.Int(row.mInodeId);
        
        docWriter.String("user_id");
        docWriter.Int(row.mUserId);
        
        docWriter.String("app_id");
        docWriter.String(row.mAppId.c_str());
        
        docWriter.String("logical_time");
        docWriter.Int(row.mLogicalTime);
        
        docWriter.String("partition_id");
        docWriter.Int(row.mPartitionId);
        
        docWriter.String("parent_id");
        docWriter.Int(row.mParentId);
        
        docWriter.String("project_name");
        docWriter.String(row.mProjectName.c_str());
        
        docWriter.String("dataset_name");
        docWriter.String(row.mDatasetName.c_str());
        
        docWriter.String("inode_name");
        docWriter.String(row.mInodeName.c_str());
        
        docWriter.String("user_name");
        docWriter.String(row.mUserName.c_str());
        
        docWriter.String("logical_time_batch");
        docWriter.Int(row.mLogicalTimeBatch);
        
        docWriter.String("timestamp");
        docWriter.Int64(row.mTimestamp);
        
        docWriter.String("timestamp_batch");
        docWriter.Int64(row.mTimestampBatch);
        
        docWriter.String("operation");
        docWriter.Int(row.mOperation);
        
        docWriter.EndObject();
        
        docWriter.String("doc_as_upsert");
        docWriter.Bool(true);
        
        docWriter.EndObject();
        
        out << sbDoc.GetString() << endl;
                
    }
    
    bulk.mArrivalTimes = arrivalTimes;
    bulk.mJSON = out.str();
}



ProvenanceDataReader::~ProvenanceDataReader() {
      for(int i=0; i< mNumReaders; i++){
        delete mNdbConnections[i];
    }
}

