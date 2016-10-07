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
 * File:   SchemalessMetadataReader.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef SCHEMALESSMETADATAREADER_H
#define SCHEMALESSMETADATAREADER_H

#include "NdbDataReader.h"
#include "SchemalessMetadataTailer.h"
#include "DatasetTableTailer.h"


class SchemalessMetadataReader : public NdbDataReader<SchemalessMetadataEntry, MConn>{
public:
    SchemalessMetadataReader(MConn* connections, const int num_readers, const bool hopsworks, 
            ElasticSearch* elastic, ProjectDatasetINodeCache* cache);
    virtual ~SchemalessMetadataReader();
private:
    virtual void processAddedandDeleted(MConn connection, Smq* data_batch, Bulk& bulk);
};

SchemalessMetadataReader::SchemalessMetadataReader(MConn* connections, 
        const int num_readers, const bool hopsworks, ElasticSearch* elastic,
        ProjectDatasetINodeCache* cache) 
        : NdbDataReader<SchemalessMetadataEntry, MConn>(connections, num_readers, hopsworks, elastic, cache){
}


void SchemalessMetadataReader::processAddedandDeleted(MConn connection, Smq* data_batch, Bulk& bulk){
    if(mHopsworksEnalbed){
        UISet inodes_ids;
        for(Smq::iterator it = data_batch->begin(); it != data_batch->end(); ++it){
            SchemalessMetadataEntry entry = *it;
            inodes_ids.insert(entry.mINodeId);
        }
        DatasetTableTailer::refreshCache(connection, inodes_ids, mPDICache);
    }
    
    
    vector<ptime> arrivalTimes(data_batch->size());
    stringstream out;
    int i=0;
    for(Smq::iterator it=data_batch->begin(); it != data_batch->end(); ++it, i++){
        SchemalessMetadataEntry entry = *it;
        
        arrivalTimes[i] = entry.mEventCreationTime;
        
        // INode Operation
        rapidjson::StringBuffer sbOp;
        rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

        opWriter.StartObject();

        opWriter.String("update");
        opWriter.StartObject();


        if(mHopsworksEnalbed){
            int datasetId = mPDICache->getDatasetId(entry.mINodeId);
            // set project (rounting) and dataset (parent) ids 
            opWriter.String("_parent");
            opWriter.Int(datasetId);

            opWriter.String("_routing");
            opWriter.Int(mPDICache->getProjectId(datasetId));  
            
            if(datasetId == entry.mINodeId){
                opWriter.String("_type");
                opWriter.String(mElasticSearch->getDatasetType());
            }
            
        }

        opWriter.String("_id");
        opWriter.Int(entry.mINodeId);
        
        opWriter.EndObject();
        
        out << sbOp.GetString() << endl;
        
        rapidjson::StringBuffer sbDoc;
        rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
        
        docWriter.StartObject();
        docWriter.String("doc");
        docWriter.StartObject();

        docWriter.String("xattr");
        docWriter.StartObject();
        //TODO: parse the json and add it
        
        docWriter.EndObject();
        
        docWriter.EndObject();

        docWriter.String("doc_as_upsert");
        docWriter.Bool(true);

        docWriter.EndObject();

        out << sbDoc.GetString() << endl;
    }
    
    bulk.mArrivalTimes = arrivalTimes;
    bulk.mJSON = out.str();
}

SchemalessMetadataReader::~SchemalessMetadataReader(){
    
}
#endif /* SCHEMALESSMETADATAREADER_H */

