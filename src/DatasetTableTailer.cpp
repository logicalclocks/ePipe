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
const char *DATASET_TABLE_NAME= "dataset";
const int NO_DATASET_TABLE_COLUMNS= 6;
const char *DATASET_TABLE_COLUMNS[NO_DATASET_TABLE_COLUMNS]=
    {"inode_id",
     "inode_pid",
     "inode_name",
     "projectId",
     "description",
     "public_ds"
    };

const int DATASET_NUM_EVENT_TYPES_TO_WATCH = 3; 
const NdbDictionary::Event::TableEvent DATASET_EVENT_TYPES_TO_WATCH[DATASET_NUM_EVENT_TYPES_TO_WATCH] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE, 
      NdbDictionary::Event::TE_DELETE
    };

DatasetTableTailer::DatasetTableTailer(Ndb* ndb, const int poll_maxTimeToWait, string elastic_addr, 
        const string elastic_index, const string elastic_dataset_type,DatasetProjectCache* cache) 
    : TableTailer(ndb, DATASET_TABLE_NAME, DATASET_TABLE_COLUMNS, NO_DATASET_TABLE_COLUMNS, 
        DATASET_EVENT_TYPES_TO_WATCH,DATASET_NUM_EVENT_TYPES_TO_WATCH, poll_maxTimeToWait), 
        mElasticAddr(elastic_addr), mElasticIndex(elastic_index), mElasticDatasetType(elastic_dataset_type),
        mDatasetProjectCache(cache){
    
}

void DatasetTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
    
    if(eventType == NdbDictionary::Event::TE_DELETE){
        //TODO: handle deleted
        return;
    }
    
    int id = value[0]->int32_value();
    int projectId = value[3]->int32_value();
    
    mDatasetProjectCache->addDatasetToProject(id, projectId);
    
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();
    
    docWriter.String("doc");
    docWriter.StartObject();
    
    docWriter.String("parent_id");
    docWriter.Int(value[1]->int32_value());
    
    docWriter.String("inode_name");
    docWriter.String(Utils::get_string(value[2]).c_str());
    
    docWriter.String("projectId");
    docWriter.Int(projectId);
    
    docWriter.String("description");
    docWriter.String(Utils::get_string(value[4]).c_str());
    
    bool public_ds = value[5]->int8_value() == 1;
    
    docWriter.String("public_ds");
    docWriter.Bool(public_ds);
    
    docWriter.EndObject();
    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);
    docWriter.EndObject();
    
    string data = string(sbDoc.GetString());
    
    string url = Utils::getElasticSearchUpdateDoc(mElasticAddr, mElasticIndex, mElasticDatasetType, id, projectId);
    LOG_INFO() << "Dataset ::  " << data;
    string resp = Utils::elasticSearchPOST(url, data);
    LOG_INFO() << "Resp :: " << resp;
}


DatasetTableTailer::~DatasetTableTailer() {
}

