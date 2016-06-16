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

using namespace Utils::NdbC;

const char* _dataset_table= "dataset";
const int _dataset_noCols= 8;
const char* _dataset_cols[_dataset_noCols]=
    {"inode_id",
     "inode_pid",
     "inode_name",
     "projectId",
     "description",
     "public_ds",
     "searchable",
     "shared"
    };

const int _dataset_noEvents = 3; 
const NdbDictionary::Event::TableEvent _dataset_events[_dataset_noEvents] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE, 
      NdbDictionary::Event::TE_DELETE
    };

const WatchTable DatasetTableTailer::TABLE = {_dataset_table, _dataset_cols, _dataset_noCols , _dataset_events, _dataset_noEvents, "inode_id", "inode_id"};


DatasetTableTailer::DatasetTableTailer(Ndb* ndb, const int poll_maxTimeToWait, 
        ElasticSearch* elastic,ProjectDatasetINodeCache* cache) 
    : TableTailer(ndb, TABLE, poll_maxTimeToWait), mElasticSearch(elastic), mPDICache(cache){
}

void DatasetTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
    int id = value[0]->int32_value();
    int projectId = value[3]->int32_value();
    bool originalDS = value[7]->int8_value() == 0;
    
    if(!originalDS){
        LOG_DEBUG("Ignore shared Dataset [" << id << "] in Project [" << projectId << "]");
        return;
    }
    
    if(eventType == NdbDictionary::Event::TE_DELETE){
        
        rapidjson::StringBuffer sbDoc;
        rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
        docWriter.StartObject();
        docWriter.String("query");

        docWriter.StartObject();

        docWriter.String("match");
        docWriter.StartObject();
        docWriter.String("dataset_id");
        docWriter.Int(id);
        docWriter.EndObject();

        docWriter.EndObject();

        docWriter.EndObject();
        
        //TODO: handle failures in elastic search
        if(mElasticSearch->deleteDatasetChildren(projectId, id, string(sbDoc.GetString()))){
            LOG_INFO("Delete Dataset[" << id << "] children inodes: Succeeded");
        }
        
        if(mElasticSearch->deleteDataset(projectId, id)){
            LOG_INFO("Delete Dataset[" << id << "]: Succeeded");
        }

        mPDICache->removeDataset(id);
        
        return;
    }
    
    mPDICache->addDatasetToProject(id, projectId);
    
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();
    
    docWriter.String("doc");
    docWriter.StartObject();
    
    docWriter.String("parent_id");
    docWriter.Int(value[1]->int32_value());
    
    docWriter.String("name");
    docWriter.String(get_string(value[2]).c_str());
    
    docWriter.String("project_id");
    docWriter.Int(projectId);
    
    docWriter.String("description");
    docWriter.String(get_string(value[4]).c_str());
    
    bool public_ds = value[5]->int8_value() == 1;
    
    docWriter.String("public_ds");
    docWriter.Bool(public_ds);
    
    bool searchable = value[6]->int8_value() == 1;
    
    docWriter.String("searchable");
    docWriter.Bool(searchable);
    
    docWriter.EndObject();
    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);
    docWriter.EndObject();
    
    string data = string(sbDoc.GetString());
    if(mElasticSearch->addDataset(projectId, id, data)){
        LOG_INFO("Add Dataset[" << id << "]: Succeeded");
    }
    
    const NdbDictionary::Dictionary*  database = getDatabase(mNdbConnection);
    NdbTransaction* transaction = startNdbTransaction(mNdbConnection);
    Recovery::checkpointDataset(database, transaction, id);
    transaction->close();
}

void DatasetTableTailer::updateProjectIds(const NdbDictionary::Dictionary* database,
        NdbTransaction* transaction, UISet dataset_ids, ProjectDatasetINodeCache* cache) {    

    const NdbDictionary::Index * index= getIndex(database, TABLE.mTableName, _dataset_cols[0]);
    
    vector<NdbIndexScanOperation*> indexScanOps;
    UIRowMap rows;
    for (UISet::iterator it = dataset_ids.begin(); it != dataset_ids.end(); ++it) {
        NdbIndexScanOperation* op = getNdbIndexScanOperation(transaction, index);
        op->readTuples(NdbOperation::LM_CommittedRead);
       
        op->equal(_dataset_cols[0], *it);
        
        NdbRecAttr* id_col = getNdbOperationValue(op, _dataset_cols[0]);
        NdbRecAttr* proj_id_col = getNdbOperationValue(op, _dataset_cols[3]);
        NdbRecAttr* shared_col = getNdbOperationValue(op, _dataset_cols[7]);
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

DatasetTableTailer::~DatasetTableTailer() {
}

