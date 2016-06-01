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
 * File:   ProjectTableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "ProjectTableTailer.h"

using namespace Utils::NdbC;
using namespace Utils::ElasticSearch;

const char* _project_table= "project";
const int _project_noCols= 5;
const char* _project_cols[_project_noCols]=
    {"id",
     "inode_pid",
     "inode_name",
     "username",
     "description"
    };

const int _project_noEvents = 3; 
const NdbDictionary::Event::TableEvent _project_events[_project_noEvents] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE, 
      NdbDictionary::Event::TE_DELETE
    };

const WatchTable ProjectTableTailer::TABLE = {_project_table, _project_cols, _project_noCols , _project_events, _project_noEvents};

ProjectTableTailer::ProjectTableTailer(Ndb* ndb, const int poll_maxTimeToWait, string elastic_addr, 
        const string elastic_index, const string elastic_project_type, ProjectDatasetINodeCache* cache) 
    : TableTailer(ndb, TABLE, poll_maxTimeToWait), mElasticAddr(elastic_addr), mElasticIndex(elastic_index), 
        mElasticProjectType(elastic_project_type), mPDICache(cache){
}

void ProjectTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
    int id = value[0]->int32_value();
        
    if(eventType == NdbDictionary::Event::TE_DELETE){
        string deleteDatasetUrl = getElasticSearchUrlOnDoc(mElasticAddr, mElasticIndex, mElasticProjectType, id);
        if(elasticSearchDELETE(deleteDatasetUrl)){
            LOG_INFO("Delete Project[" << id << "]: Succeeded");
        }
        
        string deteteProjectChildren = getElasticSearchDeleteByQueryUrl(mElasticAddr, mElasticIndex, id);

        rapidjson::StringBuffer sbDoc;
        rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
        docWriter.StartObject();
        docWriter.String("query");

        docWriter.StartObject();

        docWriter.String("match");
        docWriter.StartObject();
        docWriter.String("project_id");
        docWriter.Int(id);
        docWriter.EndObject();

        docWriter.EndObject();

        docWriter.EndObject();
        
        //TODO: handle failures in elastic search
        if(elasticSearchDELETE(deteteProjectChildren, string(sbDoc.GetString()))){
            LOG_INFO("Delete Project[" << id << "] children inodes and datasets : Succeeded");
        }

        mPDICache->removeProject(id);
        
        return;
    }
    
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();
    
    docWriter.String("doc");
    docWriter.StartObject();
    
    docWriter.String("parent_id");
    docWriter.Int(value[1]->int32_value());
    
    docWriter.String("name");
    docWriter.String(get_string(value[2]).c_str());
    
    docWriter.String("user");
    docWriter.String(get_string(value[3]).c_str());
    
    docWriter.String("description");
    docWriter.String(get_string(value[4]).c_str());
    
    docWriter.EndObject();
    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);
    docWriter.EndObject();
    
    string data = string(sbDoc.GetString());
    string url = getElasticSearchUpdateDocUrl(mElasticAddr, mElasticIndex, mElasticProjectType, id);
    if(elasticSearchPOST(url, data)){
        LOG_INFO("Add Project[" << id << "]: Succeeded");
    }
}

ProjectTableTailer::~ProjectTableTailer() {
}

