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

const char *PROJECT_TABLE_NAME= "project";
const int NO_PROJECT_TABLE_COLUMNS= 6;
const char *PROJECT_TABLE_COLUMNS[NO_PROJECT_TABLE_COLUMNS]=
    {"inode_id",
     "inode_pid",
     "inode_name",
     "projectname",
     "username",
     "description"
    };

const int PROJECT_NUM_EVENT_TYPES_TO_WATCH = 3; 
const NdbDictionary::Event::TableEvent PROJECT_EVENT_TYPES_TO_WATCH[PROJECT_NUM_EVENT_TYPES_TO_WATCH] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE, 
      NdbDictionary::Event::TE_DELETE
    };

ProjectTableTailer::ProjectTableTailer(Ndb* ndb, const int poll_maxTimeToWait, string elastic_addr) 
    : TableTailer(ndb, PROJECT_TABLE_NAME, PROJECT_TABLE_COLUMNS, NO_PROJECT_TABLE_COLUMNS, 
        PROJECT_EVENT_TYPES_TO_WATCH,PROJECT_NUM_EVENT_TYPES_TO_WATCH, poll_maxTimeToWait), mElasticAddr(elastic_addr){
}

void ProjectTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
    int id = value[0]->int32_value();
    
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();
    
    docWriter.String("doc");
    docWriter.StartObject();
    
    docWriter.String("parent_id");
    docWriter.Int(value[1]->int32_value());
    
    docWriter.String("inode_name");
    docWriter.String(Utils::get_string(value[2]).c_str());
    
    docWriter.String("project_name");
    docWriter.String(Utils::get_string(value[3]).c_str());
    
    docWriter.String("user");
    docWriter.String(Utils::get_string(value[4]).c_str());
    
    docWriter.String("description");
    docWriter.String(Utils::get_string(value[5]).c_str());
    
    docWriter.EndObject();
    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);
    docWriter.EndObject();
    
    string data = string(sbDoc.GetString());
    string url = Utils::getElasticSearchUpdateDoc(mElasticAddr, "projects", "proj", id);
    LOG_INFO() << "Project ::  " << data;
    string resp = Utils::elasticSearchPOST(url, data);
    LOG_INFO() << "Resp :: " << resp;
}

ProjectTableTailer::~ProjectTableTailer() {
}

