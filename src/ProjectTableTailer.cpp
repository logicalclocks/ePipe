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

const string _project_table= "project";
const int _project_noCols= 5;
const string _project_cols[_project_noCols]=
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

const WatchTable ProjectTableTailer::TABLE = {_project_table, _project_cols, _project_noCols , _project_events, _project_noEvents, "PRIMARY", _project_cols[0]};

const int PR_ID = 0;
const int PR_INODE_PID = 1;
const int PR_INODE_NAME = 2;
const int PR_USER = 3;
const int PR_DESC = 4;

ProjectTableTailer::ProjectTableTailer(Ndb* ndb, const int poll_maxTimeToWait, 
        ElasticSearch* elastic, ProjectDatasetINodeCache* cache) 
    : TableTailer(ndb, TABLE, poll_maxTimeToWait), mElasticSearch(elastic), mPDICache(cache){
}

void ProjectTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
    int projectId = value[PR_ID]->int32_value();

    switch (eventType) {
        case NdbDictionary::Event::TE_INSERT:
            handleAdd(projectId, value);
            break;
        case NdbDictionary::Event::TE_DELETE:
            handleDelete(projectId);
            break;
        case NdbDictionary::Event::TE_UPDATE:
            handleUpdate(projectId, value);
            break;
    }
}

void ProjectTableTailer::handleDelete(int projectId) {
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();
    docWriter.String("query");

    docWriter.StartObject();

    docWriter.String("match");
    docWriter.StartObject();
    docWriter.String("project_id");
    docWriter.Int(projectId);
    docWriter.EndObject();

    docWriter.EndObject();

    docWriter.EndObject();

    //TODO: handle failures in elastic search
    if (mElasticSearch->deleteProjectChildren(projectId, string(sbDoc.GetString()))) {
        LOG_INFO("Delete Project[" << projectId << "] children inodes and datasets : Succeeded");
    }

    if (mElasticSearch->deleteProject(projectId)) {
        LOG_INFO("Delete Project[" << projectId << "]: Succeeded");
    }

    mPDICache->removeProject(projectId);
}

void ProjectTableTailer::handleAdd(int projectId, NdbRecAttr* value[]) {
    string data = createJSONUpSert(value);
    if (mElasticSearch->addProject(projectId, data)) {
        LOG_INFO("Add Project[" << projectId << "]: Succeeded");
    }

    const NdbDictionary::Dictionary* database = getDatabase(mNdbConnection);
    NdbTransaction* transaction = startNdbTransaction(mNdbConnection);
    Recovery::checkpointProject(database, transaction, projectId);
    transaction->close();
}

void ProjectTableTailer::handleUpdate(int projectId, NdbRecAttr* value[]) {
    string data = createJSONUpSert(value);
    if (mElasticSearch->addProject(projectId, data)) {
        LOG_INFO("Update Project[" << projectId << "]: Succeeded");
    }
}

string ProjectTableTailer::createJSONUpSert(NdbRecAttr* value[]) {
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();

    docWriter.String("doc");
    docWriter.StartObject();

    if (value[PR_INODE_PID]->isNULL() != -1) {
        docWriter.String("parent_id");
        docWriter.Int(value[PR_INODE_PID]->int32_value());
    }

    if (value[PR_INODE_NAME]->isNULL() != -1) {
        docWriter.String("name");
        docWriter.String(get_string(value[PR_INODE_NAME]).c_str());
    }

    if (value[PR_USER]->isNULL() != -1) {
        docWriter.String("user");
        docWriter.String(get_string(value[PR_USER]).c_str());
    }

    if (value[PR_DESC]->isNULL() != -1) {
        docWriter.String("description");
        docWriter.String(get_string(value[PR_DESC]).c_str());
    }

    docWriter.EndObject();
    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);
    docWriter.EndObject();

    return string(sbDoc.GetString());
}

ProjectTableTailer::~ProjectTableTailer() {
}

