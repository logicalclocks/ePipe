/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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

#ifndef PROJECTTABLE_H
#define PROJECTTABLE_H

#include "DBTable.h"
#include "Cache.h"

#define DOC_TYPE_PROJECT "proj"

struct ProjectRow {
  int mId;
  std::string mProjectName;
  std::string mUserName;
  std::string mDescription;

  static std::string to_delete_json(std::string index, Int64 inodeId) {
    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("delete");
    opWriter.StartObject();

    opWriter.String("_id");
    opWriter.Int64(inodeId);
    opWriter.String("_index");
    opWriter.String(index.c_str());
    opWriter.EndObject();

    opWriter.EndObject();

    return sbOp.GetString();
  }

  std::string to_upsert_json(std::string index, Int64 inodeId) {
    std::stringstream out;
    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("update");
    opWriter.StartObject();

    opWriter.String("_id");
    opWriter.Int64(inodeId);
    opWriter.String("_index");
    opWriter.String(index.c_str());

    opWriter.EndObject();

    opWriter.EndObject();

    out << sbOp.GetString() << std::endl;

    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();

    docWriter.String("doc");
    docWriter.StartObject();

    docWriter.String("doc_type");
    docWriter.String(DOC_TYPE_PROJECT);

    docWriter.String("project_id");
    docWriter.Int(mId);

    docWriter.String("user");
    docWriter.String(mUserName.c_str());

    docWriter.String("name");
    docWriter.String(mProjectName.c_str());

    docWriter.String("description");
    docWriter.String(mDescription.c_str());

    docWriter.EndObject();
    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);
    docWriter.EndObject();

    out << sbDoc.GetString() << std::endl;
    return out.str();
  }

};

typedef CacheSingleton<Cache<int, std::string>> ProjectCache;
typedef std::vector<ProjectRow> ProjectVec;

class ProjectTable : public DBTable<ProjectRow> {
public:

  ProjectTable(int lru_cap) : DBTable("project") {
    addColumn("id");
    addColumn("projectname");
    addColumn("username");
    addColumn("description");
    ProjectCache::getInstance(lru_cap, "Project");
  }

  ProjectRow get(Ndb* connection, int projectId) {
    ProjectRow row = doRead(connection, projectId);
    ProjectCache::getInstance().put(row.mId, row.mProjectName);
    return row;
  }

  ProjectRow getRow(NdbRecAttr* values[]) {
    ProjectRow row;
    row.mId = values[0]->int32_value();
    row.mProjectName = get_string(values[1]);
    row.mUserName = get_string(values[2]);
    row.mDescription = get_string(values[3]);
    return row;
  }

  void loadProject(Ndb* connection, int projectId) {
    if (ProjectCache::getInstance().contains(projectId)) {
      return;
    }

    ProjectRow row = get(connection, projectId);
    ProjectCache::getInstance().put(projectId, row.mProjectName);
  }

  std::string getProjectNameFromCache(int projectId) {
    boost::optional<std::string> projectName = ProjectCache::getInstance().get(projectId);
    if(projectName) {
      return projectName.get();
    } else {
      return DONT_EXIST_STR();
    }
  }
};

#endif /* PROJECTTABLE_H */

