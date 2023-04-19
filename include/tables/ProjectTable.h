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
#include "DocType.h"
#include "INodeTable.h"

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

typedef CacheSingleton<Cache<int, ProjectRow>> ProjectCacheById;
typedef CacheSingleton<Cache<std::string, ProjectRow>> ProjectCacheByName;
typedef std::vector<ProjectRow> ProjectVec;

class ProjectTable : public DBTable<ProjectRow> {
public:

  ProjectTable(int lru_cap) : DBTable("project") {
    addColumn("id");
    addColumn("projectname");
    addColumn("username");
    addColumn("description");
    ProjectCacheById::getInstance(lru_cap, "ProjectCacheById");
    ProjectCacheByName::getInstance(lru_cap, "ProjectCacheByName");
  }

  ProjectRow get(Ndb* connection, int projectId) {
    ProjectRow row = doRead(connection, projectId);
    return row;
  }

  ProjectRow getByName(Ndb* connection, std::string projectName) {
    AnyMap args;
    args[1] = projectName;
    ProjectVec projects = doRead(connection, getColumn(1), args);
    if(projects.size() == 1) {
      return projects[0];
    } else if (projects.size() == 0) {
      std::stringstream cause;
      cause << "Project [" << projectName << "] does not exist";
      throw std::logic_error(cause.str());
    } else {
      std::stringstream cause;
      cause << "Project [" << projectName << "] has multiple entries with the same name";
      throw std::logic_error(cause.str());
    }
  }

  ProjectRow getRow(NdbRecAttr* values[]) {
    ProjectRow row;
    row.mId = values[0]->int32_value();
    row.mProjectName = get_string(values[1]);
    row.mUserName = get_string(values[2]);
    row.mDescription = get_string(values[3]);
    return row;
  }

  void updateProjectCache(ProjectRow project) {
    ProjectCacheById::getInstance().put(project.mId, project);
    ProjectCacheByName::getInstance().put(project.mProjectName, project);
  }

  ProjectRow loadProjectByName(Ndb* connection, std::string projectName) {
    boost::optional<ProjectRow> projectOpt = ProjectCacheByName::getInstance().get(projectName);
    if (projectOpt) {
      return projectOpt.get();
    } else {
      ProjectRow project = getByName(connection, projectName);
      updateProjectCache(project);
      return project;
    }
  }

  std::string getProjectNameFromCache(int projectId) {
    boost::optional<ProjectRow> project = ProjectCacheById::getInstance().get(projectId);
    if(project) {
      return project.get().mProjectName;
    } else {
      return DONT_EXIST_STR();
    }
  }

  ProjectRow getProjectByInodeId(Ndb* connection, INodeTable& inodesTable, Int64 projectInodeId) {
    INodeRow projectInode = inodesTable.loadProjectInode(connection, projectInodeId);
    return loadProjectByName(connection, projectInode.mName);
  }
};

#endif /* PROJECTTABLE_H */

