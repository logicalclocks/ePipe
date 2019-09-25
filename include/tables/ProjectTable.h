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

#define DOC_TYPE_PROJECT "proj"

struct ProjectRow {
  int mId;
  Int64 mInodeParentId;
  Int64 mInodePartitionId;
  std::string mInodeName;
  std::string mUserName;
  std::string mDescription;

  std::string to_create_json() {
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();

    docWriter.String("doc");
    docWriter.StartObject();

    docWriter.String("doc_type");
    docWriter.String(DOC_TYPE_PROJECT);

    docWriter.String("project_id");
    docWriter.Int(mId);

    docWriter.String("parent_id");
    docWriter.Int64(mInodeParentId);

    docWriter.String("partition_id");
    docWriter.Int64(mInodePartitionId);
    
    docWriter.String("name");
    docWriter.String(mInodeName.c_str());

    docWriter.String("user");
    docWriter.String(mUserName.c_str());

    docWriter.String("description");
    docWriter.String(mDescription.c_str());

    docWriter.EndObject();
    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);
    docWriter.EndObject();

    return std::string(sbDoc.GetString());
  }

};

class ProjectTable : public DBTable<ProjectRow> {
public:

  ProjectTable() : DBTable("project") {
    addColumn("id");
    addColumn("inode_pid");
    addColumn("partition_id");
    addColumn("inode_name");
    addColumn("username");
    addColumn("description");
  }

  ProjectRow get(Ndb* connection, int projectId) {
    return doRead(connection, projectId);
  }

  ProjectRow getRow(NdbRecAttr* values[]) {
    ProjectRow row;
    row.mId = values[0]->int32_value();
    row.mInodeParentId = values[1]->int64_value();
    row.mInodePartitionId = values[2]->int64_value();
    row.mInodeName = get_string(values[3]);
    row.mUserName = get_string(values[4]);
    row.mDescription = get_string(values[5]);
    return row;
  }
};

#endif /* PROJECTTABLE_H */

