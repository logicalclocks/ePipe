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

#ifndef DATASETTABLE_H
#define DATASETTABLE_H

#include "DBTable.h"
#include "DocType.h"

struct DatasetRow {
  int mId;
  std::string mDatasetName;
  int mProjectId;
  std::string mDescription;
  bool mPublicDS;

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
    docWriter.String(DOC_TYPE_DATASET);

    docWriter.String("dataset_id");
    docWriter.Int64(mId);

    docWriter.String("project_id");
    docWriter.Int(mProjectId);

    docWriter.String("description");
    docWriter.String(mDescription.c_str());

    docWriter.String("public_ds");
    docWriter.Bool(mPublicDS);

    docWriter.EndObject();
    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);
    docWriter.EndObject();

    out << sbDoc.GetString() << std::endl;
    return out.str();
  }

};

typedef std::vector<DatasetRow> DatasetVec;

class DatasetTable : public DBTable<DatasetRow> {
public:

  DatasetTable(int lru_cap) : DBTable("dataset") {
    addColumn("id");
    addColumn("inode_name");
    addColumn("projectId");
    addColumn("description");
    addColumn("public_ds");
  }

  DatasetRow getRow(NdbRecAttr* values[]) {
    DatasetRow row;
    row.mId = values[0]->int32_value();
    row.mDatasetName = get_string(values[1]);
    row.mProjectId = values[2]->int32_value();
    row.mDescription = get_string(values[3]);
    row.mPublicDS = values[4]->int8_value() == 1;
    return row;
  }

  DatasetRow get(Ndb* connection, int datasetId) {
    DatasetRow ds = doRead(connection, datasetId);
    return ds;
  }

  DatasetRow get(Ndb* connection, std::string datasetName, int projectId) {
    AnyMap args;
    args[1] = datasetName;
    args[2] = projectId;
    DatasetVec datasets = doRead(connection, "projectId_name", args);
    DatasetRow dataset;
    if(datasets.size() == 1) {
      dataset = datasets[0];
    } else if (datasets.size() == 0) {
      std::stringstream cause;
      cause << "Dataset [" << datasetName << "] does not exist in Project[" << projectId << "]";
      LOG_FATAL(cause.str());
    } else {
      std::stringstream cause;
      cause << "Dataset [" << datasetName << "] has multiple entries in Project[" << projectId << "]";
      LOG_FATAL(cause.str());
    }
    return dataset;
  }

  // This method should be avoided as much as possible since it triggers an index scan
  DatasetVec getByProjectId(Ndb* connection, Int64 projectId) {
    AnyMap key;
    key[2] = projectId;
    return doRead(connection, "projectId_name", key);
  }

protected:

  void applyConditionOnGetAll(NdbScanFilter& filter) {
    filter.begin(NdbScanFilter::AND);
    filter.eq(getColumnIdInDB("searchable"), (Uint32) 1);
    filter.end();
  }

};

#endif /* DATASETTABLE_H */

