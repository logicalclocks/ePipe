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
#include "DatasetProjectCache.h"

#define DOC_TYPE_DATASET "ds"

struct DatasetRow {
  int mId;
  Int64 mInodeId;
  Int64 mInodeParentId;
  std::string mInodeName;
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

  std::string to_upsert_json(std::string index) {
    std::stringstream out;
    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("update");
    opWriter.StartObject();

    opWriter.String("_id");
    opWriter.Int64(mInodeId);
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
    docWriter.Int64(mInodeId);

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

class DPCache : public DatasetProjectCache {
public:

  DPCache(int lru_cap, const char* prefix) : DatasetProjectCache(lru_cap, prefix) {
  }
};

typedef CacheSingleton<DPCache> DatasetProjectSCache;
typedef std::vector<DatasetRow> DatasetVec;

class DatasetTable : public DBTable<DatasetRow> {
public:

  DatasetTable(int lru_cap) : DBTable("dataset") {
    addColumn("id");
    addColumn("inode_id");
    addColumn("inode_pid");
    addColumn("inode_name");
    addColumn("projectId");
    addColumn("description");
    addColumn("public_ds");
    DatasetProjectSCache::getInstance(lru_cap, "DatasetProject");
  }

  DatasetRow getRow(NdbRecAttr* values[]) {
    DatasetRow row;
    row.mId = values[0]->int32_value();
    row.mInodeId = values[1]->int64_value();
    row.mInodeParentId = values[2]->int64_value();
    row.mInodeName = get_string(values[3]);
    row.mProjectId = values[4]->int32_value();
    row.mDescription = get_string(values[5]);
    row.mPublicDS = values[6]->int8_value() == 1;
    return row;
  }

  DatasetRow get(Ndb* connection, int datasetId) {
    DatasetRow ds = doRead(connection, datasetId);
    DatasetProjectSCache::getInstance().add(ds.mInodeId, ds.mProjectId, ds.mInodeName);
    return ds;
  }

  void removeDatasetFromCache(Int64 datasetINodeId) {
    DatasetProjectSCache::getInstance().removeDataset(datasetINodeId);
  }

  void removeProjectFromCache(int projectId) {
    DatasetProjectSCache::getInstance().removeProject(projectId);
  }

  int getProjectIdFromCache(Int64 datasetINodeId) {
    boost::optional<int> projectId = DatasetProjectSCache::getInstance().getParentProject(datasetINodeId);
    if(projectId) {
      return projectId.get();
    } else {
      return DONT_EXIST_INT();
    }
  }

  std::string getDatasetNameFromCache(Int64 datasetINodeId) {
    boost::optional<std::string> datasetName = DatasetProjectSCache::getInstance().getDatasetValue(datasetINodeId);
    if(datasetName) {
      return datasetName.get();
    } else {
      return DONT_EXIST_STR();
    }
  }

  void loadProjectIds(Ndb* connection, ULSet& datasetsINodeIds, ProjectTable& projectTable) {
    ULSet dataset_inode_ids;
    for (ULSet::iterator it = datasetsINodeIds.begin(); it != datasetsINodeIds.end(); ++it) {
      Int64 datasetId = *it;
      if (!DatasetProjectSCache::getInstance().containsDataset(datasetId)) {
        dataset_inode_ids.insert(datasetId);
      }
    }

    if (dataset_inode_ids.empty()) {
      LOG_DEBUG("All required projectIds are already in the cache");
      return;
    }

    for (ULSet::iterator it = dataset_inode_ids.begin(); it != dataset_inode_ids.end(); ++it) {
      Int64 dataset_inode_id = *it;
      AnyMap args;
      //DatasetInodeId
      args[1] = dataset_inode_id;
      
      DatasetVec datasets = doRead(connection, getColumn(1), args);

      UISet projectIds;
      for (DatasetVec::iterator it = datasets.begin(); it != datasets.end(); ++it) {
        DatasetRow row = *it;
        if (row.mInodeId != dataset_inode_id) {
          LOG_ERROR("Dataset [" << dataset_inode_id << "] doesn't exists");
          continue;
        }
        
        if (projectIds.empty()) {
          DatasetProjectSCache::getInstance().add(dataset_inode_id, row.mProjectId, row.mInodeName);
          projectTable.loadProject(connection, row.mProjectId);
        }
        projectIds.insert(row.mProjectId);
      }

      if (projectIds.size() > 1) {
        LOG_ERROR("Got " << datasets.size() << " rows of the original Dataset [" 
                << dataset_inode_id << "] in projects " << Utils::to_string(projectIds) << ", only one was expected");
      }
    }
  }

protected:

  void applyConditionOnGetAll(NdbScanFilter& filter) {
    filter.begin(NdbScanFilter::AND);
    filter.eq(getColumnIdInDB("searchable"), (Uint32) 1);
    filter.end();
  }

};

#endif /* DATASETTABLE_H */

