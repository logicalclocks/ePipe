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
 * File:   HopsworksOpsLogTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "HopsworksOpsLogTailer.h"

using namespace Utils;

HopsworksOpsLogTailer::HopsworksOpsLogTailer(MConn conn, const int
poll_maxTimeToWait, const Barrier barrier,
        ProjectsElasticSearch* elastic, const int lru_cap)
: TableTailer(conn.metadataConnection, new HopsworksOpsLogTable(), poll_maxTimeToWait, barrier),
mElasticSearch(elastic), mDatasetTable(lru_cap), mTemplateTable(lru_cap),
mINodeConnection(conn.inodeConnection), mINodeTable(lru_cap) {

}

void HopsworksOpsLogTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, HopsworksOpRow pre, HopsworksOpRow row) {
  LOG_DEBUG(row.to_string());
  bool success = false;
  switch (row.mOpOn) {
    case Dataset:
      success = handleDataset(row.mOpId, row.mOpType, row.mInodeId, row.mProjectId);
      break;
    case Project:
      success = handleProject(row.mOpId, row.mInodeId, row.mOpType);
      break;
    case Schema:
      success = handleSchema(row.mOpId, row.mOpType, row.mInodeId);
      break;
  }

  if (success) {
    mHopsworksLogTable.removeLog(mNdbConnection, row.mId);
  }
}

bool HopsworksOpsLogTailer::handleDataset(int opId, HopsworksOpType opType,
        Int64 datasetINodeId, int projectId) {
  if (opType == HopsworksDelete) {
    return handleDeleteDataset(datasetINodeId);
  } else {
    return handleUpsertDataset(opId, opType, datasetINodeId, projectId);
  }
}

bool HopsworksOpsLogTailer::handleUpsertDataset(int opId, HopsworksOpType opType, Int64 datasetINodeId, int projectId) {
  DatasetRow row = mDatasetTable.get(mNdbConnection, opId);
  ProjectRow projectRow = mProjectTable.get(mNdbConnection, projectId);
  INodeRow inodeRow = mINodeTable.get(mINodeConnection, row.mInodeParentId, row
      .mInodeName, row.mInodePartitionId);
  row.mModificationTime = inodeRow.mModificationTime;
  row.mUserName = projectRow.mUserName;

  bool success = mElasticSearch->addDoc(datasetINodeId, row.to_create_json());
  if (success) {
    switch (opType) {
      case HopsworksAdd:
        LOG_INFO("Add Dataset[" << datasetINodeId << "]: Succeeded");
        break;
      case HopsworksUpdate:
        LOG_INFO("Update Dataset[" << datasetINodeId << "]: Succeeded");
        break;
    }
  }

  return success;
}

bool HopsworksOpsLogTailer::handleDeleteDataset(Int64 datasetINodeId) {
  string query = DatasetRow::to_delete_json(datasetINodeId);
  //TODO: handle failures in elastic search
  bool success = mElasticSearch->deleteDocsByQuery(query);
  if (success) {
    LOG_INFO("Delete Dataset[" << datasetINodeId << "] and all children inodes: Succeeded");
  }

  mDatasetTable.removeDatasetFromCache(datasetINodeId);
  return success;
}

bool HopsworksOpsLogTailer::handleProject(int projectId, Int64 inodeId, HopsworksOpType opType) {
  if (opType == HopsworksDelete) {
    return handleDeleteProject(projectId);
  } else {
    return handleUpsertProject(projectId, inodeId, opType);
  }
}

bool HopsworksOpsLogTailer::handleDeleteProject(int projectId) {

  string query = ProjectRow::to_delete_json(projectId);
  //TODO: handle failures in elastic search
  bool success = mElasticSearch->deleteDocsByQuery(query);
  if (success) {
    LOG_INFO("Delete Project[" << projectId << "] and all children datasets and inodes: Succeeded");
  }

  mDatasetTable.removeProjectFromCache(projectId);
  return success;
}

bool HopsworksOpsLogTailer::handleUpsertProject(int projectId, Int64 inodeId, HopsworksOpType opType) {

  ProjectRow row = mProjectTable.get(mNdbConnection, projectId);
  INodeRow inodeRow = mINodeTable.get(mINodeConnection, row.mInodeParentId, row
  .mInodeName, row.mInodePartitionId);
  row.mModificationTime = inodeRow.mModificationTime;

  bool success = mElasticSearch->addDoc(inodeId, row.to_create_json());
  if (success) {
    switch (opType) {
      case HopsworksAdd:
        LOG_INFO("Add Project[" << projectId << "]: Succeeded");
        break;
      case HopsworksUpdate:
        LOG_INFO("Update Project[" << projectId << "]: Succeeded");
        break;
    }
  }

  return success;

}

bool HopsworksOpsLogTailer::handleSchema(int schemaId, HopsworksOpType opType, Int64 inodeId) {
  if (opType == HopsworksDelete) {
    return handleSchemaDelete(schemaId, inodeId);
  } else {
    LOG_ERROR("Unsupported Schema Operation [" << HopsworksOpTypeToStr(opType) << "]. Only Delete is supported.");
    return true;
  }
}

bool HopsworksOpsLogTailer::handleSchemaDelete(int schemaId, Int64 inodeId) {
  boost::optional<TemplateRow> tmplate_ptr = mTemplateTable.get(mNdbConnection, schemaId);
  if (tmplate_ptr) {
    TemplateRow tmplate = tmplate_ptr.get();
    bool success = mElasticSearch->deleteSchemaForINode(inodeId, tmplate.to_delete_json());
    if (success) {
      LOG_INFO("Delete Schema/Template [" << schemaId << ", " << tmplate.mName << "] for INode [" << inodeId << "] : Succeeded");
    }
    return success;
  } else {
    LOG_WARN("Schema/Template [" << schemaId << "] does not exist");
    return true;
  }
}

HopsworksOpsLogTailer::~HopsworksOpsLogTailer() {

}

