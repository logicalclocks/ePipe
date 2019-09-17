/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * ePipe is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * ePipe is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */

#include "HopsworksOpsLogTailer.h"


HopsworksOpsLogTailer::HopsworksOpsLogTailer(Ndb *ndb, Ndb *ndbRecovery, const
int poll_maxTimeToWait, const Barrier barrier, ProjectsElasticSearch *
elastic, const int lru_cap)
    : TableTailer(ndb, ndbRecovery, new HopsworksOpsLogTable(),
                  poll_maxTimeToWait,
                  barrier), mElasticSearch(elastic), mDatasetTable(lru_cap),
      mTemplateTable(lru_cap) {
}

void
HopsworksOpsLogTailer::handleEvent(NdbDictionary::Event::TableEvent eventType,
                                   HopsworksOpRow pre, HopsworksOpRow row) {
  LOG_DEBUG(row.to_string());
  switch (row.mOpOn) {
    case Dataset:
      handleDataset(row.mOpId, row.mOpType, row.mInodeId, row.mProjectId);
      break;
    case Project:
      handleProject(row.mOpId, row.mInodeId, row.mOpType);
      break;
    case Schema:
      handleSchema(row.mOpId, row.mOpType, row.mInodeId);
      break;
  }

  //Elastic should fail if it didn't succeeded to perform the request
  mHopsworksLogTable.removeLog(mNdbConnection, row.mId);
}

void HopsworksOpsLogTailer::handleDataset(int opId, HopsworksOpType opType,
                                          Int64 datasetINodeId, int projectId) {
  if (opType == HopsworksDelete) {
    handleDeleteDataset(datasetINodeId);
  } else {
    handleUpsertDataset(opId, opType, datasetINodeId, projectId);
  }
}

void HopsworksOpsLogTailer::handleUpsertDataset(int opId, HopsworksOpType
opType, Int64 datasetINodeId, int projectId) {
  DatasetRow row = mDatasetTable.get(mNdbConnection, opId);
  mElasticSearch->addDataset(datasetINodeId, row.to_create_json());
  switch (opType) {
    case HopsworksAdd:
      LOG_INFO("Add Dataset[" << datasetINodeId << "]: Succeeded");
      break;
    case HopsworksUpdate:
      LOG_INFO("Update Dataset[" << datasetINodeId << "]: Succeeded");
      break;
  }
}

void HopsworksOpsLogTailer::handleDeleteDataset(Int64 datasetINodeId) {
  mElasticSearch->removeDataset(datasetINodeId);
  LOG_INFO("Delete Dataset[" << datasetINodeId << "] Succeeded");
  mDatasetTable.removeDatasetFromCache(datasetINodeId);
}

void HopsworksOpsLogTailer::handleProject(int projectId, Int64 inodeId,
                                          HopsworksOpType opType) {
  if (opType == HopsworksDelete) {
    handleDeleteProject(projectId, inodeId);
  } else {
    handleUpsertProject(projectId, inodeId, opType);
  }
}

void HopsworksOpsLogTailer::handleDeleteProject(int projectId, Int64
projectINodeId) {
  mElasticSearch->removeProject(projectINodeId);
  LOG_INFO("Delete Project[" << projectId << ", " << projectINodeId
                             << "] Succeeded");
  mDatasetTable.removeProjectFromCache(projectId);
}

void HopsworksOpsLogTailer::handleUpsertProject(int projectId, Int64 inodeId,
                                                HopsworksOpType opType) {
  ProjectRow row = mProjectTable.get(mNdbConnection, projectId);
  mElasticSearch->addProject(inodeId, row.to_create_json());
  switch (opType) {
    case HopsworksAdd:
      LOG_INFO("Add Project[" << projectId << "]: Succeeded");
      break;
    case HopsworksUpdate:
      LOG_INFO("Update Project[" << projectId << "]: Succeeded");
      break;
  }
}

void HopsworksOpsLogTailer::handleSchema(int schemaId, HopsworksOpType opType,
                                         Int64 inodeId) {
  if (opType == HopsworksDelete) {
    handleSchemaDelete(schemaId, inodeId);
  } else {
    LOG_ERROR("Unsupported Schema Operation [" << HopsworksOpTypeToStr(opType)
                                               << "]. Only Delete is supported.");
  }
}

void HopsworksOpsLogTailer::handleSchemaDelete(int schemaId, Int64 inodeId) {
  boost::optional<TemplateRow> tmplate_ptr = mTemplateTable.get(mNdbConnection,
                                                                schemaId);
  if (tmplate_ptr) {
    TemplateRow tmplate = tmplate_ptr.get();
    mElasticSearch->deleteSchemaForINode(inodeId, tmplate.to_delete_json());
    LOG_INFO("Delete Schema/Template [" << schemaId << ", " << tmplate.mName
                                        << "] for INode [" << inodeId
                                        << "] : Succeeded");
  } else {
    LOG_WARN("Schema/Template [" << schemaId << "] does not exist");
  }
}

HopsworksOpsLogTailer::~HopsworksOpsLogTailer() {

}

