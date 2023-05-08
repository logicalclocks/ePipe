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

#include "HopsworksOpsLogTailer.h"

HopsworksOpsLogTailer::HopsworksOpsLogTailer(Ndb *ndb, Ndb *ndbRecovery, const int poll_maxTimeToWait, const Barrier barrier, ProjectsElasticSearch *elastic, const int lru_cap, const std::string search_index)
    : TableTailer(ndb, ndbRecovery, new HopsworksOpsLogTable(), poll_maxTimeToWait, barrier),
      mElasticSearch(elastic), mProjectTable(lru_cap), mDatasetTable(lru_cap), mSearchIndex(search_index){
}

void HopsworksOpsLogTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, HopsworksOpRow pre, HopsworksOpRow row){
  LOG_DEBUG(row.to_string());
  eBulk bulk;
  ptime arrivalTime = Utils::getCurrentTime();
  bulk.mStartProcessing = Utils::getCurrentTime();
  switch (row.mOpOn)
  {
  case Dataset:
    handleDataset(arrivalTime, bulk, row);
    break;
  case Project:
    handleProject(arrivalTime, bulk, row);
    break;
  }
  bulk.mEndProcessing = Utils::getCurrentTime();
  mElasticSearch->addData(bulk);
}

void HopsworksOpsLogTailer::handleDataset(ptime arrivalTime, eBulk &bulk, HopsworksOpRow logEvent){
  std::string json;
  eEvent::EventType eventType;
  if (logEvent.mOpType == HopsworksDelete){
    json = DatasetRow::to_delete_json(mSearchIndex, logEvent.mInodeId);
    eventType = eEvent::EventType::DeleteEvent;
    DatasetProjectSCache2::getInstance().removeDatasetByInodeId(logEvent.mInodeId);
  } else {
    DatasetRow dataset = DatasetProjectSCache2::getInstance().loadDatasetFromId(logEvent.mOpId, mNdbConnection, mDatasetTable);
    json = dataset.to_upsert_json(mSearchIndex, logEvent.mDatasetINodeId);
    eventType = logEvent.mOpType == HopsworksAdd ? eEvent::EventType::AddEvent : eEvent::EventType::UpdateEvent;
  }
  bulk.push(mHopsworksLogTable.getLogRemovalHandler(logEvent), arrivalTime, json, eventType, eEvent::AssetType::Dataset);
}

void HopsworksOpsLogTailer::handleProject(ptime arrivalTime, eBulk &bulk, HopsworksOpRow logEvent){
  std::string json;
  eEvent::EventType eventType;
  if (logEvent.mOpType == HopsworksDelete){
    json = ProjectRow::to_delete_json(mSearchIndex, logEvent.mInodeId);
    eventType = eEvent::EventType::DeleteEvent;
    DatasetProjectSCache2::getInstance().removeProjectByInodeId(logEvent.mInodeId);
  } else {
    ProjectRow project = DatasetProjectSCache2::getInstance().loadProjectFromId(logEvent.mOpId, mNdbConnection, mProjectTable);
    json = project.to_upsert_json(mSearchIndex, logEvent.mInodeId);
    eventType = logEvent.mOpType == HopsworksAdd ? eEvent::EventType::AddEvent : eEvent::EventType::UpdateEvent;
  }
  bulk.push(mHopsworksLogTable.getLogRemovalHandler(logEvent), arrivalTime, json, eventType, eEvent::AssetType::Project);
}

HopsworksOpsLogTailer::~HopsworksOpsLogTailer(){
}
