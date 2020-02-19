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

HopsworksOpsLogTailer::HopsworksOpsLogTailer(Ndb *ndb, Ndb *ndbRecovery, const int poll_maxTimeToWait, const Barrier barrier, ProjectsElasticSearch *elastic, const int lru_cap)
    : TableTailer(ndb, ndbRecovery, new HopsworksOpsLogTable(), poll_maxTimeToWait, barrier),
      mElasticSearch(elastic), mDatasetTable(lru_cap), mTemplateTable(lru_cap){
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
  case Schema:
    handleSchema(arrivalTime, bulk, row);
    break;
  }
  bulk.mEndProcessing = Utils::getCurrentTime();
  mElasticSearch->addData(bulk);
}

void HopsworksOpsLogTailer::handleDataset(ptime arrivalTime, eBulk &bulk, HopsworksOpRow logEvent){
  std::string json;
  eEvent::EventType eventType;
  if (logEvent.mOpType == HopsworksDelete){
    json = DatasetRow::to_delete_json(logEvent.mInodeId);
    eventType = eEvent::EventType::DeleteEvent;
    mDatasetTable.removeDatasetFromCache(logEvent.mInodeId);
  } else {
    DatasetRow dataset = mDatasetTable.get(mNdbConnection, logEvent.mOpId);
    json = dataset.to_upsert_json();
    eventType = logEvent.mOpType == HopsworksAdd ? eEvent::EventType::AddEvent : eEvent::EventType::UpdateEvent;
  }
  bulk.push(mHopsworksLogTable.getLogRemovalHandler(logEvent), arrivalTime, json, eventType, eEvent::AssetType::Dataset);
}

void HopsworksOpsLogTailer::handleProject(ptime arrivalTime, eBulk &bulk, HopsworksOpRow logEvent){
  std::string json;
  eEvent::EventType eventType;
  if (logEvent.mOpType == HopsworksDelete){
    json = ProjectRow::to_delete_json(logEvent.mInodeId);
    eventType = eEvent::EventType::DeleteEvent;
    mDatasetTable.removeProjectFromCache(logEvent.mInodeId);
  } else {
    ProjectRow project = mProjectTable.get(mNdbConnection, logEvent.mOpId);
    json = project.to_upsert_json(logEvent.mInodeId);
    eventType = logEvent.mOpType == HopsworksAdd ? eEvent::EventType::AddEvent : eEvent::EventType::UpdateEvent;
  }
  bulk.push(mHopsworksLogTable.getLogRemovalHandler(logEvent), arrivalTime, json, eventType, eEvent::AssetType::Project);
}

void HopsworksOpsLogTailer::handleSchema(ptime arrivalTime, eBulk &bulk, HopsworksOpRow logEvent){
  if (logEvent.mOpType == HopsworksDelete){
    boost::optional<TemplateRow> tmplate_ptr = mTemplateTable.get(mNdbConnection, logEvent.mOpId);
    if (tmplate_ptr){
      TemplateRow tmplate = tmplate_ptr.get();
      std::string json = tmplate.to_delete_json(logEvent.mInodeId);
      bulk.push(mHopsworksLogTable.getLogRemovalHandler(logEvent), arrivalTime, json, eEvent::EventType::DeleteEvent, eEvent::AssetType::INode);
    } else {
      LOG_WARN("Schema/Template [" << logEvent.mOpId << "] does not exist");
    }
  }else{
    LOG_ERROR("Unsupported Schema Operation [" << HopsworksOpTypeToStr(logEvent.mOpType)
                                               << "]. Only Delete is supported.");
  }
}

HopsworksOpsLogTailer::~HopsworksOpsLogTailer(){
}
