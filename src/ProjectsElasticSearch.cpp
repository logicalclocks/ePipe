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

#include "ProjectsElasticSearch.h"
#include "MetadataLogTailer.h"

using namespace Utils;

ProjectsElasticSearch::ProjectsElasticSearch(const HttpClientConfig elastic_client_config,
        int time_to_wait_before_inserting,
        int bulk_size, const bool stats, MConn conn) : ElasticSearchBase
        (elastic_client_config, time_to_wait_before_inserting, bulk_size, stats,
         new MovingCountersBulkSet("fs")),
         mConn(conn) {
  mElasticBulkAddr = getElasticSearchBulkUrl();
}

void ProjectsElasticSearch::process(std::vector<eBulk>* bulks) {
  std::vector<const LogHandler*> logRHandlers;
  std::string batch;
  int fslogs=0, metalogs=0, hopsworkslogs=0;
  for (auto it = bulks->begin(); it != bulks->end();++it) {
    eBulk bulk = *it;
    LOG_DEBUG(bulk.toString());
    batch += bulk.batchJSON();
    logRHandlers.insert(logRHandlers.end(), bulk.mLogHandlers.begin(),
        bulk.mLogHandlers.end());
    fslogs += bulk.getCount(LogType::FSLOG);
    metalogs += bulk.getCount(LogType::METALOG);
    hopsworkslogs += bulk.getCount(LogType::HOPSWORKSLOG);
    if(mStats){
      mCounters->bulkReceived(bulk);
    }
  }

  ptime start_time = Utils::getCurrentTime();
  if (httpPostRequest(mElasticBulkAddr, batch).mSuccess) {
    if (metalogs > 0) {
      MetadataLogTable().removeLogs(mConn.metadataConnection, logRHandlers);
    }

    if (fslogs > 0) {
      FsMutationsLogTable().removeLogs(mConn.inodeConnection, logRHandlers);
    }

    if(hopsworkslogs > 0){
      HopsworksOpsLogTable().removeLogs(mConn.metadataConnection, logRHandlers);
    }

    if (mStats) {
      mCounters->bulksProcessed(start_time, bulks);
    }
  }else{
    for (auto it = bulks->begin(); it != bulks->end();++it) {
      eBulk bulk = *it;
      for(eEvent event : bulk.mEvents){
        if(!bulkRequest(event)){
          LOG_FATAL("Failure while processing log : " << event.getLogHandler
          ()->getDescription() << std::endl << event.getJSON());
        }
      }
      if (mStats) {
        mCounters->bulkProcessed(start_time, bulk);
      }
    }
  }
}


bool ProjectsElasticSearch::bulkRequest(eEvent& event) {
  if (httpPostRequest(mElasticBulkAddr, event.getJSON()).mSuccess){
    if(event.getLogHandler()->getType() == LogType::FSLOG){
      event.getLogHandler()->removeLog(mConn.inodeConnection);
    }else if(event.getLogHandler()->getType() == LogType::METALOG || event.getLogHandler()->getType() == LogType::HOPSWORKSLOG){
      event.getLogHandler()->removeLog(mConn.metadataConnection);
    }
    return true;
  }
  return false;
}

ProjectsElasticSearch::~ProjectsElasticSearch() {
}

