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

#include "AppProvenanceElastic.h"

AppProvenanceElastic::AppProvenanceElastic(const HttpClientConfig elastic_client_config, std::string index,
        int time_to_wait_before_inserting, int bulk_size, const bool stats, SConn conn) : 
ElasticSearchBase(elastic_client_config, time_to_wait_before_inserting,
    bulk_size, stats, new MovingCountersBulkSet("app_prov")),
mIndex(index), mConn(conn) {
  mElasticBulkAddr = getElasticSearchBulkUrl(mIndex);
}

void AppProvenanceElastic::process(std::vector<eBulk>* bulks) {
  std::vector<const LogHandler*> logRHandlers;
  std::string batch;
  for (auto it = bulks->begin(); it != bulks->end();++it) {
    eBulk bulk = *it;
    batch += bulk.batchJSON();
    logRHandlers.insert(logRHandlers.end(), bulk.mLogHandlers.begin(), bulk.mLogHandlers.end());
    if(mStats){
      mCounters->bulkReceived(bulk);
    }
  }

  ptime start_time = Utils::getCurrentTime();
  if (httpPostRequest(mElasticBulkAddr, batch).mSuccess) {
    AppProvenanceLogTable().removeLogs(mConn, logRHandlers);
    if (mStats) {
      mCounters->bulksProcessed(start_time, bulks);
    }
  }else{
    for (auto it = bulks->begin(); it != bulks->end();++it) {
      eBulk bulk = *it;
      for(eEvent event : bulk.mEvents){
        if(!bulkRequest(event)){
          LOG_FATAL("app prov - elastic failure while processing log : "
          << event.getLogHandler()->getDescription() << std::endl << event.getJSON());
        }
      }
      if (mStats) {
        mCounters->bulkProcessed(start_time, bulk);
      }
    }
  }
}

bool AppProvenanceElastic::bulkRequest(eEvent& event) {
  if (httpPostRequest(mElasticBulkAddr, event.getJSON()).mSuccess){
    event.getLogHandler()->removeLog(mConn);
    return true;
  }
  return false;
}

AppProvenanceElastic::~AppProvenanceElastic() {
}

