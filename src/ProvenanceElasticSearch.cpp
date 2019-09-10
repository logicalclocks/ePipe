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

#include "ProvenanceElasticSearch.h"

ProvenanceElasticSearch::ProvenanceElasticSearch(std::string elastic_addr,
        std::string index, int time_to_wait_before_inserting,
        int bulk_size, const bool stats, SConn conn) :
ElasticSearchBase(elastic_addr, time_to_wait_before_inserting, bulk_size),
mIndex(index), mStats(stats), mConn(conn) {
  mElasticBulkAddr = getElasticSearchBulkUrl(mIndex);
}

void ProvenanceElasticSearch::process(std::vector<eBulk>* bulks) {
  std::string batch;
  std::vector<const LogHandler*> logRHandlers;
  for (auto it = bulks->begin(); it != bulks->end(); ++it) {
    eBulk bulk = *it;
    batch.append(bulk.batchJSON());
    logRHandlers.insert(logRHandlers.end(), bulk.mLogHandlers.begin(),
                        bulk.mLogHandlers.end());
  }

  //TODO: handle failures
  if (httpPostRequest(mElasticBulkAddr, batch)) {
    if (!logRHandlers.empty()) {
      ProvenanceLogTable().removeLogs(mConn, logRHandlers);
    }

  }
  //TODO: stats
}

ProvenanceElasticSearch::~ProvenanceElasticSearch() {

}

