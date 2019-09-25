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

