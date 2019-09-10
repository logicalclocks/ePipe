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

void ProvenanceElasticSearch::process(std::vector<PBulk>* bulks) {
  PKeys keys;
  std::string batch;
  for (std::vector<PBulk>::iterator it = bulks->begin(); it != bulks->end(); ++it) {
    PBulk bulk = *it;
    batch.append(bulk.mJSON);
    keys.insert(keys.end(), bulk.mPKs.begin(), bulk.mPKs.end());
  }

  //TODO: handle failures
  if (httpPostRequest(mElasticBulkAddr, batch)) {
    if (!keys.empty()) {
      ProvenanceLogTable().removeLogs(mConn, keys);
    }

  }
  //TODO: stats
}

ProvenanceElasticSearch::~ProvenanceElasticSearch() {

}

