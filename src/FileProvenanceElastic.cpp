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

#include <FileProvenanceConstants.h>
#include "FileProvenanceElastic.h"

FileProvenanceElastic::FileProvenanceElastic(std::string elastic_addr, int time_to_wait_before_inserting,
    int bulk_size, const bool stats, SConn conn) :
ElasticSearchWithMetrics(elastic_addr, time_to_wait_before_inserting, bulk_size, stats), mConn(conn) {}

void FileProvenanceElastic::intProcessOneByOne(eBulk bulk) {
  std::vector<eBulk>::iterator itB, endB;
  std::deque<eEvent>::iterator itE, endE;
  std::string mElasticBulkAddr = getElasticSearchBulkUrl();
  for(auto event : bulk.mEvents) {
    if (event.getJSON() != FileProvenanceConstants::ELASTIC_NOP
        && event.getJSON() != FileProvenanceConstants::ELASTIC_NOP2) {
      LOG_DEBUG("val:" << event.getJSON());
      if (event.getJSON() != FileProvenanceConstants::ELASTIC_NOP) {
        if (httpPostRequest(mElasticBulkAddr, event.getJSON())){
          FileProvenanceLogTable().cleanLog(mConn, event.getLogHandler());
        } else {
          LOG_FATAL("Failure while processing log : "
                        << event.getLogHandler()->getDescription() << std::endl << event.getJSON());
        }
      }
      FileProvenanceLogTable().cleanLog(mConn, event.getLogHandler());
    }
  }
}

bool FileProvenanceElastic::intProcessBatch(std::string val, std::vector<eBulk>* bulks, std::vector<const LogHandler*> cleanupHandlers, ptime start_time) {
  LOG_DEBUG("writting batch to index consists of events:" << bulks->size());
  if (!val.empty()) {
    LOG_DEBUG("bulk write size:" << val.length() << " val:" << val);
    std::string mElasticBulkAddr = getElasticSearchBulkUrl();
    if (httpPostRequest(mElasticBulkAddr, val)) {
      //bulk success
      FileProvenanceLogTable().cleanLogs(mConn, cleanupHandlers);
      if (mStats && !bulks->empty()) {
        mCounters.bulksProcessed(start_time, bulks);
      }
      return true;
    } else {
      return false;
    }
  } else {
    //maybe this was only nops for this index
    LOG_DEBUG("only nop events");
    FileProvenanceLogTable().cleanLogs(mConn, cleanupHandlers);
    if (mStats && !bulks->empty()) {
      LOG_DEBUG("adding to stats");
      mCounters.bulksProcessed(start_time, bulks);
      LOG_DEBUG("added to stats");
    }
    return true;
  }
}

void FileProvenanceElastic::process(std::vector<eBulk>* bulks) {
  LOG_DEBUG("processing batch of size:" << bulks->size());
  ptime start_time = Utils::getCurrentTime();
  std::string val = "";
  std::vector<const LogHandler*> cleanupHandlers;

  for(auto bulk : *bulks) {
    if(mStats){
      mCounters.bulkReceived(bulk);
    }
    for(auto event : bulk.mEvents) {
      if (event.getJSON() != FileProvenanceConstants::ELASTIC_NOP
      && event.getJSON() != FileProvenanceConstants::ELASTIC_NOP2) {
        val += event.getJSON();
      }
      cleanupHandlers.push_back(event.getLogHandler());
    }
  }
  if(!intProcessBatch(val, bulks, cleanupHandlers, start_time)) {
    LOG_INFO("batch write failed - trying one by one");
    //bulk failed - process one by one and stop and failing one
    for(auto bulk : *bulks) {
      intProcessOneByOne(bulk);
      if (mStats) {
        mCounters.bulkProcessed(start_time, bulk);
      }
    }
  }
}

FileProvenanceElastic::~FileProvenanceElastic() {
}