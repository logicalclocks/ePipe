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

FileProvenanceElastic::FileProvenanceElastic(const HttpClientConfig elastic_client_config, int time_to_wait_before_inserting,
    int bulk_size, const bool stats, SConn conn, int file_lru_cap, int xattr_lru_cap)
    : ElasticSearchBase(elastic_client_config, time_to_wait_before_inserting, bulk_size, stats, new MovingCountersBulkSet("file_prov")),
    mConn(conn), mFileProvTable(file_lru_cap, xattr_lru_cap) {}

void FileProvenanceElastic::intProcessOneByOne(eBulk bulk) {
  std::deque<eEvent>::iterator itE, endE;
  std::string mElasticBulkAddr = getElasticSearchBulkUrl();
  for(auto event : bulk.mEvents) {
    if (event.getJSON() != FileProvenanceConstants::ELASTIC_NOP && event.getJSON() != FileProvenanceConstants::ELASTIC_NOP2) {
      LOG_DEBUG("val:" << event.getJSON());
      if (event.getJSON() != FileProvenanceConstants::ELASTIC_NOP) {
        ParsingResponse pr = httpPostRequest(mElasticBulkAddr, event.getJSON());
        if (!pr.mSuccess) {
          if (boost::starts_with(pr.errorMsg, "document_missing_exception:")) {
            LOG_INFO("file prov - elastic document missing - skipped op" << event.getLogHandler()->getDescription() << std::endl << event.getJSON());
          } else {
            LOG_ERROR("file prov - elastic error while processing: " << event.getLogHandler()->getDescription() << std::endl << event.getJSON());
            LOG_FATAL("file prov - elastic - cannot recover");
          }
        }
      }
      mFileProvTable.cleanLog(mConn, event.getLogHandler());
    }
  }
}

bool FileProvenanceElastic::intProcessBatch(std::string val, std::vector<eBulk>* bulks, std::vector<const LogHandler*> cleanupHandlers, ptime start_time) {
  if (!val.empty()) {
    LOG_DEBUG("file prov - elastic bulk write size:" << val.length() << " val:" << val);
    std::string mElasticBulkAddr = getElasticSearchBulkUrl();
    if (httpPostRequest(mElasticBulkAddr, val).mSuccess) {
      //bulk success
      mFileProvTable.cleanLogs(mConn, cleanupHandlers);
      if (mStats && !bulks->empty()) {
        mCounters->bulksProcessed(start_time, bulks);
      }
      return true;
    } else {
      return false;
    }
  } else {
    //maybe this was only nops for this index
    LOG_TRACE("file prov - elastic bulk has only nop events");
    mFileProvTable.cleanLogs(mConn, cleanupHandlers);
    if (mStats && !bulks->empty()) {
      mCounters->bulksProcessed(start_time, bulks);
    }
    return true;
  }
}

void FileProvenanceElastic::process(std::vector<eBulk>* bulks) {
  LOG_DEBUG("file prov - elastic writting batch to index consists of events:" << bulks->size());
  ptime start_time = Utils::getCurrentTime();
  std::string val = "";
  std::vector<const LogHandler*> cleanupHandlers;

  for(auto bulk : *bulks) {
    if(mStats){
      mCounters->bulkReceived(bulk);
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
    LOG_INFO("file prov - elastic batch write failed - trying one by one");
    //bulk failed - process one by one and stop and failing one
    for(auto bulk : *bulks) {
      intProcessOneByOne(bulk);
      if (mStats) {
        mCounters->bulkProcessed(start_time, bulk);
      }
    }
  }
}

FileProvenanceElastic::~FileProvenanceElastic() {
}