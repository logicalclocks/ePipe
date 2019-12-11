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

ProjectsElasticSearch::ProjectsElasticSearch(const HttpClientConfig elastic_client_config, std::string index,
        int time_to_wait_before_inserting,
        int bulk_size, const bool stats, MConn conn) : ElasticSearchBase(elastic_client_config, time_to_wait_before_inserting, bulk_size),
mIndex(index),
mStats(stats), mConn(conn), mStartTime(getCurrentTime()){
  mElasticBulkAddr = getElasticSearchBulkUrl(mIndex);
}

void ProjectsElasticSearch::process(std::vector<eBulk>* bulks) {
  std::vector<const LogHandler*> logRHandlers;
  std::string batch;
  int fslogs=0, metalogs=0;
  for (auto it = bulks->begin(); it != bulks->end();++it) {
    eBulk bulk = *it;
    batch += bulk.batchJSON();
    logRHandlers.insert(logRHandlers.end(), bulk.mLogHandlers.begin(),
        bulk.mLogHandlers.end());
    fslogs += bulk.getCount(LogType::FSLOG);
    metalogs += bulk.getCount(LogType::METALOG);
    if(mStats){
      mCounters.bulkReceived(bulk);
    }
  }

  ptime start_time = Utils::getCurrentTime();
  if (httpPostRequest(mElasticBulkAddr, batch)) {
    if (metalogs > 0) {
      MetadataLogTable().removeLogs(mConn.metadataConnection, logRHandlers);
    }

    if (fslogs > 0) {
      FsMutationsLogTable().removeLogs(mConn.inodeConnection, logRHandlers);
    }
    if (mStats) {
      mCounters.bulksProcessed(start_time, bulks);
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
        mCounters.bulkProcessed(start_time, bulk);
      }
    }
  }
}


bool ProjectsElasticSearch::bulkRequest(eEvent& event) {
  if (httpPostRequest(mElasticBulkAddr, event.getJSON())){
    if(event.getLogHandler()->getType() == LogType::FSLOG){
      event.getLogHandler()->removeLog(mConn.inodeConnection);
    }else if(event.getLogHandler()->getType() ==
             LogType::METALOG){
      event.getLogHandler()->removeLog(mConn.metadataConnection);
    }
    return true;
  }
  return false;
}

void ProjectsElasticSearch::addDoc(Int64 inodeId, std::string json) {
  std::string url = getElasticSearchUpdateDocUrl(mIndex, inodeId);
  if(!httpPostRequest(url, json)){
    LOG_FATAL("Failure while add doc " << inodeId << std::endl << json);
  }
}

void ProjectsElasticSearch::deleteDoc(Int64 inodeId) {
  std::string url = getElasticSearchUrlOnDoc(mIndex, inodeId);
  if(!httpDeleteRequest(url)){
    LOG_FATAL("Failure while deleting doc " << inodeId);
  }
}

void ProjectsElasticSearch::deleteSchemaForINode(Int64 inodeId, std::string
json) {
  std::string url = getElasticSearchUpdateDocUrl(mIndex, inodeId);
  if(!httpPostRequest(url, json)){
    LOG_FATAL("Failure while deleting schema for inode " << inodeId << std::endl
    << json);
  }
}

ProjectsElasticSearch::~ProjectsElasticSearch() {
}

std::string ProjectsElasticSearch::getMetrics() const {
  std::stringstream out;
  out << "up_seconds " << getTimeDiffInSeconds(mStartTime, getCurrentTime())
  << std::endl;
  out << "epipe_elastic_queue_length " << mCurrentQueueSize << std::endl;

  if(mElasticConnetionFailed) {
    out << "epipe_elastic_connection_failed " << mElasticConnetionFailed <<
    std::endl;
    out << "epipe_elastic_connection_failed_since_seconds " <<
    Utils::getTimeDiffInSeconds(mTimeElasticConnectionFailed,
        Utils::getCurrentTime()) << std::endl;
  }
  out << mCounters.getMetrics(mStartTime);
  return out.str();
}

void ProjectsElasticSearch::addDataset(Int64 inodeId, std::string json) {
 addDoc(inodeId, json);
  if(mStats){
    mCounters.datasetAdded();
  }
}

void ProjectsElasticSearch::addProject(Int64 inodeId, std::string json) {
  addDoc(inodeId, json);
  if(mStats){
    mCounters.projectAdded();
  }
}

void ProjectsElasticSearch::removeDataset(Int64 inodeId) {
  deleteDoc(inodeId);
  if(mStats){
    mCounters.datasetRemoved();
  }
}

void ProjectsElasticSearch::removeProject(Int64 inodeId) {
  deleteDoc(inodeId);
  if(mStats){
    mCounters.projectRemoved();
  }
}

