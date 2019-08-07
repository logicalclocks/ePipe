/*
 * Copyright (C) 2016 Hops.io
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

/* 
 * File:   ElasticSearch.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "ProjectsElasticSearch.h"
#include "MetadataLogTailer.h"

using namespace Utils;

ProjectsElasticSearch::ProjectsElasticSearch(std::string elastic_addr, std::string index,
        int time_to_wait_before_inserting,
        int bulk_size, const bool stats, MConn conn) : ElasticSearchBase(elastic_addr, time_to_wait_before_inserting, bulk_size),
mIndex(index),
mStats(stats), mConn(conn), mStartTime(getCurrentTime()){
  mElasticBulkAddr = getElasticSearchBulkUrl(mIndex);
}

void ProjectsElasticSearch::process(std::vector<FSBulk>* bulks) {
  FSKeys keys;
  std::string batch;
  for (std::vector<FSBulk>::iterator it = bulks->begin(); it != bulks->end(); ++it) {
    FSBulk bulk = *it;
    batch.append(bulk.mJSON);
    keys.mMetaPKs.insert(bulk.mPKs.mMetaPKs.begin(), bulk.mPKs.mMetaPKs.end());
    keys.mFSPKs.insert(keys.mFSPKs.end(), bulk.mPKs.mFSPKs.begin(), bulk.mPKs.mFSPKs.end());
  }

  //TODO: handle failures
  if (httpRequest(HTTP_POST, mElasticBulkAddr, batch)) {
    if (!keys.mMetaPKs.empty()) {
      MetadataLogTable().removeLogs(mConn.metadataConnection, keys.mMetaPKs);
    }

    if (!keys.mFSPKs.empty()) {
      FsMutationsLogTable().removeLogs(mConn.inodeConnection, keys.mFSPKs);
    }
    if (mStats) {
      stats(bulks);
    }
  }else{
    if(mStats){
      mCounters.check();
      mCounters.elasticSearchRequestFailed();
    }
  }
}

void ProjectsElasticSearch::stats(std::vector<FSBulk>* bulks) {
  ptime t_end = getCurrentTime();

  ptime firstEventInCurrentBulksArrivalTime = bulks->at(0).mArrivalTimes.at(0);
  int numOfEvents = 0;
  for (std::vector<FSBulk>::iterator it = bulks->begin(); it != bulks->end(); ++it) {
    FSBulk bulk = *it;
    stats(bulk, t_end);
    numOfEvents += bulk.mArrivalTimes.size();
  }

  float bulksTotalTime = getTimeDiffInMilliseconds(firstEventInCurrentBulksArrivalTime, t_end);
  float bulksEventPerSecond = (numOfEvents * 1000.0) / bulksTotalTime;

  LOG_INFO("Bulks[" << numOfEvents << "/" << bulks->size() << "] took " << bulksTotalTime << " msec at Rate=" << bulksEventPerSecond << " events/second");

}

void ProjectsElasticSearch::stats(FSBulk bulk, ptime t_elastic_done) {
  mCounters.check();

  int size = bulk.mArrivalTimes.size();
  float batch_time, wait_time, processing_time, ewait_time, total_time;
  if (size > 0) {
    batch_time = getTimeDiffInMilliseconds(bulk.mArrivalTimes[0], bulk.mArrivalTimes[size - 1]);
    wait_time = getTimeDiffInMilliseconds(bulk.mArrivalTimes[size - 1], bulk.mStartProcessing);
    total_time = getTimeDiffInMilliseconds(bulk.mArrivalTimes[0], t_elastic_done);
  }

  processing_time = getTimeDiffInMilliseconds(bulk.mStartProcessing, bulk.mEndProcessing);
  ewait_time = getTimeDiffInMilliseconds(bulk.mEndProcessing, t_elastic_done);

  for (int i = 0; i < size; i++) {
    float total_time_per_event = getTimeDiffInMilliseconds(bulk.mArrivalTimes[i], t_elastic_done);
    mCounters.addTotalTimePerEvent(total_time_per_event);
  }

  mCounters.processBulk(batch_time, wait_time, processing_time,
      ewait_time, total_time, size);
}

bool ProjectsElasticSearch::addDoc(Int64 inodeId, std::string json) {
  std::string url = getElasticSearchUpdateDocUrl(mIndex, inodeId);
  return httpRequest(HTTP_POST, url, json);
}

bool ProjectsElasticSearch::addBulk(std::string json) {
  return httpRequest(HTTP_POST, mElasticBulkAddr, json);
}

bool ProjectsElasticSearch::deleteDocsByQuery(std::string json) {
  std::string deleteProjUrl = getElasticSearchDeleteByQuery(mIndex);
  return httpRequest(HTTP_POST, deleteProjUrl, json);
}

bool ProjectsElasticSearch::deleteSchemaForINode(Int64 inodeId, std::string json) {
  std::string url = getElasticSearchUpdateDocUrl(mIndex, inodeId);
  return httpRequest(HTTP_POST, url, json);
}

ProjectsElasticSearch::~ProjectsElasticSearch() {
}

std::string ProjectsElasticSearch::getMetrics() const {
  std::stringstream out;
  out << "up_seconds " << getTimeDiffInSeconds(mStartTime, getCurrentTime())
  << std::endl;
  out << mCounters.getMetrics(mStartTime);
  return out.str();
}

bool ProjectsElasticSearch::addDataset(Int64 inodeId, std::string json) {
  bool res = addDoc(inodeId, json);
  if(mStats){
    mCounters.check();
  }
  if(res){
    mCounters.datasetAdded();
  }else{
    mCounters.elasticSearchRequestFailed();
  }

  return res;
}

bool ProjectsElasticSearch::addProject(Int64 inodeId, std::string json) {
  bool res = addDoc(inodeId, json);
  if(mStats){
    mCounters.check();
  }
  if(res){
    mCounters.projectAdded();
  }else{
    mCounters.elasticSearchRequestFailed();
  }
  return res;
}

bool ProjectsElasticSearch::removeDataset(std::string json) {
  bool res = deleteDocsByQuery(json);
  if(mStats){
    mCounters.check();
  }
  if(res){
    mCounters.datasetRemoved();
  }else{
    mCounters.elasticSearchRequestFailed();
  }
  return res;
}

bool ProjectsElasticSearch::removeProject(std::string json) {
  bool res = deleteDocsByQuery(json);
  if(mStats){
    mCounters.check();
  }
  if(res){
    mCounters.projectRemoved();
  }else{
    mCounters.elasticSearchRequestFailed();
  }
  return res;
}

