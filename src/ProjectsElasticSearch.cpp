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

static string getAccString(Accumulator acc) {
  stringstream out;
  out << "[" << bc::min(acc) << "," << bc::mean(acc) << "," << bc::max(acc) << "]";
  return out.str();
}

ProjectsElasticSearch::ProjectsElasticSearch(string elastic_addr, string index,
        int time_to_wait_before_inserting,
        int bulk_size, const bool stats, MConn conn) : ElasticSearchBase(elastic_addr, time_to_wait_before_inserting, bulk_size),
mIndex(index),
mStats(stats), mConn(conn), mTotalNumOfEventsProcessed(0),
mTotalNumOfBulksProcessed(0), mIsFirstEventArrived(false) {
  mElasticBulkAddr = getElasticSearchBulkUrl(mIndex);
}

void ProjectsElasticSearch::process(vector<FSBulk>* bulks) {
  FSKeys keys;
  string batch;
  for (vector<FSBulk>::iterator it = bulks->begin(); it != bulks->end(); ++it) {
    FSBulk bulk = *it;
    batch.append(bulk.mJSON);
    keys.mMetaPKs.insert(bulk.mPKs.mMetaPKs.begin(), bulk.mPKs.mMetaPKs.end());
    keys.mFSPKs.insert(keys.mFSPKs.end(), bulk.mPKs.mFSPKs.begin(), bulk.mPKs.mFSPKs.end());
  }

  //TODO: handle failures
  if (elasticSearchHttpRequest(HTTP_POST, mElasticBulkAddr, batch)) {
    if (!keys.mMetaPKs.empty()) {
      MetadataLogTable().removeLogs(mConn.metadataConnection, keys.mMetaPKs);
    }

    if (!keys.mFSPKs.empty()) {
      FsMutationsLogTable().removeLogs(mConn.inodeConnection, keys.mFSPKs);
    }
  }

  if (mStats) {
    stats(bulks);
  }
}

void ProjectsElasticSearch::stats(vector<FSBulk>* bulks) {
  ptime t_end = getCurrentTime();

  ptime firstEventInCurrentBulksArrivalTime = bulks->at(0).mArrivalTimes.at(0);
  int numOfEvents = 0;
  for (vector<FSBulk>::iterator it = bulks->begin(); it != bulks->end(); ++it) {
    FSBulk bulk = *it;
    stats(bulk, t_end);
    numOfEvents += bulk.mArrivalTimes.size();
  }

  float bulksTotalTime = getTimeDiffInMilliseconds(firstEventInCurrentBulksArrivalTime, t_end);
  float bulksEventPerSecond = (numOfEvents * 1000.0) / bulksTotalTime;

  LOG_INFO("Bulks[" << numOfEvents << "/" << bulks->size() << "] took " << bulksTotalTime << " msec at Rate=" << bulksEventPerSecond << " events/second");

  float totalTime = getTimeDiffInMilliseconds(mFirstEventArrived, t_end);
  float totalEventsPerSecond = (mTotalNumOfEventsProcessed * 1000.0) / totalTime;

  LOG_INFO("Bulks[" << mTotalNumOfEventsProcessed << "/" << mTotalNumOfBulksProcessed << "] took " << totalTime << " msec at Rate=" << totalEventsPerSecond << " events/second" << endl
          << "Total/Bulk=" << getAccString(mTotalTimePerBulkAcc) << ", Total/Event=" << getAccString(mTotalTimePerEventAcc) << endl
          << "Batch=" << getAccString(mBatchingAcc) << ", WaitTime=" << getAccString(mWaitTimeBeforeProcessingAcc) << endl
          << "Processing=" << getAccString(mProcessingAcc) << ", eWaitTime=" << getAccString(mWaitTimeUntillElasticCalledAcc));
}

void ProjectsElasticSearch::stats(FSBulk bulk, ptime t_elastic_done) {

  int size = bulk.mArrivalTimes.size();
  float batch_time, wait_time, processing_time, ewait_time, total_time;
  if (size > 0) {
    if (!mIsFirstEventArrived) {
      mFirstEventArrived = bulk.mArrivalTimes[0];
      mIsFirstEventArrived = true;
    }
    batch_time = getTimeDiffInMilliseconds(bulk.mArrivalTimes[0], bulk.mArrivalTimes[size - 1]);
    wait_time = getTimeDiffInMilliseconds(bulk.mArrivalTimes[size - 1], bulk.mStartProcessing);
    total_time = getTimeDiffInMilliseconds(bulk.mArrivalTimes[0], t_elastic_done);
  }

  processing_time = getTimeDiffInMilliseconds(bulk.mStartProcessing, bulk.mEndProcessing);
  ewait_time = getTimeDiffInMilliseconds(bulk.mEndProcessing, t_elastic_done);

  Accumulator total_time_acc;
  for (int i = 0; i < size; i++) {
    float total_time_per_event = getTimeDiffInMilliseconds(bulk.mArrivalTimes[i], t_elastic_done);
    total_time_acc(total_time_per_event);
    mTotalTimePerEventAcc(total_time_per_event);
  }


  LOG_INFO("Bulk[" << size << "] took " << total_time << " msec, TotalTime/Event=" << getAccString(total_time_acc)
          << ", Batch=" << batch_time << " msec, WaitTime=" << wait_time << " msec, Processing="
          << processing_time << " msec, eWait=" << ewait_time << " msec");

  mBatchingAcc(batch_time);
  mWaitTimeBeforeProcessingAcc(wait_time);
  mProcessingAcc(processing_time);
  mWaitTimeUntillElasticCalledAcc(ewait_time);
  mTotalTimePerBulkAcc(total_time);
  mTotalNumOfEventsProcessed += size;
  mTotalNumOfBulksProcessed++;
}

bool ProjectsElasticSearch::addDoc(int inodeId, string json) {
  string url = getElasticSearchUpdateDocUrl(mIndex, inodeId);
  return elasticSearchHttpRequest(HTTP_POST, url, json);
}

bool ProjectsElasticSearch::addBulk(string json) {
  return elasticSearchHttpRequest(HTTP_POST, mElasticBulkAddr, json);
}

bool ProjectsElasticSearch::deleteDocsByQuery(string json) {
  string deleteProjUrl = getElasticSearchDeleteByQuery(mIndex);
  return elasticSearchHttpRequest(HTTP_POST, deleteProjUrl, json);
}

bool ProjectsElasticSearch::deleteSchemaForINode(int inodeId, string json) {
  string url = getElasticSearchUpdateDocUrl(mIndex, inodeId);
  return elasticSearchHttpRequest(HTTP_POST, url, json);
}

ProjectsElasticSearch::~ProjectsElasticSearch() {
}

