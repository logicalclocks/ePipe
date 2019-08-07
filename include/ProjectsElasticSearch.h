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
 * File:   ProjectsElasticSearch.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 * Created on June 9, 2016, 1:39 PM
 */

#ifndef PROJECTSELASTICSEARCH_H
#define PROJECTSELASTICSEARCH_H

#include "ElasticSearchBase.h"
#include "FsMutationsTableTailer.h"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>

namespace bc = boost::accumulators;

typedef bc::accumulator_set<double, bc::stats<bc::tag::mean> > Accumulator;

struct FSKeys {
  UISet mMetaPKs;
  FPK mFSPKs;
};

typedef Bulk<FSKeys> FSBulk;

struct MovingCounters{
  virtual void check() = 0;
  virtual void addTotalTimePerEvent(float total_time_per_event) = 0;
  virtual void processBulk(float batch_time, float wait_time, float processing_time,
      float ewait_time, float total_time, int size) = 0;
  virtual void datasetAdded() = 0;
  virtual void projectAdded() = 0;
  virtual void datasetRemoved() = 0;
  virtual void projectRemoved() = 0;
  virtual void elasticSearchRequestFailed() = 0;
  virtual std::string getMetrics(const ptime startTime) const = 0;
};

struct MovingCountersImpl : public MovingCounters{

  MovingCountersImpl(int stepInSeconds, std::string prefix) : mStepSeconds
  (stepInSeconds), mPrefix(prefix){
    mlastCleared = getCurrentTime();
    reset();
  }

  void check() override{
    if(mStepSeconds == -1)
      return;

    ptime now = getCurrentTime();
    if(getTimeDiffInSeconds(mlastCleared, now) >= mStepSeconds){
      reset();
      mlastCleared = now;
    }
  }

  void addTotalTimePerEvent(float total_time_per_event) override{
    mTotalTimePerEventAcc(total_time_per_event);
  }
  void processBulk(float batch_time, float wait_time, float processing_time,
      float ewait_time, float total_time, int size) override{
    mBatchingAcc(batch_time);
    mWaitTimeBeforeProcessingAcc(wait_time);
    mProcessingAcc(processing_time);
    mWaitTimeUntillElasticCalledAcc(ewait_time);
    mTotalTimePerBulkAcc(total_time);
    mTotalNumOfEventsProcessed += size;
    mTotalNumOfBulksProcessed++;
  }

  void datasetAdded() override{
    mCreatedDatasets++;
  }

  void projectAdded() override{
    mCreatedProjects++;
  }

  void datasetRemoved() override{
    mDeletedDatasets++;
  }

  void projectRemoved() override{
    mDeletedProjects++;
  }

  void elasticSearchRequestFailed() override {
    mElasticSearchFailedRequests++;
  }

  std::string getMetrics(const ptime startTime) const override{
    std::stringstream out;
    out << "relative_start_time_seconds{scope=\"" << mPrefix << "\"} " <<
       ( mStepSeconds == -1 || mTotalNumOfEventsProcessed == 0 ? 0 :
       getTimeDiffInSeconds(startTime,mlastCleared))<< std::endl;
    out << "num_processed_events{scope=\"" << mPrefix << "\"} " <<
    mTotalNumOfEventsProcessed << std::endl;
    out << "num_processed_batches{scope=\"" << mPrefix << "\"} " <<
    mTotalNumOfBulksProcessed << std::endl;
    if(mTotalNumOfEventsProcessed > 0 ) {
      out << "avg_total_time_per_event_milliseconds{scope=\"" << mPrefix <<
      "\"} " << bc::mean(mTotalTimePerEventAcc) << std::endl;
      out << "avg_total_time_per_batch_milliseconds{scope=\"" << mPrefix <<
      "\"} " << bc::mean(mTotalTimePerEventAcc) << std::endl;
      out << "avg_batching_time_milliseconds{scope=\"" << mPrefix << "\"} " <<
          bc::mean(mBatchingAcc) << std::endl;
      out << "avg_ndb_processing_time_milliseconds{scope=\"" << mPrefix <<
      "\"} " << bc::mean(mProcessingAcc) << std::endl;
      out << "avg_elastic_batching_time_milliseconds{scope=\"" << mPrefix <<
      "\"} " << bc::mean(mWaitTimeUntillElasticCalledAcc) << std::endl;
    }else{
      out << "avg_total_time_per_event_milliseconds{scope=\"" << mPrefix <<
      "\"} 0" << std::endl;
      out << "avg_total_time_per_batch_milliseconds{scope=\"" << mPrefix <<
      "\"} 0" << std::endl;
      out << "avg_batching_time_milliseconds{scope=\"" << mPrefix << "\"} 0"
      << std::endl;
      out << "avg_ndb_processing_time_milliseconds{scope=\"" << mPrefix <<
      "\"} 0" << std::endl;
      out << "avg_elastic_batching_time_milliseconds{scope=\"" << mPrefix <<
      "\"} 0" << std::endl;
    }

    out << "num_added_datasets{scope=\"" << mPrefix << "\"} " <<
    mCreatedDatasets << std::endl;
    out << "num_deleted_datasets{scope=\"" << mPrefix << "\"} " <<
    mDeletedDatasets << std::endl;
    out << "num_added_projects{scope=\"" << mPrefix << "\"} " <<
    mCreatedProjects << std::endl;
    out << "num_deleted_projects{scope=\"" << mPrefix << "\"} " <<
    mDeletedProjects << std::endl;
    out << "num_failed_elasticsearch_batch_requests{scope=\"" << mPrefix <<
    "\"} " << mElasticSearchFailedRequests << std::endl;
   return out.str();
  }

private:
  int mStepSeconds;
  std::string mPrefix;

  ptime mlastCleared;
  Accumulator mBatchingAcc;
  Accumulator mWaitTimeBeforeProcessingAcc;
  Accumulator mProcessingAcc;
  Accumulator mWaitTimeUntillElasticCalledAcc;
  Accumulator mTotalTimePerEventAcc;
  Accumulator mTotalTimePerBulkAcc;
  Int64 mTotalNumOfEventsProcessed;
  Int64 mTotalNumOfBulksProcessed;
  Int64 mCreatedDatasets;
  Int64 mCreatedProjects;
  Int64 mDeletedDatasets;
  Int64 mDeletedProjects;
  Int64 mElasticSearchFailedRequests;

  void reset(){
    mBatchingAcc = {};
    mWaitTimeBeforeProcessingAcc = {};
    mProcessingAcc={};
    mWaitTimeUntillElasticCalledAcc={};
    mTotalTimePerEventAcc={};
    mTotalTimePerBulkAcc={};
    mTotalNumOfEventsProcessed = 0;
    mTotalNumOfBulksProcessed = 0;
    mCreatedDatasets = 0;
    mCreatedProjects = 0;
    mDeletedDatasets = 0;
    mDeletedProjects = 0;
    mElasticSearchFailedRequests = 0;
  }
};

struct MovingCountersSet : public MovingCounters{
  MovingCountersSet(){
    mCounters = {MovingCountersImpl(60, "last_minute"), MovingCountersImpl
                 (3600, "last_hour"), MovingCountersImpl(-1, "all_time")};
  }
  void check() override {
    for(auto& c : mCounters){
      c.check();
    }
  }

  void addTotalTimePerEvent(float total_time_per_event) override {
    for(auto& c : mCounters){
      c.addTotalTimePerEvent(total_time_per_event);
    }
  }

  void processBulk(float batch_time, float wait_time, float processing_time,
                   float ewait_time, float total_time, int size) override {
    for(auto& c : mCounters){
      c.processBulk(batch_time, wait_time, processing_time, ewait_time,
          total_time, size);
    }
  }

  void datasetAdded() override {
    for(auto& c : mCounters){
      c.datasetAdded();
    }
  }

  void projectAdded() override {
    for(auto& c : mCounters){
      c.projectAdded();
    }
  }

  void datasetRemoved() override {
    for(auto& c : mCounters){
      c.datasetRemoved();
    }
  }

  void projectRemoved() override {
    for(auto& c : mCounters){
      c.projectRemoved();
    }
  }

  void elasticSearchRequestFailed() override {
    for(auto& c : mCounters){
      c.elasticSearchRequestFailed();
    }
  }

  std::string getMetrics(const ptime startTime) const override {
    std::stringstream out;
    for(auto& c : mCounters){
      out << c.getMetrics(startTime);
    }
    return out.str();
  }

private:
  std::vector<MovingCountersImpl> mCounters;
};

class ProjectsElasticSearch : public ElasticSearchBase<FSKeys> {
public:
  ProjectsElasticSearch(std::string elastic_addr, std::string index,
          int time_to_wait_before_inserting, int bulk_size,
          const bool stats, MConn conn);

  bool addDataset(Int64 inodeId, std::string json);
  bool addProject(Int64 inodeId, std::string json);
  bool removeDataset(std::string json);
  bool removeProject(std::string json);

  bool addDoc(Int64 inodeId, std::string json);
  bool addBulk(std::string json);
  bool deleteDocsByQuery(std::string json);
  bool deleteSchemaForINode(Int64 inodeId, std::string json);

  std::string getMetrics() const override;

  virtual ~ProjectsElasticSearch();
private:
  const std::string mIndex;
  const bool mStats;
  std::string mElasticBulkAddr;
  MConn mConn;
  const ptime mStartTime;

  MovingCountersSet mCounters;

  virtual void process(std::vector<FSBulk>* bulks);

  void stats(std::vector<FSBulk>* bulks);
  void stats(FSBulk bulk, ptime t_elastic_done);
};

#endif /* PROJECTSELASTICSEARCH_H */

