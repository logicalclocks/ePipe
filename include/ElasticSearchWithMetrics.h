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

#ifndef EPIPE_ELASTICSEARCHWITHMETRICS_H
#define EPIPE_ELASTICSEARCHWITHMETRICS_H

#include "ElasticSearchBase.h"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>

namespace bc = boost::accumulators;

typedef bc::accumulator_set<double, bc::stats<bc::tag::mean> > Accumulator;

struct ElasticMovingCounters{
  virtual void check() = 0;
  virtual void bulkReceived(const eBulk& bulk) = 0;
  virtual void bulkProcessed(const ptime elastic_start_time, const eBulk&bulk) = 0;
  virtual void bulksProcessed(const ptime elastic_start_time, const std::vector<eBulk>* bulk) = 0;
  virtual std::string getMetrics(const ptime startTime) const = 0;
  virtual std::string report() = 0;
};

struct ElasticMovingCountersImpl : public ElasticMovingCounters{

  ElasticMovingCountersImpl(int stepInSeconds, std::string prefix) :
  mStepSeconds(stepInSeconds), mPrefix(prefix){
    mlastCleared = Utils::getCurrentTime();
    reset();
  }

  void check() override{
    if(mStepSeconds == -1)
      return;

    ptime now = Utils::getCurrentTime();
    if(Utils::getTimeDiffInSeconds(mlastCleared, now) >= mStepSeconds){
      //LOG_WARN(report());
      reset();
      mlastCleared = now;
    }
  }

  void bulkReceived(const eBulk& bulk) override{
    mBatchingAcc(bulk.getBatchTimeMS());
    mWaitTimeBeforeProcessingAcc(bulk.getWaitTimeMS());
    mProcessingAcc(bulk.getProcessingTimeMS());
  }

  void bulkProcessed(const ptime elastic_start_time, const eBulk& bulk)
  override{
    ptime end_time = Utils::getCurrentTime();
    mElasticBulkTimeAcc(Utils::getTimeDiffInMilliseconds(elastic_start_time, end_time));
    bulkProcessed(bulk, end_time);
  }

  void bulksProcessed(const ptime elastic_start_time, const
  std::vector<eBulk>* bulks) override{
    ptime end_time = Utils::getCurrentTime();
    mElasticBulkTimeAcc(Utils::getTimeDiffInMilliseconds(elastic_start_time, end_time));
    for (auto it = bulks->begin(); it != bulks->end(); ++it) {
      eBulk bulk = *it;
      bulkProcessed(bulk, end_time);
    }
  }

  std::string report() override {
    std::stringstream out;
    out << mTotalNumOfEventsProcessed << ",";
    out << mTotalNumOfBulksProcessed << ",";
    out << bc::mean(mTotalTimePerEventAcc) << ",";
    out << bc::mean(mTotalTimePerBulkAcc) << ",";
    out << bc::mean(mBatchingAcc) << ",";
    out << bc::mean(mProcessingAcc) << ",";
    out << bc::mean(mElasticBulkTimeAcc);
    return out.str();
  }

  std::string getMetrics(const ptime startTime) const override{
    std::stringstream out;
    out << "epipe_relative_start_time_seconds{scope=\"" << mPrefix << "\"} " <<
        ( mStepSeconds == -1 || mTotalNumOfEventsProcessed == 0 ? 0 :
          Utils::getTimeDiffInSeconds(startTime,mlastCleared))<< std::endl;
    out << "epipe_num_processed_events{scope=\"" << mPrefix << "\"} " <<
        mTotalNumOfEventsProcessed << std::endl;
    out << "epipe_num_processed_batches{scope=\"" << mPrefix << "\"} " <<
        mTotalNumOfBulksProcessed << std::endl;
    if(mTotalNumOfEventsProcessed > 0 ) {
      out << "epipe_avg_total_time_per_event_milliseconds{scope=\"" <<
          mPrefix << "\"} " << bc::mean(mTotalTimePerEventAcc) << std::endl;
      out << "epipe_avg_total_time_per_batch_milliseconds{scope=\"" <<
          mPrefix << "\"} " << bc::mean(mTotalTimePerBulkAcc) << std::endl;
      out << "epipe_avg_batching_time_milliseconds{scope=\"" << mPrefix <<
          "\"} " << bc::mean(mBatchingAcc) << std::endl;
      out << "epipe_avg_ndb_processing_time_milliseconds{scope=\"" << mPrefix <<
          "\"} " << bc::mean(mProcessingAcc) << std::endl;
      out << "epipe_avg_elastic_batching_time_milliseconds{scope=\"" <<
          mPrefix << "\"} " << bc::mean(mWaitTimeUntillElasticCalledAcc) <<
          std::endl;
      out << "epipe_avg_elastic_bulk_req_time_milliseconds{scope=\"" <<
          mPrefix << "\"} " << bc::mean(mElasticBulkTimeAcc) <<
          std::endl;
    }else{
      out << "epipe_avg_total_time_per_event_milliseconds{scope=\"" <<
          mPrefix << "\"} 0" << std::endl;
      out << "epipe_avg_total_time_per_batch_milliseconds{scope=\"" <<
          mPrefix << "\"} 0" << std::endl;
      out << "epipe_avg_batching_time_milliseconds{scope=\"" << mPrefix <<
          "\"} 0" << std::endl;
      out << "epipe_avg_ndb_processing_time_milliseconds{scope=\"" << mPrefix <<
          "\"} 0" << std::endl;
      out << "epipe_avg_elastic_batching_time_milliseconds{scope=\"" <<
          mPrefix << "\"} 0" << std::endl;
      out << "epipe_avg_elastic_bulk_req_time_milliseconds{scope=\"" <<
          mPrefix << "\"} 0"  << std::endl;
    }
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
  Accumulator mElasticBulkTimeAcc;
  Int64 mTotalNumOfEventsProcessed;
  Int64 mTotalNumOfBulksProcessed;

  void reset(){
    mBatchingAcc = {};
    mWaitTimeBeforeProcessingAcc = {};
    mProcessingAcc={};
    mWaitTimeUntillElasticCalledAcc={};
    mTotalTimePerEventAcc={};
    mTotalTimePerBulkAcc={};
    mTotalNumOfEventsProcessed = 0;
    mTotalNumOfBulksProcessed = 0;
  }

  void bulkProcessed(const eBulk& bulk, const ptime end_time){
    mWaitTimeUntillElasticCalledAcc(bulk.geteWaitTimeMS(end_time));
    mTotalTimePerBulkAcc(bulk.getTotalTimeMS(end_time));
    for(eEvent event : bulk.mEvents){
      mTotalTimePerEventAcc(Utils::getTimeDiffInMilliseconds(event.getArrivalTime(), end_time));
    }
    mTotalNumOfBulksProcessed++;
    mTotalNumOfEventsProcessed += bulk.mEvents.size();
  }
};

struct ElasticMovingCountersSet : public ElasticMovingCounters{
  ElasticMovingCountersSet(){
    mCounters = {ElasticMovingCountersImpl(1, "last_second"),
                 ElasticMovingCountersImpl(60, "last_minute"),
                 ElasticMovingCountersImpl(3600, "last_hour"),
                 ElasticMovingCountersImpl(-1, "all_time")};
  }

  void check() override {
    for(auto& c : mCounters){
      c.check();
    }
  }

  void bulkReceived(const eBulk& bulk) override {
    for(auto& c : mCounters){
      c.check();
      c.bulkReceived(bulk);
    }
  }

  void bulkProcessed(const ptime elastic_start_time, const eBulk& bulk) override {
    for(auto& c : mCounters){
      c.check();
      c.bulkProcessed(elastic_start_time, bulk);
    }
  }

  void bulksProcessed(const ptime elastic_start_time, const std::vector<eBulk>* bulks) override {
    for(auto& c : mCounters){
      c.check();
      c.bulksProcessed(elastic_start_time, bulks);
    }

    ptime t_end = Utils::getCurrentTime();

    ptime firstEventInCurrentBulksArrivalTime = bulks->at(0).getFirstArrivalTime();
    int numOfEvents = 0;
    for (auto it = bulks->begin(); it != bulks->end(); ++it) {
      eBulk bulk = *it;
      numOfEvents += bulk.mEvents.size();
    }

    float bulksTotalTime = Utils::getTimeDiffInMilliseconds(firstEventInCurrentBulksArrivalTime, t_end);
    float bulksEventPerSecond = (numOfEvents * 1000.0) / bulksTotalTime;

//    LOG_INFO("Bulks[" << numOfEvents << "/" << bulks->size() << "] took " << bulksTotalTime << " msec at Rate=" << bulksEventPerSecond << " events/second");
  }

  std::string report() override {
    return mCounters[0].report();
  }

  std::string getMetrics(const ptime startTime) const override {
    std::stringstream out;
    for(auto& c : mCounters){
      out << c.getMetrics(startTime);
    }
    return out.str();
  }

private:
  std::vector<ElasticMovingCountersImpl> mCounters;

};

class ElasticSearchWithMetrics : public ElasticSearchBase {
public:
  ElasticSearchWithMetrics(std::string elastic_addr, int time_to_wait_before_inserting, int bulk_size, const bool stats);

  virtual ~ElasticSearchWithMetrics();

  std::string getMetrics() const override;

protected:
  const bool mStats;
  const ptime mStartTime;
  ElasticMovingCountersSet mCounters;
};
#endif //EPIPE_ELASTICSEARCHWITHMETRICS_H
