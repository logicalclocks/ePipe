/*
 * This file is part of ePipe
 * Copyright (C) 2020, Logical Clocks AB. All rights reserved
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
#ifndef EPIPE_METRICSMOVINGCOUNTERS_H
#define EPIPE_METRICSMOVINGCOUNTERS_H
#include "TimedRestBatcher.h"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>

namespace bc = boost::accumulators;

typedef bc::accumulator_set<Uint64, bc::stats<bc::tag::mean, bc::tag::count> > Accumulator;

struct MovingCounters {
  MovingCounters(int stepInSeconds, std::string timePrefix, std::string
  pipePrefix) :  mStepSeconds(stepInSeconds), mTimePrefix(timePrefix),
  mPipePrefix(pipePrefix){
    mlastCleared = Utils::getCurrentTime();
    reset();
  }

  virtual void check() final {
    if(mStepSeconds == -1)
      return;

    ptime now = Utils::getCurrentTime();
    if(Utils::getTimeDiffInSeconds(mlastCleared, now) >= mStepSeconds){
      reset();
      mlastCleared = now;
    }
  }

  virtual std::string getMetrics(const ptime startTime) const final {
    std::stringstream out;
    for(auto it=mCounters.begin(); it!=mCounters.end(); ++it){
      out << getVar(it->first) << it->second << std::endl;
    }
    for(auto it=mAccumulators.begin(); it!=mAccumulators.end(); ++it){
      if(bc::count(it->second) > 0) {
        out << getVar(it->first) << bc::mean(it->second) << std::endl;
      }
    }
    return out.str();
  }

  static std::string getVar(std::string pipe, std::string scope, std::string
  var) {
    if(scope.empty()){
      return "epipe_" + pipe + "_" + var + " ";
    }else{
      return "epipe_" + pipe + "_" + var +"{scope=\"" + scope + "\"} ";
    }
  }

  static std::string getVar(std::string pipe, std::string
  var) {
    return getVar(pipe, std::string(), var);
  }

private:
  int mStepSeconds;
  ptime mlastCleared;
  std::string mTimePrefix;
  std::string mPipePrefix;
  boost::unordered_map<std::string, Uint64> mCounters;
  boost::unordered_map<std::string, Accumulator> mAccumulators;

  void reset() {
    mCounters.clear();
    mAccumulators.clear();
  }

  std::string getVar(std::string var) const{
    return getVar(mPipePrefix, mTimePrefix, var);
  }

protected:
  void addToCounter(std::string id, Uint64 value){
    if(mCounters.find(id) == mCounters.end()){
      mCounters[id] = value;
    }else {
      mCounters[id] += value;
    }
  }

  void updateAccumlator(std::string id, Uint64 value){
    if(mAccumulators.find(id) == mAccumulators.end()){
      mAccumulators[id] = Accumulator(value);
    }else{
      mAccumulators[id](value);
    }
  }
};

struct MovingCountersBulk{
  virtual void bulkReceived(const eBulk& bulk) = 0;
  virtual void bulkProcessed(const ptime elastic_start_time, const eBulk&
  bulk) = 0;
  virtual void bulksProcessed(const ptime elastic_start_time, const
  std::vector<eBulk>* bulk) = 0;
};

struct MovingCountersBulkImpl : public MovingCounters, public MovingCountersBulk{

  MovingCountersBulkImpl(int stepInSeconds, std::string timePrefix, std::string
  pipePrefix): MovingCounters(stepInSeconds, timePrefix, pipePrefix){
  }

  void bulkReceived(const eBulk& bulk) override{
    updateAccumlator("avg_batching_time_milliseconds", bulk.getBatchTimeMS());
    updateAccumlator("avg_waiting_time_before_processing_milliseconds",bulk.getBatchTimeMS());
    updateAccumlator("avg_ndb_processing_time_milliseconds",bulk.getProcessingTimeMS());
  }

  void bulkProcessed(const ptime elastic_start_time, const eBulk& bulk)
  override{
    ptime end_time = Utils::getCurrentTime();
    updateAccumlator("avg_elastic_bulk_req_time_milliseconds",
        Utils::getTimeDiffInMilliseconds(elastic_start_time, end_time));
    bulkProcessedInternal(bulk, end_time);
  }

  void bulksProcessed(const ptime elastic_start_time, const
  std::vector<eBulk>* bulks) override{
    ptime end_time = Utils::getCurrentTime();
    updateAccumlator("avg_elastic_bulk_req_time_milliseconds",
        Utils::getTimeDiffInMilliseconds(elastic_start_time, end_time));
    for (auto it = bulks->begin(); it != bulks->end(); ++it) {
      eBulk bulk = *it;
      bulkProcessedInternal(bulk, end_time);
    }
  }

private:
  void bulkProcessedInternal(const eBulk& bulk,
                     const ptime end_time){
    updateAccumlator("avg_elastic_batching_time_milliseconds", bulk
    .geteWaitTimeMS(end_time));
    updateAccumlator("avg_total_time_per_batch_milliseconds", bulk.getTotalTimeMS(end_time));
    for(eEvent event : bulk.mEvents){
      updateAccumlator("avg_total_time_per_event_milliseconds",
          Utils::getTimeDiffInMilliseconds(event.getArrivalTime(), end_time));
    }
    addToCounter("num_processed_batches", 1);
    addToCounter("num_processed_events", bulk.mEvents.size());
    updateCounters(bulk);
  }

  void updateCounters(const eBulk& bulk){
    for(eEvent event : bulk.mEvents){
      if(event.getAssetType() != eEvent::AssetType::AssetNA){
        switch(event.getEventType()){
          case eEvent::EventType::AddEvent:
          case eEvent::EventType::UpdateEvent:
              addToCounter("num_added_" + event.getAssetTypeString(), 1);
          break;
          case eEvent::EventType::DeleteEvent:
              addToCounter("num_deleted_" + event.getAssetTypeString(), 1);
          break;
        }
      }
    }
  }
};

struct MovingCountersSet : public MovingCountersBulk{

  void bulkReceived(const eBulk &bulk) override {
  }
  void
  bulkProcessed(const ptime elastic_start_time, const eBulk &bulk) override {
  }

  void bulksProcessed(const ptime elastic_start_time, const
  std::vector<eBulk> *bulk) override {
  }

  virtual std::string getMetrics() {
    check();
    return std::string();
  };
  virtual std::string getMetrics(Uint32 currentqueue, bool failure,
      ptime failedtime) {
    check();
    return std::string();
  };

protected:
  virtual void check() {};
};

struct MovingCountersBulkSet : public MovingCountersSet{
  MovingCountersBulkSet(std::string pipePrefix) : mStartTime
  (Utils::getCurrentTime()) {
    mCounters = {MovingCountersBulkImpl(60, "last_minute", pipePrefix),
                 MovingCountersBulkImpl
        (3600, "last_hour", pipePrefix), MovingCountersBulkImpl(-1,
            "all_time", pipePrefix)};
    mPipePrefix = pipePrefix;
  }

  void bulkReceived(const eBulk& bulk) override {
    for(auto& c : mCounters){
      c.check();
      c.bulkReceived(bulk);
    }
  }

  void bulkProcessed(const ptime elastic_start_time, const eBulk& bulk)
  override {
    for(auto& c : mCounters){
      c.check();
      c.bulkProcessed(elastic_start_time, bulk);
    }
  }

  void bulksProcessed(const ptime elastic_start_time, const std::vector<eBulk>*
  bulks) override {
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

    float bulksTotalTime = Utils::getTimeDiffInMilliseconds
        (firstEventInCurrentBulksArrivalTime, t_end);
    float bulksEventPerSecond = (numOfEvents * 1000.0) / bulksTotalTime;

    LOG_INFO( mPipePrefix << ":Bulks[" << numOfEvents << "/" << bulks->size()
    << "] took " << bulksTotalTime << " msec at Rate=" << bulksEventPerSecond << " events/second");
  }

  std::string getMetrics() override {
    std::stringstream out;
    out << MovingCountersSet::getMetrics();
    for(auto& c : mCounters){
      out << c.getMetrics(mStartTime);
    }
    return out.str();
  }

  virtual std::string getMetrics(Uint32
  currentqueue, bool failure, ptime failedtime) override {
    std::stringstream out;
    out << MovingCountersSet::getMetrics(currentqueue, failure, failedtime);
    out << getVar("up_seconds") <<  Utils::getTimeDiffInSeconds(mStartTime,
        Utils::getCurrentTime()) << std::endl;
    out << getVar("event_queue_length") << currentqueue << std::endl;
    if(failure){
      out << getVar("elastic_connection_failed_since_seconds") <<
      Utils::getTimeDiffInSeconds(failedtime, Utils::getCurrentTime()) << std::endl;
    } else{
      out << getVar("elastic_connection_failed_since_seconds") <<
          "0" << std::endl;
    }
    out << getMetrics();
    return out.str();
  }

protected:
  const ptime mStartTime;

  void check() override {
    for(auto& c : mCounters){
      c.check();
    }
  }

private:
  std::string mPipePrefix;
  std::vector<MovingCountersBulkImpl> mCounters;

  std::string getVar(std::string var) const{
    return MovingCounters::getVar(mPipePrefix, var);
  }
};
#endif //EPIPE_METRICSMOVINGCOUNTERS_H
