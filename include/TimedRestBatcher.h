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

#ifndef TIMEDRESTBATCHER_H
#define TIMEDRESTBATCHER_H
#include "Batcher.h"
#include "rapidjson/document.h"
#include "Utils.h"
#include "ConcurrentQueue.h"
#include "tables/DBTableBase.h"
#include "http/HttpClient.h"
#include "tables/DBWatchTable.h"

struct eEvent{
  eEvent(const LogHandler* ptr, const ptime arrivalTime, const
  std::string json) : mLogHandler(ptr), mArrivalTime(arrivalTime){
    mJSON = json + "\n";
  }

  const LogHandler* getLogHandler(){
    return mLogHandler;
  };

  const ptime& getArrivalTime(){
    return mArrivalTime;
  }

  const std::string& getJSON(){
    return mJSON;
  }

private:
  const LogHandler* mLogHandler;
  ptime mArrivalTime;
  std::string mJSON;
};

struct eBulk {
  Uint64 mProcessingIndex;
  std::deque<eEvent> mEvents;
  Uint32 mJSONLength;
  ptime mStartProcessing;
  ptime mEndProcessing;

  std::vector<const LogHandler*> mLogHandlers;

  void push(const ptime arrivaltime, const std::string json){
    push(nullptr, arrivaltime, json);
  }

  void push(const LogHandler* lh, const ptime arrivaltime, const
  std::string json){
    eEvent e(lh, arrivaltime, json);
    mEvents.push_back(e);
    mJSONLength += e.getJSON().length();
    mArrivalTimes.push_back(e.getArrivalTime());
    mLogHandlers.push_back(e.getLogHandler());
    if(e.getLogHandler() != nullptr) {
      if (mLogHandlerCounters.find(e.getLogHandler()->getType()) ==
          mLogHandlerCounters.end()) {
        mLogHandlerCounters[e.getLogHandler()->getType()] = 1;
      } else {
        mLogHandlerCounters[e.getLogHandler()->getType()] += 1;
      }
    }
  }

  std::string batchJSON(){
    std::string out;
    for(auto e : mEvents){
      out += e.getJSON();
    }
    out += "\n";
    return out;
  }

  int getCount(LogType type){
    if(mLogHandlerCounters.find(type) ==
       mLogHandlerCounters.end()){
      return 0;
    }
    return mLogHandlerCounters[type];
  }

  void sortArrivalTimes(){
    sort(mArrivalTimes.begin(), mArrivalTimes.end());
  }

  ptime getFirstArrivalTime() const{
    return mArrivalTimes[0];
  }

  ptime getLastArrivalTime() const{
    return mArrivalTimes[mArrivalTimes.size() - 1];
  }

  int getBatchTimeMS() const{
    return Utils::getTimeDiffInMilliseconds(getFirstArrivalTime(),
        getLastArrivalTime());
  }

  int getWaitTimeMS() const{
    return Utils::getTimeDiffInMilliseconds(getLastArrivalTime(),
                                            mStartProcessing);
  }

  int getProcessingTimeMS() const{
    return Utils::getTimeDiffInMilliseconds(mStartProcessing,
                                            mEndProcessing);
  }

  int geteWaitTimeMS(ptime done) const{
    return Utils::getTimeDiffInMilliseconds(mEndProcessing, done);
  }

  int getTotalTimeMS(ptime done) const{
    return Utils::getTimeDiffInMilliseconds(getFirstArrivalTime(), done);
  }

private:
  std::vector<ptime> mArrivalTimes;
  boost::unordered_map<LogType, int> mLogHandlerCounters;

};

class TimedRestBatcher : public Batcher {
public:
  TimedRestBatcher(const HttpClientConfig elastic_client_config, int time_to_wait_before_inserting, int bulk_size);

  void addData(eBulk data);
  
  void shutdown();
  
  virtual ~TimedRestBatcher();

protected:
  bool mElasticConnetionFailed;
  ptime mTimeElasticConnectionFailed;
  Uint32 mCurrentQueueSize;

  bool httpPostRequest(std::string requestUrl, std::string json);
  bool httpDeleteRequest(std::string requestUrl);

  virtual void process(std::vector<eBulk>* data) = 0;

  struct ParsingResponse{
    bool mSuccess;
    bool mRetryable;
  };

  virtual ParsingResponse parseResponse(std::string response) = 0;

private:
  ConcurrentQueue<eBulk> mQueue;
  std::vector<eBulk>* mToProcess;
  int mToProcessLength;
  boost::mutex mLock;
  bool mShutdown;
  HttpClient mHttpClient;
  int mToProcessEvents;

  virtual void run();
  virtual void processBatch();

  enum HttpVerb{
    POST,
    DELETE
  };

  bool handleHttpRequestWithRetry(HttpVerb verb,std::string
  requestUrl, std::string json);

};
#endif //TIMEDRESTBATCHER_H