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
  enum EventType{
    EventNA = 0,
    AddEvent = 1,
    UpdateEvent = 2,
    DeleteEvent = 3
  };
  enum AssetType{
    AssetNA = 0,
    INode = 1,
    Dataset = 2,
    Project = 3
  };

  eEvent(const LogHandler* ptr, const ptime arrivalTime, const
  std::string json, const EventType eventType, const AssetType assetType) 
  : mLogHandler(ptr), mArrivalTime(arrivalTime), mEventType(eventType), mAssetType(assetType){
    mJSON = json + "\n";
  }

 eEvent(const LogHandler* ptr, const ptime arrivalTime, const
  std::string json) : eEvent(ptr, arrivalTime, json, EventType::EventNA, AssetType::AssetNA){
 
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

  EventType getEventType(){
    return mEventType;
  }

  AssetType getAssetType(){
    return mAssetType;
  }

  std::string getAssetTypeString(){
    switch(mAssetType){
      case AssetType::INode:
        return "inodes";
      case AssetType::Dataset:
        return "datasets";
      case AssetType::Project:
        return "projects";
    }
    return "";
  }
  
private:
  const LogHandler* mLogHandler;
  ptime mArrivalTime;
  std::string mJSON;
  EventType mEventType;
  AssetType mAssetType;
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

  void push(const LogHandler* lh, const ptime arrivaltime, const std::string json){
    push(lh, arrivaltime, json, eEvent::EventType::EventNA, eEvent::AssetType::AssetNA);
  }

  void push(const LogHandler* lh, const ptime arrivaltime, const
  std::string json, const eEvent::EventType eventType, const eEvent::AssetType assetType){
    eEvent e(lh, arrivaltime, json, eventType, assetType);
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

  std::string toString() const{
    std::stringstream out;
    out << "Bulk[" << mProcessingIndex << "] " << std::endl 
        << mEvents.size() << " events" << std::endl;
    for(auto event : mEvents){
      std::string _logdesc = "N/A";
      if(event.getLogHandler() != nullptr){
        _logdesc = event.getLogHandler()->getDescription();
      }
      out << "LogHandler: " <<  _logdesc << std::endl << "EventJSON: " << event.getJSON();
    }
    return out.str();
  }

private:
  std::vector<ptime> mArrivalTimes;
  boost::unordered_map<LogType, int> mLogHandlerCounters;

};

struct ParsingResponse{
  bool mSuccess;
  bool mRetryable;
  std::string errorMsg;
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

  ParsingResponse httpPostRequest(std::string requestUrl, std::string json);
  ParsingResponse httpDeleteRequest(std::string requestUrl);

  virtual void process(std::vector<eBulk>* data) = 0;

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

  ParsingResponse handleHttpRequestWithRetry(HttpVerb verb, std::string requestUrl, std::string json);

};
#endif //TIMEDRESTBATCHER_H