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

#ifndef TIMEDRESTBATCHER_H
#define TIMEDRESTBATCHER_H
#include "Batcher.h"
#include "rapidjson/document.h"
#include "Utils.h"
#include "ConcurrentQueue.h"
#include "tables/DBTableBase.h"
#include "http/HttpClient.h"

using namespace Utils;

template<typename Keys>
struct Bulk {
  Uint64 mProcessingIndex;
  std::string mJSON;
  std::vector<ptime> mArrivalTimes;
  ptime mStartProcessing;
  ptime mEndProcessing;
  Keys mPKs;
};

template<typename Keys>
class TimedRestBatcher : public Batcher {
public:
  TimedRestBatcher(std::string endpoint_addr, int time_to_wait_before_inserting, int bulk_size);

  void addData(Bulk<Keys> data);
  
  void shutdown();
  
  virtual ~TimedRestBatcher();

protected:

  bool httpPostRequest(std::string requestUrl, std::string json);
  bool httpDeleteRequest(std::string requestUrl);

  virtual void process(std::vector<Bulk<Keys> >* data) = 0;
  virtual bool parseResponse(std::string response) = 0;

private:
  ConcurrentQueue<Bulk<Keys> > mQueue;
  std::vector<Bulk<Keys> >* mToProcess;
  int mToProcessLength;
  boost::mutex mLock;
  bool mShutdown;
  HttpClient mHttpClient;

  virtual void run();
  virtual void processBatch();
};

template<typename Keys>
TimedRestBatcher<Keys>::TimedRestBatcher(std::string endpoint_addr, int time_to_wait_before_inserting, int bulk_size)
: Batcher(time_to_wait_before_inserting, bulk_size), mToProcessLength(0), mHttpClient(endpoint_addr){
  mToProcess = new std::vector<Bulk<Keys> >();
  mShutdown = false;
}

template<typename Keys>
void TimedRestBatcher<Keys>::addData(Bulk<Keys> data) {
  LOG_DEBUG("Add Bulk JSON:" << std::endl << data.mJSON << std::endl);
  mQueue.push(data);
}

template<typename Keys>
void TimedRestBatcher<Keys>::shutdown(){
  LOG_INFO("Shutting down timed rest batcher...");
  mShutdown = true;
}

template<typename Keys>
void TimedRestBatcher<Keys>::run() {
  while (true) {
    if(mShutdown && mQueue.empty()){
      LOG_INFO("Shutdown timed rest batcher.");
      Batcher::shutdown();
      break;
    }
    Bulk<Keys> msg;
    mQueue.wait_and_pop(msg);
    
    mLock.lock();
    mToProcess->push_back(msg);
    mToProcessLength += msg.mJSON.length();
    mLock.unlock();

    if (mToProcessLength >= mBatchSize && !mTimerProcessing) {
      processBatch();
    }
  }
}

template<typename Keys>
void TimedRestBatcher<Keys>::processBatch() {
  if (mToProcessLength > 0) {
    LOG_DEBUG("Process Bulk JSONs [" << mToProcessLength << "]");

    mLock.lock();
    std::vector<Bulk<Keys> >* data = mToProcess;
    mToProcess = new std::vector<Bulk<Keys> >;
    mToProcessLength = 0;
    mLock.unlock();

    process(data);

    delete data;
  }
  
  if(mShutdown){
    LOG_INFO("Shutting down, remaining " << mQueue.size() << " bulks to process");
    if(mQueue.empty()){
      mQueue.push(Bulk<Keys>());
      Batcher::shutdown();
    }
  }
}

template<typename Keys>
bool TimedRestBatcher<Keys>::httpPostRequest(std::string requestUrl, std::string json) {
  ptime t1 = Utils::getCurrentTime();
  HttpResponse res = mHttpClient.post(requestUrl, json);

  if(!res.mSuccess){
    //TODO: handle different failure scenarios
    return false;
  }

  bool success = parseResponse(res.mResponse);
  ptime t2 = Utils::getCurrentTime();
  LOG_INFO("POST " << requestUrl << " [" << json.length() << "]  took " <<
  Utils::getTimeDiffInMilliseconds(t1, t2) << " msec");
  return success;
}

template<typename Keys>
bool TimedRestBatcher<Keys>::httpDeleteRequest(std::string requestUrl) {
  ptime t1 = Utils::getCurrentTime();
  HttpResponse res = mHttpClient.delete_(requestUrl);

  if (!res.mSuccess) {
    //TODO: handle different failure scenarios
    return false;
  }

  bool success = parseResponse(res.mResponse);
  ptime t2 = Utils::getCurrentTime();
  LOG_INFO("DELETE " << requestUrl << " took " <<
                     Utils::getTimeDiffInMilliseconds(t1, t2) << " msec");
  return success;
}

template<typename Keys>
TimedRestBatcher<Keys>::~TimedRestBatcher() {

}

#endif //TIMEDRESTBATCHER_H