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

#include "TimedRestBatcher.h"

TimedRestBatcher::TimedRestBatcher(std::string endpoint_addr, int time_to_wait_before_inserting, int bulk_size)
    : Batcher(time_to_wait_before_inserting, bulk_size), mToProcessLength(0), mHttpClient(endpoint_addr){
  mToProcess = new std::vector<eBulk>();
  mShutdown = false;
  mElasticConnetionFailed = false;
  mCurrentQueueSize = 0;
  mToProcessEvents = 0;
}

void TimedRestBatcher::addData(eBulk data) {
  LOG_DEBUG("Add Bulk JSON:" << std::endl << data.batchJSON() << std::endl);
  mQueue.push(data);
  mCurrentQueueSize += data.mEvents.size();
}

void TimedRestBatcher::shutdown(){
  LOG_INFO("Shutting down timed rest batcher...");
  mShutdown = true;
}

void TimedRestBatcher::run() {
  while (true) {
    if(mShutdown && mQueue.empty()){
      LOG_INFO("Shutdown timed rest batcher.");
      Batcher::shutdown();
      break;
    }
    eBulk msg;
    mQueue.wait_and_pop(msg);

    mLock.lock();
    mToProcess->push_back(msg);
    mToProcessLength += msg.mJSONLength;
    mToProcessEvents += msg.mEvents.size();
    mLock.unlock();

    if (mToProcessLength >= mBatchSize && !mTimerProcessing) {
      processBatch();
    }
  }
}

void TimedRestBatcher::processBatch() {
  if (mToProcessLength > 0) {
    LOG_DEBUG("Process Bulk JSONs [" << mToProcessLength << "]");

    mLock.lock();
    std::vector<eBulk >* data = mToProcess;
    mToProcess = new std::vector<eBulk >;
    mToProcessLength = 0;
    mCurrentQueueSize -= mToProcessEvents;
    mToProcessEvents = 0;
    mLock.unlock();

    process(data);

    delete data;
  }

  if(mShutdown){
    LOG_INFO("Shutting down, remaining " << mQueue.size() << " bulks to process");
    if(mQueue.empty()){
      mQueue.push(eBulk());
      Batcher::shutdown();
    }
  }
}

bool TimedRestBatcher::httpPostRequest(std::string requestUrl, std::string json) {
  ptime t1 = Utils::getCurrentTime();
  std::string res = handleHttpRequestWithRetry(HttpVerb::POST,requestUrl, json);

  bool success = parseResponse(res);
  ptime t2 = Utils::getCurrentTime();
  LOG_INFO("POST " << requestUrl << " [" << json.length() << "]  took " <<
                   Utils::getTimeDiffInMilliseconds(t1, t2) << " msec");
  return success;
}

bool TimedRestBatcher::httpDeleteRequest(std::string requestUrl) {
  ptime t1 = Utils::getCurrentTime();
  std::string res = handleHttpRequestWithRetry(HttpVerb::DELETE,requestUrl, "");

  bool success = parseResponse(res);
  ptime t2 = Utils::getCurrentTime();
  LOG_INFO("DELETE " << requestUrl << " took " <<
                     Utils::getTimeDiffInMilliseconds(t1, t2) << " msec");
  return success;
}

std::string TimedRestBatcher::handleHttpRequestWithRetry(HttpVerb verb,
                                                         std::string requestUrl, std::string json){
  do {
    HttpResponse res;
    if(verb == HttpVerb::POST) {
      res = mHttpClient.post(requestUrl, json);
    }else if (verb == HttpVerb::DELETE){
      res = mHttpClient.delete_(requestUrl);
    }
    if(res.mSuccess){
      mElasticConnetionFailed = false;
      return res.mResponse;
    }

    if(!mElasticConnetionFailed){
      mTimeElasticConnectionFailed = Utils::getCurrentTime();
    }
    mElasticConnetionFailed = true;
    LOG_ERROR("Failed to connect to elastic, " << Utils::getTimeDiffInSeconds
        (mTimeElasticConnectionFailed, Utils::getCurrentTime())
                                               << " seconds have passed since first failure");
    boost::this_thread::sleep(boost::posix_time::milliseconds(mTimeToWait));

  } while(mElasticConnetionFailed);
  return "";
}

TimedRestBatcher::~TimedRestBatcher() {

}