/*
 * Copyright (C) 2018 Logical Clocks AB
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
 * File:   TimedRestBatcher.h
 * Author: Alexandru Ormenisan<aaor@kth.se>
 *
 */

#ifndef TIMEDRESTBATCHER_H
#define TIMEDRESTBATCHER_H
#include "Batcher.h"
#include <curl/curl.h>
#include "rapidjson/document.h"
#include "Utils.h"
#include "ConcurrentQueue.h"
#include "tables/DBTableBase.h"

using namespace Utils;

enum HttpOp {
  HTTP_POST,
  HTTP_DELETE
};

struct Response {
  std::string mResponse;
  CURLcode mCode;
};

static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
  ((std::string*)userp)->append((char*) contents, size * nmemb);
  return size * nmemb;
}

static const char* getStr(HttpOp op) {
  switch (op) {
    case HTTP_POST:
      return "POST";
    case HTTP_DELETE:
      return "DELETE";
  }
  return "UNKOWN";
}

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
  std::string mEndpointAddr;

  bool httpRequest(HttpOp op, std::string requestUrl, std::string json);

  virtual void process(std::vector<Bulk<Keys> >* data) = 0;
  virtual bool parseResponse(std::string response) = 0;

private:
  ConcurrentQueue<Bulk<Keys> > mQueue;
  std::vector<Bulk<Keys> >* mToProcess;
  int mToProcessLength;
  boost::mutex mLock;
  bool mShutdown;
  
  bool httpRequestInternal(HttpOp op, std::string requestUrl, std::string json);
  Response perform(HttpOp op, std::string requestUrl, std::string json);

  virtual void run();
  virtual void processBatch();
};

template<typename Keys>
TimedRestBatcher<Keys>::TimedRestBatcher(std::string endpoint_addr, int time_to_wait_before_inserting, int bulk_size)
: Batcher(time_to_wait_before_inserting, bulk_size), mToProcessLength(0) {
  mEndpointAddr = "http://" + endpoint_addr;
  curl_global_init(CURL_GLOBAL_ALL);
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
bool TimedRestBatcher<Keys>::httpRequest(HttpOp op, std::string requestUrl, std::string json) {
  ptime t1 = Utils::getCurrentTime();
  bool res = httpRequestInternal(op, requestUrl, json);
  ptime t2 = Utils::getCurrentTime();
  LOG_INFO(getStr(op) << " " << requestUrl << " [" << json.length() << "]  took " << Utils::getTimeDiffInMilliseconds(t1, t2) << " msec");
  return res;
}

template<typename Keys>
bool TimedRestBatcher<Keys>::httpRequestInternal(HttpOp op, std::string requestUrl, std::string json) {
  //TODO: handle different failure scenarios
  Response res = perform(op, requestUrl, json);

  if (res.mCode != CURLE_OK) {
    LOG_ERROR("CURL Failed: " << curl_easy_strerror(res.mCode));
    return false;
  }

  LOG_DEBUG(getStr(op) << " " << requestUrl << std::endl
          << json << std::endl << "Response::" << std::endl << res.mResponse);

  return parseResponse(res.mResponse);
}

template<typename Keys>
Response TimedRestBatcher<Keys>::perform(HttpOp op, std::string requestUrl, std::string json) {
  Response response;

  CURL* curl = curl_easy_init();

  if (Logger::isTrace()) {
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
  }

  curl_easy_setopt(curl, CURLOPT_URL, requestUrl.c_str());
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response.mResponse);

  struct curl_slist *headers = NULL;
  headers = curl_slist_append(headers, "Content-Type: application/json");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  switch (op) {
    case HTTP_POST:
      curl_easy_setopt(curl, CURLOPT_POST, 1);
      break;
    case HTTP_DELETE:
      curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
      break;
  }

  if (!json.empty()) {
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, json.length());
  }

  response.mCode = curl_easy_perform(curl);

  curl_easy_cleanup(curl);

  return response;
}

template<typename Keys>
TimedRestBatcher<Keys>::~TimedRestBatcher() {

}

#endif //TIMEDRESTBATCHER_H