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

#include "ElasticSearch.h"
#include "MetadataLogTailer.h"

using namespace Utils;

static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
    ((std::string*)userp)->append((char*)contents, size * nmemb);
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


static string getAccString(Accumulator acc){
    stringstream out;
    out << "[" << bc::min(acc) << "," << bc::mean(acc) << "," << bc::max(acc) << "]";
    return out.str();
}

ElasticSearch::ElasticSearch(string elastic_addr, string index, string proj_type,
        string ds_type, string inode_type, int time_to_wait_before_inserting, 
        int bulk_size, const bool stats, MConn conn) : Batcher(time_to_wait_before_inserting, bulk_size),
        mIndex(index), mProjectType(proj_type), mDatasetType(ds_type), mInodeType(inode_type),
        mStats(stats), mConn(conn), mToProcessLength(0), mTotalNumOfEventsProcessed(0),
        mTotalNumOfBulksProcessed(0), mIsFirstEventArrived(false){
    
    mElasticAddr = "http://" + elastic_addr;
    mElasticBulkAddr = mElasticAddr + "/" + mIndex + "/" + mInodeType + "/_bulk";
    mToProcess = new vector<Bulk>();
    
    curl_global_init(CURL_GLOBAL_ALL); 
}

void ElasticSearch::addBulk(Bulk data) {
    LOG_TRACE("Add Bulk JSON:" << endl << data.mJSON << endl); 
    mQueue.push(data);
}

void ElasticSearch::run() {
    while(true){
        Bulk msg;
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

void ElasticSearch::processBatch() {
    if (mToProcessLength > 0) {
        LOG_TRACE("Process Bulk JSONs [" << mToProcessLength << "]");

        mLock.lock();
        vector<Bulk>* bulks = mToProcess;
        mToProcess = new vector<Bulk>;
        mToProcessLength = 0;
        mLock.unlock();

        UISet metaPKS;
        FPK fsPKS;
        string batch;
        for (vector<Bulk>::iterator it = bulks->begin(); it != bulks->end(); ++it) {
            Bulk bulk = *it;
            batch.append(bulk.mJSON);
            metaPKS.insert(bulk.mMetaPKs.begin(), bulk.mMetaPKs.end());
            fsPKS.insert(fsPKS.end(), bulk.mFSPKs.begin(), bulk.mFSPKs.end());
        }

        //TODO: handle failures
        if(elasticSearchHttpRequest(HTTP_POST, mElasticBulkAddr, batch)){
            if(!metaPKS.empty()){
                MetadataLogTailer::removeLogs(mConn.metadataConnection, metaPKS);
            }
            
            if(!fsPKS.empty()){
                FsMutationsTableTailer::removeLogs(mConn.inodeConnection, fsPKS);
            }
        }

        if (mStats) {
            stats(bulks);
        }
        delete bulks;
    }
}

void ElasticSearch::stats(vector<Bulk>* bulks) {
    ptime t_end = getCurrentTime();
    
    ptime firstEventInCurrentBulksArrivalTime = bulks->at(0).mArrivalTimes.at(0);
    int numOfEvents = 0;    
    for (vector<Bulk>::iterator it = bulks->begin(); it != bulks->end(); ++it) {
        Bulk bulk = *it;
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

void ElasticSearch::stats(Bulk bulk, ptime t_elastic_done) {
    
    int size = bulk.mArrivalTimes.size();
    float batch_time, wait_time, processing_time, ewait_time, total_time;
    if(size > 0){
        if(!mIsFirstEventArrived){
            mFirstEventArrived = bulk.mArrivalTimes[0];
            mIsFirstEventArrived = true;
        }
        batch_time = getTimeDiffInMilliseconds(bulk.mArrivalTimes[0], bulk.mArrivalTimes[size-1]);
        wait_time = getTimeDiffInMilliseconds(bulk.mArrivalTimes[size -1], bulk.mStartProcessing); 
        total_time = getTimeDiffInMilliseconds(bulk.mArrivalTimes[0], t_elastic_done);
    }
    
    processing_time = getTimeDiffInMilliseconds(bulk.mStartProcessing, bulk.mEndProcessing);
    ewait_time = getTimeDiffInMilliseconds(bulk.mEndProcessing, t_elastic_done);
    
    Accumulator total_time_acc;
    for(int i=0; i<size; i++){
        float total_time_per_event = getTimeDiffInMilliseconds(bulk.mArrivalTimes[i], t_elastic_done);
        total_time_acc(total_time_per_event);
        mTotalTimePerEventAcc(total_time_per_event);
    }
    
    
    LOG_INFO("Bulk[" << size << "] took " << total_time << " msec, TotalTime/Event=" << getAccString(total_time_acc) 
            <<  ", Batch=" << batch_time << " msec, WaitTime=" << wait_time << " msec, Processing="
            << processing_time << " msec, eWait=" << ewait_time << " msec");
    
    mBatchingAcc(batch_time);
    mWaitTimeBeforeProcessingAcc(wait_time);    
    mProcessingAcc(processing_time);
    mWaitTimeUntillElasticCalledAcc(ewait_time);
    mTotalTimePerBulkAcc(total_time);
    mTotalNumOfEventsProcessed += size;
    mTotalNumOfBulksProcessed++;
}

const char* ElasticSearch::getIndex() {
    return mIndex.c_str();
}

const char* ElasticSearch::getProjectType() {
    return mProjectType.c_str();
}

const char* ElasticSearch::getDatasetType() {
    return mDatasetType.c_str();
}

const char* ElasticSearch::getINodeType() {
    return mInodeType.c_str();
}

bool ElasticSearch::addProject(int projectId, string json) {
    string url = getElasticSearchUpdateDocUrl(mIndex, mProjectType, projectId);
    return elasticSearchHttpRequest(HTTP_POST, url, json);
}

bool ElasticSearch::deleteProject(int projectId) {
    string deleteProjUrl = getElasticSearchUrlOnDoc(mIndex, mProjectType, projectId);
    return elasticSearchHttpRequest(HTTP_DELETE, deleteProjUrl, string());
}

bool ElasticSearch::deleteProjectChildren(int projectId, string json) {
    string deteteProjectChildren = getElasticSearchDeleteByQueryUrl(mIndex, projectId);
    return elasticSearchHttpRequest(HTTP_DELETE, deteteProjectChildren, json);
}

bool ElasticSearch::addDataset(int projectId, int datasetId, string json) {
    string url = getElasticSearchUpdateDocUrl(mIndex, mDatasetType, datasetId, projectId);
    return elasticSearchHttpRequest(HTTP_POST, url, json);
}

bool ElasticSearch::deleteDataset(int projectId, int datasetId) {
    string deleteDatasetUrl = getElasticSearchDeleteDocUrl(mIndex, mDatasetType, datasetId, projectId);
    return elasticSearchHttpRequest(HTTP_DELETE, deleteDatasetUrl, string());
}

bool ElasticSearch::deleteDatasetChildren(int projectId, int datasetId, string json) {
    string deteteDatasetChildren = getElasticSearchDeleteByQueryUrl(mIndex, datasetId, projectId);
    return elasticSearchHttpRequest(HTTP_DELETE, deteteDatasetChildren, json);
}

bool ElasticSearch::deleteSchemaForINode(int projectId, int datasetId, int inodeId, string json) {
    string url = getElasticSearchUpdateDocUrl(mIndex, (inodeId == datasetId ? mDatasetType : mInodeType), inodeId, datasetId, projectId);
    return elasticSearchHttpRequest(HTTP_POST, url, json);
}

string ElasticSearch::getElasticSearchUrlonIndex(string index) {
    string str = mElasticAddr + "/" + index;
    return str;
}

string ElasticSearch::getElasticSearchUrlOnDoc(string index, string type, int doc) {
    stringstream out;
    out << getElasticSearchUrlonIndex(index) << "/" << type << "/" << doc;
    return out.str();
}

string ElasticSearch::getElasticSearchUpdateDocUrl(string index, string type, int doc) {
    string str = getElasticSearchUrlOnDoc(index, type, doc) + "/_update";
    return str;
}

string ElasticSearch::getElasticSearchUpdateDocUrl(string index, string type, int doc, int parent) {
    stringstream out;
    out << getElasticSearchUpdateDocUrl(index, type, doc) << "?parent=" << parent;
    return out.str();
}

string ElasticSearch::getElasticSearchUpdateDocUrl(string index, string type, int doc, int parent, int routing) {
    stringstream out;
    out << getElasticSearchUpdateDocUrl(index, type, doc, parent) << "&routing=" << routing;
    return out.str();
}

string ElasticSearch::getElasticSearchDeleteDocUrl(string index, string type, int doc, int parent) {
    stringstream out;
    out << getElasticSearchUrlOnDoc(index, type, doc) << "?parent=" << parent;
    return out.str();
}

string ElasticSearch::getElasticSearchDeleteByQueryUrl(string index, int routing) {
    stringstream out;
    out << getElasticSearchUrlonIndex(index) << "/_query" << "?routing=" << routing;
    return out.str();
}

string ElasticSearch::getElasticSearchDeleteByQueryUrl(string index, int parent, int routing) {
    stringstream out;
    out << getElasticSearchDeleteByQueryUrl(index, routing) << "&parent=" << parent;
    return out.str();
}

bool ElasticSearch::elasticSearchHttpRequest(HttpOp op, string elasticUrl, string json) {
    ptime t1 = Utils::getCurrentTime();
    bool res = elasticSearchHttpRequestInternal(op, elasticUrl, json);
    ptime t2 = Utils::getCurrentTime();
    LOG_INFO(getStr(op) << " " << elasticUrl << " [" << json.length() << "]  took " << Utils::getTimeDiffInMilliseconds(t1, t2) << " msec");
    return res;
}

bool ElasticSearch::elasticSearchHttpRequestInternal(HttpOp op, string elasticUrl, string json) {
    //TODO: handle different failure scenarios
    ESResponse res = perform(op, elasticUrl, json);
    
    if (res.mCode != CURLE_OK) {
        LOG_ERROR("CURL Failed: " << curl_easy_strerror(res.mCode));
        return false;
    }

    LOG_TRACE(getStr(op) << " " << elasticUrl << endl
            << json << endl << "Response::" << endl << res.mResponse);
    
    return parseResponse(res.mResponse);
}

ESResponse ElasticSearch::perform(HttpOp op, string elasticUrl, string json) {
    ESResponse response;
    
    CURL* curl = curl_easy_init();

    if (Logger::isTrace()) {
        curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
    }
     
    curl_easy_setopt(curl, CURLOPT_URL, elasticUrl.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response.mResponse);
    
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

bool ElasticSearch::parseResponse(string response) {
    try {
        rapidjson::Document d;
        if (!d.Parse<0>(response.c_str()).HasParseError()) {
            if (d.HasMember("errors")) {
                const rapidjson::Value &bulkErrors = d["errors"];
                if (bulkErrors.IsBool() && bulkErrors.GetBool()) {
                    const rapidjson::Value &items = d["items"];
                    stringstream errors;
                    for (rapidjson::SizeType i = 0; i < items.Size(); ++i) {
                        const rapidjson::Value &obj = items[i];
                        for (rapidjson::Value::ConstMemberIterator itr = obj.MemberBegin(); itr != obj.MemberEnd(); ++itr) {
                            const rapidjson::Value & opObj = itr->value;
                            if (opObj.HasMember("error")) {
                                const rapidjson::Value & error = opObj["error"];
                                if (error.IsObject()) {
                                    const rapidjson::Value & errorType = error["type"];
                                    const rapidjson::Value & errorReason = error["reason"];
                                    errors << errorType.GetString() << ":" << errorReason.GetString();
                                } else if (error.IsString()) {
                                    errors << error.GetString();
                                }
                                errors << ", ";
                            }
                        }
                    }
                    string errorsStr = errors.str();
                    LOG_ERROR(" ES got errors: " << errorsStr);
                    return false;
                }
            } else if (d.HasMember("error")) {
                const rapidjson::Value &error = d["error"];
                if (error.IsObject()) {
                    const rapidjson::Value & errorType = error["type"];
                    const rapidjson::Value & errorReason = error["reason"];
                    LOG_ERROR(" ES got error: " << errorType.GetString() << ":" << errorReason.GetString());
                } else if (error.IsString()) {
                    LOG_ERROR(" ES got error: " << error.GetString());
                }
                return false;
            }
        } else {
            LOG_ERROR(" ES got json error (" << d.GetParseError() << ") while parsing (" << response << ")");
            return false;
        }

    } catch (std::exception &e) {
        LOG_ERROR(e.what());
        return false;
    }
    return true;
}

ElasticSearch::~ElasticSearch() {
}

