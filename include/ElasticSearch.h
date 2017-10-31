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
 * File:   ElasticSearch.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 * Created on June 9, 2016, 1:39 PM
 */

#ifndef ELASTICSEARCH_H
#define ELASTICSEARCH_H
#include "Batcher.h"
#include "ConcurrentQueue.h"
#include <curl/curl.h>
#include "rapidjson/document.h"
#include "Utils.h"
#include "FsMutationsTableTailer.h"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>

namespace bc = boost::accumulators;

typedef bc::accumulator_set<double, bc::stats<bc::tag::mean, bc::tag::min, bc::tag::max> >  Accumulator;

enum HttpOp {
    HTTP_POST,
    HTTP_DELETE
};

struct Bulk{
    string mJSON;
    vector<ptime> mArrivalTimes;
    ptime mStartProcessing;
    ptime mEndProcessing;
    UISet mMetaPKs;
    FPK mFSPKs;
};

struct ESResponse{
    string mResponse;
    CURLcode mCode;
};

class ElasticSearch : public Batcher{
public:
    ElasticSearch(string elastic_addr, string index, string proj_type, string ds_type,
            string inode_type, int time_to_wait_before_inserting, int bulk_size,
            const bool stats, MConn conn);
    
    void addBulk(Bulk data);
    
    bool addProject(int projectId, string json);
    bool deleteProject(int projectId);
    bool deleteProjectChildren(int projectId, string json);
    
    bool addDataset(int projectId, int datasetId, string json);
    bool deleteDataset(int projectId, int datasetId);
    bool deleteDatasetChildren(int projectId, int datasetId, string json);
    
    bool deleteSchemaForINode(int projectId, int datasetId, int inodeId, string json);
    
    const char* getIndex();
    const char* getProjectType();
    const char* getDatasetType();
    const char* getINodeType();
    
    virtual ~ElasticSearch();
private:
    const string mIndex;
    const string mProjectType;
    const string mDatasetType;
    const string mInodeType;
    const bool mStats;
    
    string mElasticAddr;
    string mElasticBulkAddr;
    
    MConn mConn;
    ConcurrentQueue<Bulk> mQueue;
    
    vector<Bulk>* mToProcess;
    int mToProcessLength;
    boost::mutex mLock;
    
    Accumulator mBatchingAcc;
    Accumulator mWaitTimeBeforeProcessingAcc;
    Accumulator mProcessingAcc;
    Accumulator mWaitTimeUntillElasticCalledAcc;
    Accumulator mTotalTimePerEventAcc;
    Accumulator mTotalTimePerBulkAcc;
    long mTotalNumOfEventsProcessed;
    long mTotalNumOfBulksProcessed;
    ptime mFirstEventArrived;
    bool mIsFirstEventArrived;
    
    virtual void run();
    virtual void processBatch();
    
    void stats(vector<Bulk>* bulks);
    void stats(Bulk bulk, ptime t_elastic_done);
    string getElasticSearchBulkUrl();
    string getElasticSearchUrlonIndex(string index);
    string getElasticSearchUrlOnDoc(string index, string type, int doc);
    string getElasticSearchUpdateDocUrl(string index, string type, int doc);
    string getElasticSearchUpdateDocUrl(string index, string type, int doc, int parent);
    string getElasticSearchUpdateDocUrl(string index, string type, int doc, int parent, int routing); 
    string getElasticSearchDeleteDocUrl(string index, string type, int doc, int parent);
    string getElasticSearchDeleteByQueryUrl(string index, int routing);
    string getElasticSearchDeleteByQueryUrl(string index, int parent, int routing);
    
    bool elasticSearchHttpRequest(HttpOp op, string elasticUrl, string json);
    bool elasticSearchHttpRequestInternal(HttpOp op, string elasticUrl, string json);
    bool parseResponse(string response);
    ESResponse perform(HttpOp op, string elasticUrl, string json);
};

#endif /* ELASTICSEARCH_H */

