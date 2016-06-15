/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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

enum HttpOp {
    HTTP_POST,
    HTTP_DELETE
};

class ElasticSearch : public Batcher{
public:
    ElasticSearch(string elastic_addr, string index, string proj_type, string ds_type,
            string inode_type, int time_to_wait_before_inserting, int bulk_size);
    
    void addBulk(string json);
    
    bool addProject(int projectId, string json);
    bool deleteProject(int projectId);
    bool deleteProjectChildren(int projectId, string json);
    
    bool addDataset(int projectId, int datasetId, string json);
    bool deleteDataset(int projectId, int datasetId);
    bool deleteDatasetChildren(int projectId, int datasetId, string json);
    
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
    
    string mElasticAddr;
    string mElasticBulkAddr;
    
    ConcurrentQueue<string> mQueue;
    
    mutable boost::mutex mLock;
    stringstream mToProcess;
    int mToProcessLength;
    
    CURL* mHttpHandle;
    
    void addToProcess(string str);
    string resetToProcess();
    
    virtual void run();
    virtual void processBatch();
    
    string getElasticSearchBulkUrl();
    string getElasticSearchUrlonIndex(string index);
    string getElasticSearchUrlOnDoc(string index, string type, int doc);
    string getElasticSearchUpdateDocUrl(string index, string type, int doc);
    string getElasticSearchUpdateDocUrl(string index, string type, int doc, int parent);
    string getElasticSearchDeleteDocUrl(string index, string type, int doc, int parent);
    string getElasticSearchDeleteByQueryUrl(string index, int routing);
    string getElasticSearchDeleteByQueryUrl(string index, int parent, int routing);
    
    bool elasticSearchHttpRequest(HttpOp op, string elasticUrl, string json);
    bool elasticSearchHttpRequestInternal(HttpOp op, string elasticUrl, string json);
    bool parseResponse(string response);
    CURLcode perform(HttpOp op, string elasticUrl, string json, string &response);
    CURL* getCurlHandle();
};

#endif /* ELASTICSEARCH_H */

