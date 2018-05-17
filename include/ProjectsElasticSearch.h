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
 * File:   ProjectsElasticSearch.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 * Created on June 9, 2016, 1:39 PM
 */

#ifndef PROJECTSELASTICSEARCH_H
#define PROJECTSELASTICSEARCH_H

#include "ElasticSearchBase.h"
#include "FsMutationsTableTailer.h"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>

namespace bc = boost::accumulators;

typedef bc::accumulator_set<double, bc::stats<bc::tag::mean, bc::tag::min, bc::tag::max> >  Accumulator;

struct FSKeys{
    UISet mMetaPKs;
    FPK mFSPKs;
};

typedef Bulk<FSKeys> FSBulk;

class ProjectsElasticSearch : public ElasticSearchBase<FSKeys>{
public:
    ProjectsElasticSearch(string elastic_addr, string index, string proj_type, string ds_type,
            string inode_type, int time_to_wait_before_inserting, int bulk_size,
            const bool stats, MConn conn);

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
    
    virtual ~ProjectsElasticSearch();
private:
    const string mIndex;
    const string mProjectType;
    const string mDatasetType;
    const string mInodeType;
    const bool mStats;

    string mElasticBulkAddr;

    MConn mConn;

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

    virtual void process(vector<FSBulk>* bulks);

    void stats(vector<FSBulk>* bulks);
    void stats(FSBulk bulk, ptime t_elastic_done);
    string getElasticSearchBulkUrl();
    string getElasticSearchUrlonIndex(string index);
    string getElasticSearchUrlOnDoc(string index, string type, int doc);
    string getElasticSearchUpdateDocUrl(string index, string type, int doc);
    string getElasticSearchUpdateDocUrl(string index, string type, int doc, int parent);
    string getElasticSearchUpdateDocUrl(string index, string type, int doc, int parent, int routing); 
    string getElasticSearchDeleteDocUrl(string index, string type, int doc, int parent);
    string getElasticSearchDeleteByQueryUrl(string index, int routing);
    string getElasticSearchDeleteByQueryUrl(string index, int parent, int routing);
};

#endif /* PROJECTSELASTICSEARCH_H */

