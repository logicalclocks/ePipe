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
    ProjectsElasticSearch(string elastic_addr, string index, 
            int time_to_wait_before_inserting, int bulk_size,
            const bool stats, MConn conn);

    bool addDoc(int inodeId, string json);
    bool deleteDocsByQuery(string json);
    
    bool deleteSchemaForINode(int inodeId, string json);
    
    static const string getProjectType();
    static const string getDatasetType();
    static const string getINodeType();
    
    virtual ~ProjectsElasticSearch();
private:
    const string mIndex;
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
};

#endif /* PROJECTSELASTICSEARCH_H */

