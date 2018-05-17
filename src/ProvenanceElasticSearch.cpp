/*
 * Copyright (C) 2018 Hops.io
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
 * File:   ProvenanceElasticSearch.cpp
 * Author: Mahmoud Ismail <maism@kth.se>
 * 
 */

#include "ProvenanceElasticSearch.h"

ProvenanceElasticSearch::ProvenanceElasticSearch(string elastic_addr, 
        string index, string type, int time_to_wait_before_inserting,
        int bulk_size, const bool stats, SConn conn) : 
ElasticSearchBase(elastic_addr,time_to_wait_before_inserting,bulk_size), 
        mIndex(index), mType(type), mStats(stats), mConn(conn){
    mElasticBulkAddr = mElasticAddr + "/" + mIndex + "/" + mType + "/_bulk";
}

void ProvenanceElasticSearch::process(vector<PBulk>* bulks){
    PKeys keys;
    string batch;
    for (vector<PBulk>::iterator it = bulks->begin(); it != bulks->end(); ++it) {
        PBulk bulk = *it;
        batch.append(bulk.mJSON);
        keys.insert(keys.end(), bulk.mPKs.begin(), bulk.mPKs.end());
    }

    //TODO: handle failures
    if(elasticSearchHttpRequest(HTTP_POST, mElasticBulkAddr, batch)){
        if(!keys.empty()){
            ProvenanceTableTailer::removeLogs(mConn, keys);
        }

    }
   //TODO: stats
}

ProvenanceElasticSearch::~ProvenanceElasticSearch(){
    
}

