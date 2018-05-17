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
 * File:   ProvenanceElasticSearch.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef PROVENANCEELASTICSEARCH_H
#define PROVENANCEELASTICSEARCH_H

#include "ElasticSearchBase.h"
#include "ProvenanceTableTailer.h"

typedef Bulk<PKeys> PBulk;

class ProvenanceElasticSearch : public ElasticSearchBase<PKeys>{
public:
    ProvenanceElasticSearch(string elastic_addr, string index, string type, 
            int time_to_wait_before_inserting, int bulk_size, const bool stats, 
            SConn conn);
    virtual ~ProvenanceElasticSearch();
private:
    const string mIndex;
    const string mType;
    const bool mStats;
    
    string mElasticBulkAddr;

    SConn mConn;
    
    virtual void process(vector<PBulk>* bulks);

};

#endif /* PROVENANCEELASTICSEARCH_H */

