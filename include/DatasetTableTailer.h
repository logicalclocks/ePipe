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
 * File:   DatasetTableTailer.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef DATASETTABLETAILER_H
#define DATASETTABLETAILER_H

#include "TableTailer.h"
#include "DatasetProjectCache.h"

class DatasetTableTailer : public TableTailer{
public:
    DatasetTableTailer(Ndb* ndb, const int poll_maxTimeToWait, string elastic_addr, 
            const string elastic_index, const string elastic_dataset_type, DatasetProjectCache* cache);
    virtual ~DatasetTableTailer();
private:
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);
    string mElasticAddr;
    const string mElasticIndex;
    const string mElasticDatasetType;
    DatasetProjectCache* mDatasetProjectCache;
};

#endif /* DATASETTABLETAILER_H */

