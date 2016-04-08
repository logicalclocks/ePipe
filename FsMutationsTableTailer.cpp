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
 * File:   MutationsTableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "FsMutationsTableTailer.h"
#include "Utils.h"

using namespace Utils;

const char *MUTATION_TABLE_NAME= "hdfs_metadata_log";
const int NO_MUTATION_TABLE_COLUMNS= 6;
const char *MUTATION_TABLE_COLUMNS[NO_MUTATION_TABLE_COLUMNS]=
    {"dataset_id",
     "inode_id",
     "timestamp",
     "inode_pid",
     "inode_name",
     "operation"
    };

const NdbDictionary::Event::TableEvent WATCH_EVENT_TYPE = NdbDictionary::Event::TE_INSERT;

FsMutationsTableTailer::FsMutationsTableTailer(Ndb* ndb) : TableTailer(ndb, MUTATION_TABLE_NAME, MUTATION_TABLE_COLUMNS, 
        NO_MUTATION_TABLE_COLUMNS, WATCH_EVENT_TYPE) {
    mQueue = new Cpq();
}

void FsMutationsTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
    FsMutationRow row;
    row.mDatasetId = value[0]->int32_value();
    row.mInodeId =  value[1]->int32_value();
    row.mTimestamp = value[2]->int64_value();
    row.mParentId = value[3]->int8_value();
    row.mInodeName = get_string(value[4]);
    row.mOperation = value[5]->int8_value();
    LOG_DEBUG() << " push inode [" << row.mInodeId << "] to queue";
    mQueue->push(row);
}

FsMutationRow FsMutationsTableTailer::consume(){
    FsMutationRow row;
    mQueue->wait_and_pop(row);
    LOG_DEBUG() << " pop inode [" << row.mInodeId << "] from queue";
    return row;
}

FsMutationsTableTailer::~FsMutationsTableTailer() {
    delete mQueue;
}

