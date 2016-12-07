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
 * File:   SchemalessMetadataTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "SchemalessMetadataTailer.h"
#include "Utils.h"

using namespace Utils::NdbC;

const string _schemaless_metadata_table= "meta_data_schemaless";
const int _schemaless_metadata_noCols= 6;
const string _schemaless_metadata_cols[_schemaless_metadata_noCols]=
    {"id",
     "inode_id",
     "inode_partition_id",
     "inode_parent_id",
     "inode_name",
     "data"
    };

const int _schemaless_metadata_noEvents = 3; 
const NdbDictionary::Event::TableEvent _schemaless_metadata_events[_schemaless_metadata_noEvents] = { NdbDictionary::Event::TE_INSERT,
NdbDictionary::Event::TE_UPDATE, NdbDictionary::Event::TE_DELETE};

const WatchTable SchemalessMetadataTailer::TABLE = {_schemaless_metadata_table, _schemaless_metadata_cols, 
_schemaless_metadata_noCols , _schemaless_metadata_events, _schemaless_metadata_noEvents,"PRIMARY",
_schemaless_metadata_cols[0]};


SchemalessMetadataTailer::SchemalessMetadataTailer(Ndb* ndb, const int poll_maxTimeToWait) : RCTableTailer(ndb, TABLE, poll_maxTimeToWait){
    mQueue = new CSmq();
}

void SchemalessMetadataTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) {
    SchemalessMetadataEntry entry;
    entry.mId = value[0]->int32_value();
    entry.mINodeId = value[1]->int32_value();
    entry.mPartitionId = value[2]->int32_value();
    entry.mParentId = value[3]->int32_value();
    entry.mInodeName = get_string(value[4]);
    entry.mJSONData = get_string(value[5]);
    entry.mOperation = Add;
    if(eventType == NdbDictionary::Event::TE_DELETE){
        entry.mOperation = Delete;
    }
    
    LOG_TRACE(" push schemaless metadata [" << entry.mId  << "," << entry.mParentId << "," << entry.mInodeName << "] to queue, Op [" << entry.mOperation << "]");
    
    mQueue->push(entry);
}

SchemalessMetadataEntry SchemalessMetadataTailer::consume() {
    SchemalessMetadataEntry entry;
    mQueue->wait_and_pop(entry);
    
    LOG_TRACE(" pop schemaless metadata [" << entry.mId  << "," << entry.mParentId << "," << entry.mInodeName << "] from queue, Op [" << entry.mOperation << "]");

    return entry;
}

SchemalessMetadataTailer::~SchemalessMetadataTailer() {
    delete mQueue;
}

