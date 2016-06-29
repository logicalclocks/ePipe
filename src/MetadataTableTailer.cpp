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
 * File:   MetadataTableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "MetadataTableTailer.h"

using namespace Utils::NdbC;

const string _metadata_table= "meta_data";
const int _metadata_noCols= 4;
const string _metadata_cols[_metadata_noCols]=
    {"id",
     "fieldid",
     "tupleid",
     "data"
    };

const int _metadata_noEvents = 3; 
const NdbDictionary::Event::TableEvent _metadata_events[_metadata_noEvents] = 
    { NdbDictionary::Event::TE_INSERT, 
      NdbDictionary::Event::TE_UPDATE, 
      NdbDictionary::Event::TE_DELETE
    };

const WatchTable MetadataTableTailer::TABLE = {_metadata_table, _metadata_cols, _metadata_noCols , _metadata_events, _metadata_noEvents, "PRIMARY", _metadata_cols[0]};

MetadataTableTailer::MetadataTableTailer(Ndb* ndb, const int poll_maxTimeToWait)
    : RCTableTailer<MetadataEntry> (ndb, TABLE, poll_maxTimeToWait) {
    mQueue = new Cmq();
}

void MetadataTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
    MetadataEntry entry;
    entry.mEventCreationTime = Utils::getCurrentTime();
    entry.mId = value[0]->int32_value();
    entry.mFieldId = value[1]->int32_value();
    entry.mTupleId = value[2]->int32_value();
    entry.mMetadata = get_string(value[3]);
    entry.mOperation = ADD;
    if(eventType == NdbDictionary::Event::TE_DELETE){
        entry.mOperation = DELETE;
    }
    
    LOG_TRACE(" push metadata [" << entry.mId  << "," << entry.mTupleId << "," << entry.mFieldId << "] to queue, Op [" << entry.mOperation << "]");
    mQueue->push(entry);
}

MetadataEntry MetadataTableTailer::consume() {
    MetadataEntry res;
    mQueue->wait_and_pop(res);
    LOG_TRACE(" pop metadata [" << res.mId  << "," << res.mTupleId << "," << res.mFieldId << "] to queue, Op [" << res.mOperation << "]");
    res.print();
    return res;
}

MetadataTableTailer::~MetadataTableTailer() {
    delete mQueue;
}

