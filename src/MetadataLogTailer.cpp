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
 * File:   MetadataLogTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "MetadataLogTailer.h"

using namespace Utils::NdbC;

const string _metalog_table= "meta_log";
const int _metalog_noCols= 6;
const string _metalog_cols[_metalog_noCols]=
    {"id",
     "meta_pk1",
     "meta_pk2",
     "meta_pk3",
     "meta_type",
     "meta_op_type"
    };

const int _metalog_noEvents = 1; 
const NdbDictionary::Event::TableEvent _metalog_events[_metalog_noEvents] = { NdbDictionary::Event::TE_INSERT };

const WatchTable MetadataLogTailer::TABLE = {_metalog_table, _metalog_cols, _metalog_noCols , _metalog_events, _metalog_noEvents, "PRIMARY", _metalog_cols[0]};

MetadataLogTailer::MetadataLogTailer(Ndb* ndb, const int poll_maxTimeToWait)
    : RCTableTailer<MetadataLogEntry> (ndb, TABLE, poll_maxTimeToWait) {
   mSchemaBasedQueue = new Cmq();
   mSchemalessQueue = new Cmq();
}

void MetadataLogTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
    MetadataLogEntry entry;
    entry.mEventCreationTime = Utils::getCurrentTime();
    entry.mId = value[0]->int32_value();
    entry.mMetaPK1 = value[1]->int32_value();
    entry.mMetaPK2 = value[2]->int32_value();
    entry.mMetaPK3 = value[3]->int32_value();
    entry.mMetaType = static_cast<MetadataType>(value[4]->int8_value());
    entry.mMetaOpType = static_cast<OperationType>(value[5]->int8_value());

    if (entry.mMetaType == SchemaBased) {
        mSchemaBasedQueue->push(entry);
    } else if (entry.mMetaType == Schemaless) {
        mSchemalessQueue->push(entry);
    }else{
        LOG_FATAL("Unkown MetadataType " << entry.mMetaType);
        return;
    }

    LOG_TRACE(" push metalog [" << entry.mId << "," << entry.mMetaPK1 << "," << entry.mMetaPK2
            << "," << entry.mMetaPK3 << "] to queue, Op [" << Utils::OperationTypeToStr(entry.mMetaOpType) << "]");
}

MetadataLogEntry MetadataLogTailer::consumeMultiQueue(int queue_id) {
    MetadataLogEntry res;
    if(queue_id == SchemaBased){
        mSchemaBasedQueue->wait_and_pop(res);
    }else if(queue_id == Schemaless){
        mSchemalessQueue->wait_and_pop(res);
    }else{
        LOG_FATAL("Unkown Queue Id, It should be either " << SchemaBased << " or " << Schemaless << " but got " << queue_id << " instead");
        return res;
    }
    
    LOG_TRACE(" pop metalog [" << res.mId  << "] \n" << res.to_string());
    return res;
}

MetadataLogEntry MetadataLogTailer::consume() {
    MetadataLogEntry res;
    LOG_FATAL("consume shouldn't be called");
    return res;
}

MetadataLogTailer::~MetadataLogTailer() {
    delete mSchemaBasedQueue;
    delete mSchemalessQueue;
}