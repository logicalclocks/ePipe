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

MetadataLogTailer::MetadataLogTailer(Ndb* ndb, const int poll_maxTimeToWait, const Barrier barrier)
: RCTableTailer<MetadataLogEntry> (ndb, new MetadataLogTable(), poll_maxTimeToWait, barrier) {
  mSchemaBasedQueue = new CMetaQ();
  mSchemalessQueue = new CMetaQ();
}

void MetadataLogTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, MetadataLogEntry pre, MetadataLogEntry row) {

  if (row.mMetaType == Schemabased) {
    mSchemaBasedQueue->push(row);
  } else if (row.mMetaType == Schemaless) {
    mSchemalessQueue->push(row);
  } else {
    LOG_FATAL("Unkown MetadataType " << row.mMetaType);
    return;
  }

  LOG_DEBUG(" push metalog " << row.mMetaPK.to_string() << " to queue, Op [" << HopsworksOpTypeToStr(row.mMetaOpType) << "]");
}

MetadataLogEntry MetadataLogTailer::consumeMultiQueue(int queue_id) {
  MetadataLogEntry res;
  if (queue_id == Schemabased) {
    mSchemaBasedQueue->wait_and_pop(res);
  } else if (queue_id == Schemaless) {
    mSchemalessQueue->wait_and_pop(res);
  } else {
    LOG_FATAL("Unkown Queue Id, It should be either " << Schemabased << " or " << Schemaless << " but got " << queue_id << " instead");
    return res;
  }

  LOG_TRACE(" pop metalog [" << res.mId << "] \n" << res.to_string());
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