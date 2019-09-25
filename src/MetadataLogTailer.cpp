/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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

#include "MetadataLogTailer.h"

MetadataLogTailer::MetadataLogTailer(Ndb* ndb, Ndb* ndbRecovery, const int
poll_maxTimeToWait, const Barrier barrier)
: RCTableTailer<MetadataLogEntry> (ndb, ndbRecovery,new MetadataLogTable(),
    poll_maxTimeToWait, barrier) {
  mSchemaBasedQueue = new CMetaQ();
}

void MetadataLogTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, MetadataLogEntry pre, MetadataLogEntry row) {

  mSchemaBasedQueue->push(row);
  LOG_DEBUG(" push metalog " << row.mMetaPK.getPKStr() << " to queue, Op [" <<
  HopsworksOpTypeToStr(row.mMetaOpType) << "]");
}

MetadataLogEntry MetadataLogTailer::consume() {
  MetadataLogEntry res;
  mSchemaBasedQueue->wait_and_pop(res);
  LOG_TRACE(" pop metalog [" << res.mId << "] \n" << res.to_string());
  return res;
}

MetadataLogTailer::~MetadataLogTailer() {
  delete mSchemaBasedQueue;
}