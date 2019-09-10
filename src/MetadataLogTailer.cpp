/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * ePipe is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * ePipe is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
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