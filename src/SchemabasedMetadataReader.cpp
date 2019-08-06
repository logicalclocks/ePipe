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
 * File:   MetadataReader.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "SchemabasedMetadataReader.h"

SchemabasedMetadataReader::SchemabasedMetadataReader(MConn connection,
        const bool hopsworks, const int lru_cap)
: NdbDataReader<MetadataLogEntry, MConn, FSKeys>(connection, hopsworks)
, mSchemabasedTable(lru_cap) {

}

void SchemabasedMetadataReader::processAddedandDeleted(MetaQ* data_batch, FSBulk& bulk) {

  SchemabasedMq* data_queue = mSchemabasedTable.get(mNdbConnection.metadataConnection,
          data_batch, bulk.mPKs.mMetaPKs);

  createJSON(data_queue, bulk);
}

void SchemabasedMetadataReader::createJSON(SchemabasedMq* data_batch, FSBulk& bulk) {

  std::vector<ptime> arrivalTimes(data_batch->size());
  std::stringstream out;
  int i = 0;
  for (SchemabasedMq::iterator it = data_batch->begin(); it != data_batch->end(); ++it, i++) {
    SchemabasedMetadataEntry entry = *it;
    LOG_TRACE("create JSON for " << entry.to_string());
    arrivalTimes[i] = entry.mEventCreationTime;

    out << entry.to_create_json() << std::endl;
  }

  bulk.mArrivalTimes = arrivalTimes;
  bulk.mJSON = out.str();
}

SchemabasedMetadataReader::~SchemabasedMetadataReader() {

}

