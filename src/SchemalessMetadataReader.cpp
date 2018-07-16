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
 * File:   SchemalessMetadataReader.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "SchemalessMetadataReader.h"

SchemalessMetadataReader::SchemalessMetadataReader(MConn connection,
        const bool hopsworks, ProjectsElasticSearch* elastic)
: NdbDataReader<MetadataLogEntry, MConn, FSKeys>(connection, hopsworks, elastic) {
}

void SchemalessMetadataReader::processAddedandDeleted(MetaQ* data_batch, FSBulk& bulk) {

  SchemalessMq* data_queue = mSchemalessTable.get(mNdbConnection.metadataConnection,
          data_batch, bulk.mPKs.mMetaPKs);

  createJSON(data_queue, bulk);
}

void SchemalessMetadataReader::createJSON(SchemalessMq* data_batch, FSBulk& bulk) {

  vector<ptime> arrivalTimes(data_batch->size());
  stringstream out;
  int i = 0;
  for (SchemalessMq::iterator it = data_batch->begin(); it != data_batch->end(); ++it, i++) {
    SchemalessMetadataEntry entry = *it;
    LOG_TRACE("create JSON for " << entry.to_string());
    arrivalTimes[i] = entry.mEventCreationTime;

    out << entry.to_create_json();
  }

  bulk.mArrivalTimes = arrivalTimes;
  bulk.mJSON = out.str();
}

SchemalessMetadataReader::~SchemalessMetadataReader() {

}

