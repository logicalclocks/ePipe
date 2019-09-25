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

#include "SchemabasedMetadataReader.h"

SchemabasedMetadataReader::SchemabasedMetadataReader(MConn connection,
        const bool hopsworks, const int lru_cap)
: NdbDataReader<MetadataLogEntry, MConn>(connection, hopsworks)
, mSchemabasedTable(lru_cap) {

}

void SchemabasedMetadataReader::processAddedandDeleted(MetaQ* data_batch,
    eBulk& bulk) {

  SchemabasedMq* data_queue = mSchemabasedTable.get(mNdbConnection.metadataConnection,
          data_batch);

  createJSON(data_queue, bulk);
}

void SchemabasedMetadataReader::createJSON(SchemabasedMq* data_batch, eBulk&
bulk) {
  for (SchemabasedMq::iterator it = data_batch->begin(); it != data_batch->end(); ++it) {
    SchemabasedMetadataEntry entry = *it;
    LOG_TRACE("create JSON for " << entry.to_string());
    bulk.push(mMetadataLogTable.getLogRemovalHandler(entry
    .getMetadataLogEntry()), entry.mEventCreationTime, entry.to_create_json());
  }
}

SchemabasedMetadataReader::~SchemabasedMetadataReader() {

}

