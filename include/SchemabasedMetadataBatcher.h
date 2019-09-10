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

#ifndef SCHEMABASEDMETADATABATCHER_H
#define SCHEMABASEDMETADATABATCHER_H

#include "MetadataLogTailer.h"
#include "SchemabasedMetadataReader.h"

class SchemabasedMetadataBatcher : public RCBatcher<MetadataLogEntry, MConn, FSKeys> {
public:

  SchemabasedMetadataBatcher(MetadataLogTailer* table_tailer, SchemabasedMetadataReaders* data_reader,
          const int time_before_issuing_ndb_reqs, const int batch_size)
  : RCBatcher<MetadataLogEntry, MConn, FSKeys>(table_tailer, data_reader,
  time_before_issuing_ndb_reqs, batch_size) {

  }
};

#endif /* SCHEMABASEDMETADATABATCHER_H */

