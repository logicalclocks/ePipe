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

#ifndef PROVENANCEBATCHER_H
#define PROVENANCEBATCHER_H
#include "RCBatcher.h"
#include "ProvenanceTableTailer.h"
#include "ProvenanceDataReader.h"

class ProvenanceBatcher : public RCBatcher<ProvenanceRow, SConn, PKeys> {
public:

  ProvenanceBatcher(ProvenanceTableTailer* table_tailer, ProvenanceDataReaders* data_reader,
          const int time_before_issuing_ndb_reqs, const int batch_size)
  : RCBatcher(table_tailer, data_reader, time_before_issuing_ndb_reqs, batch_size) {

  }

};

#endif /* PROVENANCEBATCHER_H */

