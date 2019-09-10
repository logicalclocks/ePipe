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

#ifndef PARTTAILER_H
#define PARTTAILER_H

#include "TableTailer.h"
#include "tables/hive/PartitionsTable.h"
#include "tables/hive/SDSTable.h"

class PARTTailer : public TableTailer<PartitionsRow> {
public:
  PARTTailer(Ndb *ndb, const int poll_maxTimeToWait, const Barrier barrier)
      : TableTailer(ndb, new PartitionsTable(), poll_maxTimeToWait, barrier) {}

  virtual ~PARTTailer() {}

private:
  SDSTable mSDSTable;

  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType,
                           PartitionsRow pre, PartitionsRow row) {
    LOG_INFO("Delete PART event received. Primary Key value: " << pre.mPARTID);
    mSDSTable.remove(mNdbConnection, pre.mSDID);
  }
};

#endif /* PARTTAILER_H */
