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

#ifndef TBLSTAILER_H
#define TBLSTAILER_H

#include "TableTailer.h"
#include "tables/hive/TBLSTable.h"
#include "tables/hive/SDSTable.h"

class TBLSTailer : public TableTailer<TBLSRow> {
public:
  TBLSTailer(Ndb *ndb, const int poll_maxTimeToWait, const Barrier barrier)
      : TableTailer(ndb, new TBLSTable(), poll_maxTimeToWait, barrier) {}

  virtual ~TBLSTailer() {}

private:
  SDSTable mSDSTable;

  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, TBLSRow
  pre, TBLSRow row) {
    LOG_INFO("Delete TBLS event received. Primary Key value: " << pre.mTBLID);
    mSDSTable.remove(mNdbConnection, pre.mSDID);
  }
};

#endif /* TBLSTAILER_H */
