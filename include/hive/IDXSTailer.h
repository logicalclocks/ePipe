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

#ifndef IDXSTAILER_H
#define IDXSTAILER_H

#include "TableTailer.h"
#include "tables/hive/IDXSTable.h"
#include "tables/hive/SDSTable.h"

class IDXSTailer : public TableTailer<IDXSRow> {
public:
  IDXSTailer(Ndb *ndb, const int poll_maxTimeToWait, const Barrier barrier)
      : TableTailer(ndb, new IDXSTable(), poll_maxTimeToWait, barrier) {}

  virtual ~IDXSTailer() {}

private:
  SDSTable mSDSTable;

  // If the table has been deleted from the fs the index information will be
  // deleted from the db however the index directory will remain on the fs.
  // Users will have to delete it from the fs as well.
  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType,
                           IDXSRow pre, IDXSRow row) {
    LOG_INFO("Delete IDXS event received. Primary Key value: " << pre.mINDEXID);
    // Delete SDS entry related to the index
    mSDSTable.remove(mNdbConnection, pre.mSDID);
  }
};

#endif /* IDXSTAILER_H */
