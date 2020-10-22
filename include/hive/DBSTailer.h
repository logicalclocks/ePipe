/*
 * This file is part of ePipe
 * Copyright (C) 2020, Logical Clocks AB. All rights reserved
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

#ifndef DBSTAILER_H
#define DBSTAILER_H

#include "TableTailer.h"
#include "tables/hive/DBSTable.h"
#include "tables/hive/TBLSTable.h"

class DBSTailer : public TableTailer<DBSRow> {
public:
  DBSTailer(Ndb *ndb, const int poll_maxTimeToWait, const Barrier barrier)
      : TableTailer(ndb, new DBSTable(), poll_maxTimeToWait, barrier) {}

  virtual ~DBSTailer() {}

private:
  TBLSTable mTBLSTable;

  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, DBSRow pre,
    DBSRow row) {
    LOG_INFO("Delete DBS event received. Primary Key value: " << pre.mDBID);
    mTBLSTable.removeByDBID(mNdbConnection, pre.mDBID);
  }
};

#endif /* DBSTAILER_H */
