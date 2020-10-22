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

#ifndef EPIPE_TBLSTABLE_H
#define EPIPE_TBLSTABLE_H

#include "tables/DBWatchTable.h"

struct TBLSRow {
  Int64 mTBLID;
  Int64 mSDID;
  Int64 mDBID;
};

class TBLSTable : public DBWatchTable<TBLSRow> {

public:
  TBLSTable() : DBWatchTable("TBLS") {
    addColumn("TBL_ID");
    addColumn("SD_ID");
    addColumn("VIEW_EXPANDED_TEXT");
    addColumn("VIEW_ORIGINAL_TEXT");
    addColumn("DB_ID");
    addWatchEvent(NdbDictionary::Event::TE_DELETE);
  }

  TBLSRow getRow(NdbRecAttr *value[]) {
    TBLSRow row;
    row.mTBLID = value[0]->int64_value();
    row.mSDID = value[1]->int64_value();
    row.mDBID = value[4]->int64_value();
    return row;
  }

  void removeByDBID(Ndb *conn, Int64 DBID)
  {
    AnyMap key;
    key[4] = DBID;
    int count = deleteByIndex(conn, "TBLS_N49", key);
    LOG_INFO("Removed " << count << " entries of TBLS for DB_ID: " << DBID);
  }

  void removeBySDID(Ndb *conn, Int64 SDID)
  {
    AnyMap key;
    key[1] = SDID;
    int count = deleteByIndex(conn, "TBLS_N50", key);
    LOG_INFO("Removed " << count << " entries of TBLS for SD_ID: " << SDID);
  }
};

#endif //EPIPE_TBLSTABLE_H
