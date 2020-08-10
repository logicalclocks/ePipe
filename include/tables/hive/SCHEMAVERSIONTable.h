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

#ifndef EPIPE_SCHEMAVERSIONTABLE_H
#define EPIPE_SCHEMAVERSIONTABLE_H

#include "tables/DBTable.h"

struct SCHEMAVERSIONRow {
  Int64 mSCHEMAID;
  Int64 mSERDEID;
  Int64 mCDID;
};

class SCHEMAVERSIONTable : public DBTable<SCHEMAVERSIONRow> {

public:
  SCHEMAVERSIONTable() : DBTable("SCHEMA_VERSION") {
    addColumn("SCHEMA_ID");
    addColumn("SERDE_ID");
    addColumn("CD_ID");
  }

  SCHEMAVERSIONRow getRow(NdbRecAttr *value[]) {
    SCHEMAVERSIONRow row;
    row.mSCHEMAID = value[0]->int64_value();
    row.mSERDEID = value[1]->int64_value();
    row.mCDID = value[2]->int64_value();
    return row;
  }

  void removeBySchemaID(Ndb *conn, Int64 SCHEMAID)
  {
    AnyMap key;
    key[0] = SCHEMAID;
    int count = deleteByIndex(conn, "SIIndex", key);
    LOG_INFO("Removed " << count << " entries of SCHEMA_VERSION for SCHEMA_ID: " << SCHEMAID);
  }

  void removeBySERDEID(Ndb *conn, Int64 SERDEID)
  {
    AnyMap key;
    key[1] = SERDEID;
    int count = deleteByIndex(conn, "SERDEIDIndex", key);
    LOG_INFO("Removed " << count << " entries of SCHEMA_VERSION for SERDE_ID: " << SERDEID);
  }

  void removeByCDID(Ndb *conn, Int64 CDID)
  {
    AnyMap key;
    key[2] = CDID;
    int count = deleteByIndex(conn, "CDIDIndex", key);
    LOG_INFO("Removed " << count << " entries of SCHEMA_VERSION for CD_ID: " << CDID);
  }
};

#endif //EPIPE_SCHEMAVERSIONTABLE_H
