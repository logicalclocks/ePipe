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

#ifndef EPIPE_CDSTABLE_H
#define EPIPE_CDSTABLE_H

#include "tables/DBTable.h"

struct CDSRow {
  Int64 mCDSID;
};

class CDSTable : public DBTable<CDSRow> {

public:
  CDSTable() : DBTable("CDS") {
    addColumn("CD_ID");
  }

  CDSRow getRow(NdbRecAttr *value[]) {
    CDSRow row;
    row.mCDSID = value[0]->int64_value();
    return row;
  }

  void remove(Ndb *conn, Int64 CDID) {
    if(hasCDS(conn, CDID)) {
      start(conn);
      doDelete(CDID);
      LOG_DEBUG("Remove CDS entry with PK: " << CDID);
      end();
    }
  }

  bool hasCDS(Ndb *conn, Int64 pCDID){
    AnyMap key;
    key[0] = pCDID;
    return rowsExists(conn, "PRIMARY", key);
  }

};

#endif //EPIPE_CDSTABLE_H
