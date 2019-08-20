/*
 * Copyright (C) 2019 Logical Clocks AB
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

/*
 * File:   SDSTable.h
 * Author: Mahmoud Ismail<mahmoud@logicalclocks.com>
 *
 */

#ifndef EPIPE_SDSTABLE_H
#define EPIPE_SDSTABLE_H

#include "tables/DBWatchTable.h"

struct SDSRow {
  Int64 mSDID;
  Int64 mCDID;
  Int64 mSERDEID;
};

typedef std::vector<SDSRow> SDSVec;
class SDSTable : public DBWatchTable<SDSRow>{

public:
  SDSTable() : DBWatchTable("SDS"){
    addColumn("SD_ID");
    addColumn("CD_ID");
    addColumn("SERDE_ID");
    addWatchEvent(NdbDictionary::Event::TE_DELETE);
  }

  SDSRow getRow(NdbRecAttr* value[]){
    SDSRow row;
    row.mSDID = value[0]->int64_value();
    row.mCDID = value[1]->int64_value();
    row.mSERDEID = value[2]->int64_value();
    return row;
  }

  void remove(Ndb* conn, Int64 SDID) {
    if(hasSDS(conn, SDID)) {
      start(conn);
      doDelete(SDID);
      LOG_DEBUG("Remove SDS entry with PK: " << SDID);
      end();
    }
  }

  bool hasCDID(Ndb* conn, Int64 pCDID){
    AnyMap key;
    key[1] = pCDID;
    return rowsExists(conn, "SDS_N50", key);
  }

  bool hasSDS(Ndb* conn, Int64 pSDSID){
    AnyMap key;
    key[1] = pSDSID;
    return rowsExists(conn, "PRIMARY", key);
  }

};
#endif //EPIPE_SDSTABLE_H
