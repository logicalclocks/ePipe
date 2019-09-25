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

#ifndef EPIPE_SERDESTABLE_H
#define EPIPE_SERDESTABLE_H
#include "tables/DBTable.h"

struct SERDESRow{
  Int64 mSERDEID;
};

class SERDESTable : public DBTable<SERDESRow>{

public:
  SERDESTable() : DBTable("SERDES"){
    addColumn("SERDE_ID");
  }

  SERDESRow getRow(NdbRecAttr* value[]){
    SERDESRow row;
    row.mSERDEID = value[0]->int64_value();
    return row;
  }

  void remove(Ndb* conn, Int64 pSERDEID) {
    if(hasSERDES(conn, pSERDEID)) {
      start(conn);
      doDelete(pSERDEID);
      LOG_DEBUG("Remove SERDES entry with PK: " << pSERDEID);
      end();
    }
  }

  bool hasSERDES(Ndb *conn, Int64 pSERDEID){
    AnyMap key;
    key[0] = pSERDEID;
    return rowsExists(conn, "PRIMARY", key);
  }
};
#endif //EPIPE_SERDESTABLE_H
