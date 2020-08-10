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

#ifndef EPIPE_COLUMNSV2TABLE_H
#define EPIPE_COLUMNSV2TABLE_H

#include "tables/DBTable.h"

struct COLUMNSV2Row {
  Int64 mCDID;
};

class COLUMNSV2Table : public DBTable<COLUMNSV2Row> {

public:
  COLUMNSV2Table() : DBTable("COLUMNS_V2") {
    addColumn("CD_ID");
  }

  COLUMNSV2Row getRow(NdbRecAttr *value[]) {
    COLUMNSV2Row row;
    row.mCDID = value[0]->int64_value();
    return row;
  }

  void removeByCDID(Ndb *conn, Int64 CDID)
  {
    AnyMap key;
    key[0] = CDID;
    int count = deleteByIndex(conn, "COLUMNS_V2_N49", key);
    LOG_INFO("Removed " << count << " entries of COLUMNS_V2 for CD_ID: " << CDID);
  }
};

#endif //EPIPE_COLUMNSV2TABLE_H
