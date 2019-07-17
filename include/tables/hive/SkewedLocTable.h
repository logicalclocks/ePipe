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
 * File:   SkewedLocTable.h
 * Author: Mahmoud Ismail<mahmoud@logicalclocks.com>
 *
 */

#ifndef EPIPE_SKEWEDLOCTABLE_H
#define EPIPE_SKEWEDLOCTABLE_H

#include "tables/DBWatchTable.h"

struct SkewedLocRow{
  Int64 mSDID;
  Int64 mStringListID;
};

class SkewedLocTable : public DBWatchTable<SkewedLocRow> {
public:
  SkewedLocTable() : DBWatchTable("SKEWED_COL_VALUE_LOC_MAP"){
    addColumn("SD_ID");
    addColumn("STRING_LIST_ID_KID");
    addWatchEvent(NdbDictionary::Event::TE_DELETE);
  }

  SkewedLocRow getRow(NdbRecAttr *value[]){
    SkewedLocRow row;
    row.mSDID = value[0]->int64_value();
    row.mStringListID = value[1]->int64_value();
    return row;
  }

  bool hasStringListID(Ndb* conn, Int64 pStringListID){
    AnyMap key;
    key[1] = pStringListID;
    return rowsExists(conn, "SKEWED_COL_VALUE_LOC_MAP_N49", key);
  }
};
#endif //EPIPE_SKEWEDLOCTABLE_H
