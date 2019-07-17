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
 * File:   SkewedValuesTable.h
 * Author: Mahmoud Ismail<mahmoud@logicalclocks.com>
 *
 */

#ifndef EPIPE_SKEWEDVALUESTABLE_H
#define EPIPE_SKEWEDVALUESTABLE_H

#include "tables/DBWatchTable.h"

struct SkewedValuesRow {
  Int64 mSDID;
  Int32 mIntegerIDX;
  Int64 mStringListID;
};

class SkewedValuesTable : public DBWatchTable<SkewedValuesRow> {
public:
  SkewedValuesTable() : DBWatchTable("SKEWED_VALUES") {
    addColumn("SD_ID_OID");
    addColumn("INTEGER_IDX");
    addColumn("STRING_LIST_ID_EID");
    addWatchEvent(NdbDictionary::Event::TE_DELETE);
  }

  SkewedValuesRow getRow(NdbRecAttr *value[]) {
    SkewedValuesRow row;
    row.mSDID = value[0]->int64_value();
    row.mIntegerIDX = value[1]->int32_value();
    row.mStringListID = value[2]->int64_value();
    return row;
  }

  bool hasStringListID(Ndb* conn, Int64 pStringListID){
    AnyMap key;
    key[2] = pStringListID;
    return rowsExists(conn, "SKEWED_VALUES_N49", key);
  }

};

#endif //EPIPE_SKEWEDVALUESTABLE_H
