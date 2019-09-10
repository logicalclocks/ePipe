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
