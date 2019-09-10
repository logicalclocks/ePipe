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

#ifndef EPIPE_SKEWEDSTRINGSTABLE_H
#define EPIPE_SKEWEDSTRINGSTABLE_H

#include "tables/DBTable.h"

struct SkewedStringsRow {
  Int64 mStringListID;
};

class SkewedStringsTable : public DBTable<SkewedStringsRow> {
public:
  SkewedStringsTable() : DBTable("SKEWED_STRING_LIST") {
    addColumn("STRING_LIST_ID");
  }

  SkewedStringsRow getRow(NdbRecAttr *value[]) {
    SkewedStringsRow row;
    row.mStringListID = value[0]->int64_value();
    return row;
  }

  void remove(Ndb* conn, Int64 pStringListID) {
    if(hasSkewedStringList(conn, pStringListID)) {
      start(conn);
      doDelete(pStringListID);
      LOG_DEBUG("Remove SkewedStrings entry with PK: " << pStringListID);
      end();
    }
  }

  bool hasSkewedStringList(Ndb *conn, Int64 pStringListID){
    AnyMap key;
    key[0] = pStringListID;
    return rowsExists(conn, "PRIMARY", key);
  }
};

#endif //EPIPE_SKEWEDSTRINGSTABLE_H
