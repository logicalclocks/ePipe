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
 * File:   SkewedStringsTable.h
 * Author: Mahmoud Ismail<mahmoud@logicalclocks.com>
 *
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
    start(conn);
    doDelete(pStringListID);
    LOG_DEBUG("Remove SkewedStrings entry with PK: " << pStringListID);
    end();
  }
};

#endif //EPIPE_SKEWEDSTRINGSTABLE_H
