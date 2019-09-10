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

#ifndef EPIPE_TBLSTABLE_H
#define EPIPE_TBLSTABLE_H

#include "tables/DBWatchTable.h"

struct TBLSRow {
  Int64 mTBLID;
  Int64 mSDID;
};

class TBLSTable : public DBWatchTable<TBLSRow> {

public:
  TBLSTable() : DBWatchTable("TBLS") {
    addColumn("TBL_ID");
    addColumn("SD_ID");
    addColumn("VIEW_EXPANDED_TEXT");
    addColumn("VIEW_ORIGINAL_TEXT");
    addWatchEvent(NdbDictionary::Event::TE_DELETE);
  }

  TBLSRow getRow(NdbRecAttr *value[]) {
    TBLSRow row;
    row.mTBLID = value[0]->int64_value();
    row.mSDID = value[1]->int64_value();
    return row;
  }
};

#endif //EPIPE_TBLSTABLE_H
