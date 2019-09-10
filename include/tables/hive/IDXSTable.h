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

#ifndef EPIPE_IDXSTABLE_H
#define EPIPE_IDXSTABLE_H

#include "tables/DBWatchTable.h"

struct IDXSRow{
  Int64 mINDEXID;
  Int64 mSDID;
};

class IDXSTable : public DBWatchTable<IDXSRow> {
public:
  IDXSTable() : DBWatchTable("IDXS"){
    addColumn("INDEX_ID");
    addColumn("SD_ID");
    addWatchEvent(NdbDictionary::Event::TE_DELETE);
  }

  IDXSRow getRow(NdbRecAttr *value[]){
    IDXSRow row;
    row.mINDEXID = value[0]->int64_value();
    row.mSDID = value[1]->int64_value();
    return row;
  }

};

#endif //EPIPE_IDXSTABLE_H
