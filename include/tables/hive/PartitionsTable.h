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

#ifndef EPIPE_PARTITIONSTABLE_H
#define EPIPE_PARTITIONSTABLE_H

#include "tables/DBWatchTable.h"

struct PartitionsRow{
  Int64 mPARTID;
  Int64 mSDID;
};

class PartitionsTable : public DBWatchTable<PartitionsRow> {
public:
  PartitionsTable() : DBWatchTable("PARTITIONS"){
    addColumn("PART_ID");
    addColumn("SD_ID");
    addWatchEvent(NdbDictionary::Event::TE_DELETE);
  }

  PartitionsRow getRow(NdbRecAttr *value[]){
    PartitionsRow row;
    row.mPARTID = value[0]->int64_value();
    row.mSDID = value[1]->int64_value();
    return row;
  }

};
#endif //EPIPE_PARTITIONSTABLE_H
