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
