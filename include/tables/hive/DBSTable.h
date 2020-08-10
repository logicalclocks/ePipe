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

#ifndef EPIPE_DBSTABLE_H
#define EPIPE_DBSTABLE_H

#include "tables/DBWatchTable.h"

struct DBSRow {
  Int64 mDBID;
};

typedef std::vector<DBSRow> DBSVec;
class DBSTable  : public DBWatchTable<DBSRow>{

public:
  DBSTable() : DBWatchTable("DBS"){
    addColumn("DB_ID");
    addWatchEvent(NdbDictionary::Event::TE_DELETE);
  }

  DBSRow getRow(NdbRecAttr* value[]){
    DBSRow row;
    row.mDBID = value[0]->int64_value();
    return row;
  }
};
#endif //EPIPE_DBSTABLE_H
