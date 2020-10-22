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

#ifndef EPIPE_ISCHEMATABLE_H
#define EPIPE_ISCHEMATABLE_H

#include "tables/DBWatchTable.h"

struct ISCHEMARow {
  Int64 mSCHEMAID;
};

typedef std::vector<ISCHEMARow> ISCHEMAVec;
class ISCHEMATable  : public DBWatchTable<ISCHEMARow>{

public:
  ISCHEMATable() : DBWatchTable("I_SCHEMA"){
    addColumn("SCHEMA_ID");
    addWatchEvent(NdbDictionary::Event::TE_DELETE);
  }

  ISCHEMARow getRow(NdbRecAttr* value[]){
    ISCHEMARow row;
    row.mSCHEMAID = value[0]->int64_value();
    return row;
  }
};
#endif //EPIPE_ISCHEMATABLE_H
