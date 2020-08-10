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

#ifndef EPIPE_SDPARAMSTABLE_H
#define EPIPE_SDPARAMSTABLE_H

#include "tables/DBTable.h"

struct SDPARAMSRow {
  Int64 mSDID;
};

class SDPARAMSTable : public DBTable<SDPARAMSRow> {

public:
  SDPARAMSTable() : DBTable("SD_PARAMS") {
    addColumn("SD_ID");
  }

  SDPARAMSRow getRow(NdbRecAttr *value[]) {
    SDPARAMSRow row;
    row.mSDID = value[0]->int64_value();
    return row;
  }

  void remove(Ndb *conn, Int64 SDID)
  {
    AnyMap key;
    key[0] = SDID;
    int count = deleteByIndex(conn, "SD_PARAMS_N49", key);
    LOG_INFO("Removed " << count << " entries of SD_PARAMS for SD_ID: " << SDID);
  }
};

#endif //EPIPE_SDPARAMSTABLE_H
