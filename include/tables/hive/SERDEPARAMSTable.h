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

#ifndef EPIPE_SERDEPARAMSTABLE_H
#define EPIPE_SERDEPARAMSTABLE_H

#include "tables/DBTable.h"

struct SERDEPARAMSRow {
  Int64 mSERDEID;
};

class SERDEPARAMSTable : public DBTable<SERDEPARAMSRow> {

public:
  SERDEPARAMSTable() : DBTable("SERDE_PARAMS") {
    addColumn("SERDE_ID");
  }

  SERDEPARAMSRow getRow(NdbRecAttr *value[]) {
    SERDEPARAMSRow row;
    row.mSERDEID = value[0]->int64_value();
    return row;
  }

  void remove(Ndb *conn, Int64 SERDEID)
  {
    AnyMap key;
    key[0] = SERDEID;
    int count = deleteByIndex(conn, "SERDE_PARAMS_N49", key);
    LOG_INFO("Removed " << count << " entries of SERDE_PARAMS for SERDE_ID: " << SERDEID);
  }
};

#endif //EPIPE_SERDEPARAMSTABLE_H
