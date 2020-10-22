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

#ifndef SDSTAILER_H
#define SDSTAILER_H

#include "TableTailer.h"
#include "tables/hive/SDSTable.h"
#include "tables/hive/CDSTable.h"
#include "tables/hive/SERDESTable.h"
#include "tables/hive/SDPARAMSTable.h"
#include "tables/hive/TBLSTable.h"

class SDSTailer : public TableTailer<SDSRow> {
public:
  SDSTailer(Ndb *ndb, const int poll_maxTimeToWait, const Barrier barrier)
      : TableTailer(ndb, new SDSTable(), poll_maxTimeToWait, barrier) {}

  virtual ~SDSTailer() {}

private:
  SDSTable mSDSTable;
  CDSTable mCDSTable;
  SERDESTable mSERDESTable;
  TBLSTable mTBLSTable;
  SDPARAMSTable mSDPARAMSTable;

  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, SDSRow
  pre, SDSRow row) {
    LOG_INFO("Delete SDS event received. Primary Key value: " << pre.mSDID);

    if (!mSDSTable.hasCDID(mNdbConnection, pre.mCDID)) {
      mCDSTable.remove(mNdbConnection, pre.mCDID);
    }

    // SDS is defined as dependent in package.jdo.
    // This means that when the SD is deleted, also the SERDE entry goes away.
    // There is no possibility of duplicates.
    mSERDESTable.remove(mNdbConnection, pre.mSERDEID);

    mTBLSTable.removeBySDID(mNdbConnection, pre.mSDID);
    mSDPARAMSTable.remove(mNdbConnection, pre.mSDID);
  }
};

#endif /* SDSTAILER_H */
