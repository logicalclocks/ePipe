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

#ifndef SERDESTAILER_H
#define SERDESTAILER_H

#include "TableTailer.h"
#include "tables/hive/SERDESTable.h"
#include "tables/hive/SERDEPARAMSTable.h"
#include "tables/hive/SCHEMAVERSIONTable.h"

class SERDESTailer : public TableTailer<SERDESRow> {
public:
  SERDESTailer(Ndb *ndb, const int poll_maxTimeToWait, const Barrier barrier)
      : TableTailer(ndb, new SERDESTable(), poll_maxTimeToWait, barrier) {}

  virtual ~SERDESTailer() {}

private:
  SCHEMAVERSIONTable mSCHEMAVERSIONTable;
  SERDEPARAMSTable  mSERDEPARAMSTable;

  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, SERDESRow pre,
   SERDESRow row) {
    LOG_INFO("Delete SERDES event received. Primary Key value: " << pre.mSERDEID);
    mSCHEMAVERSIONTable.removeBySERDEID(mNdbConnection, pre.mSERDEID);
    mSERDEPARAMSTable.remove(mNdbConnection, pre.mSERDEID);
  }
};

#endif /* SERDESTAILER_H */
