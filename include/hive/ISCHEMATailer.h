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

#ifndef ISCHEMATAILER_H
#define ISCHEMATAILER_H

#include "TableTailer.h"
#include "tables/hive/ISCHEMATable.h"
#include "tables/hive/SCHEMAVERSIONTable.h"

class ISCHEMATailer : public TableTailer<ISCHEMARow> {
public:
  ISCHEMATailer(Ndb *ndb, const int poll_maxTimeToWait, const Barrier barrier)
      : TableTailer(ndb, new ISCHEMATable(), poll_maxTimeToWait, barrier) {}

  virtual ~ISCHEMATailer() {}

private:
  ISCHEMATable mISCHEMATable;
  SCHEMAVERSIONTable mSCHEMAVERSIONTable;

  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, ISCHEMARow pre,
   ISCHEMARow row) {
    LOG_INFO("Delete I_SCHEMA event received. Primary Key value: " << pre.mSCHEMAID);
    mSCHEMAVERSIONTable.removeBySchemaID(mNdbConnection, pre.mSCHEMAID);
  }
};

#endif /* ISCHEMATAILER_H */
