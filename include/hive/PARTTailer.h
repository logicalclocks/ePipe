/*
 * Copyright (C) 2019 Logical Clocks AB
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

/*
 * File:   PARTTailer.h
 * Author: Fabio Buso <buso@kth.se>
 *
 */

#ifndef PARTTAILER_H
#define PARTTAILER_H

#include "TableTailer.h"
#include "tables/hive/PartitionsTable.h"
#include "tables/hive/SDSTable.h"

class PARTTailer : public TableTailer<PartitionsRow> {
public:
  PARTTailer(Ndb *ndb, const int poll_maxTimeToWait, const Barrier barrier)
      : TableTailer(ndb, new PartitionsTable(), poll_maxTimeToWait, barrier) {}

  virtual ~PARTTailer() {}

private:
  SDSTable mSDSTable;

  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType,
                           PartitionsRow pre, PartitionsRow row) {
    LOG_INFO("Delete PART event received. Primary Key value: " << pre.mPARTID);
    mSDSTable.remove(mNdbConnection, pre.mSDID);
  }
};

#endif /* PARTTAILER_H */
