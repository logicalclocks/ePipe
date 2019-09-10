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

#ifndef SKEWEDVALUESTAILER_H
#define SKEWEDVALUESTAILER_H

#include "TableTailer.h"
#include "tables/hive/SkewedLocTable.h"
#include "tables/hive/SkewedValuesTable.h"
#include "tables/hive/SkewedStringsTable.h"

class SkewedValuesTailer: public TableTailer<SkewedValuesRow>{
public:
    SkewedValuesTailer(Ndb* ndb, const int poll_maxTimeToWait, const Barrier
    barrier) : TableTailer(ndb, new SkewedValuesTable(), poll_maxTimeToWait,
        barrier) {}
    virtual ~SkewedValuesTailer() {}
private:
  SkewedLocTable mSkewedLocTable;
  SkewedValuesTable mSkewedValuesTable;
  SkewedStringsTable mSkewedStringsTable;

  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType,
      SkewedValuesRow pre, SkewedValuesRow row) {
    if(!mSkewedValuesTable.hasStringListID(mNdbConnection, pre.mStringListID)){
      mSkewedStringsTable.remove(mNdbConnection, pre.mStringListID);
    }
  }
};

#endif /* SKEWEDVALUESTAILER_H */
