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

#ifndef RCTABLETAILER_H
#define RCTABLETAILER_H

#include "TableTailer.h"

const int SINGLE_QUEUE = -1;

template<typename TableRow>
class RCTableTailer : public TableTailer<TableRow> {
public:

  RCTableTailer(Ndb* ndb, Ndb* ndbRecovery, DBWatchTable<TableRow>* table,
      const int poll_maxTimeToWait, const Barrier barrier)
  : TableTailer<TableRow>(ndb, ndbRecovery, table, poll_maxTimeToWait,
      barrier) {
  }

  virtual TableRow consumeMultiQueue(int queue_id) {
    //default behaviour is for single queue only
    return consume();
  }

  virtual TableRow consume() = 0;
};

#endif /* RCTABLETAILER_H */

