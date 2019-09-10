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

#ifndef PROVENANCETABLETAILER_H
#define PROVENANCETABLETAILER_H

#include "RCTableTailer.h"
#include "tables/ProvenanceLogTable.h"

class ProvenanceTableTailer : public RCTableTailer<ProvenanceRow> {
public:
  ProvenanceTableTailer(Ndb* ndb, Ndb* ndbRecovery, const int
  poll_maxTimeToWait, const Barrier barrier);
  ProvenanceRow consume();
  virtual ~ProvenanceTableTailer();

private:
  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, ProvenanceRow pre, ProvenanceRow row);
  void barrierChanged();

  void pushToQueue(PRpq* curr);

  CPRq *mQueue;
  PRpq* mCurrentPriorityQueue;
  boost::mutex mLock;

};


#endif //PROVENANCETABLETAILER_H
