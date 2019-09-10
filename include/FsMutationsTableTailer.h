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

#ifndef MUTATIONSTABLETAILER_H
#define MUTATIONSTABLETAILER_H

#include "RCTableTailer.h"
#include "tables/FsMutationsLogTable.h"

class FsMutationsTableTailer : public RCTableTailer<FsMutationRow> {
public:
  FsMutationsTableTailer(Ndb* ndb, Ndb* ndbRecovery, const int
  poll_maxTimeToWait, const Barrier barrier);
  FsMutationRow consume();
  virtual ~FsMutationsTableTailer();

private:
  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, FsMutationRow pre, FsMutationRow row);
  void barrierChanged();
  void pushToQueue(FSpq* curr);
  CFSq* mQueue;
  FSpq* mCurrentPriorityQueue;
  boost::mutex mLock;

  //    double mTimeTakenForEventsToArrive;
  //    long mNumOfEvents;
  //    int mPrintEveryNEvents;

};

#endif /* MUTATIONSTABLETAILER_H */

