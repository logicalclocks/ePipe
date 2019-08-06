/*
 * Copyright (C) 2018 Logical Clocks AB
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
 * File:   ProvenanceTableTailer.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef PROVENANCETABLETAILER_H
#define PROVENANCETABLETAILER_H

#include "RCTableTailer.h"
#include "tables/ProvenanceLogTable.h"

class ProvenanceTableTailer : public RCTableTailer<ProvenanceRow> {
public:
  ProvenanceTableTailer(Ndb* ndb, const int poll_maxTimeToWait, const Barrier barrier);
  ProvenanceRow consume();
  virtual ~ProvenanceTableTailer();

private:
  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, ProvenanceRow pre, ProvenanceRow row);
  void barrierChanged();

  void recover();

  void pushToQueue(PRpq* curr);
  void pushToQueue(Pv* curr);

  CPRq *mQueue;
  PRpq* mCurrentPriorityQueue;
  boost::mutex mLock;

};


#endif //PROVENANCETABLETAILER_H
