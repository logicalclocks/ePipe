/*
 * Copyright (C) 2016 Hops.io
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
 * File:   MutationsTableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "FsMutationsTableTailer.h"
#include "Utils.h"

//const static ptime EPOCH_TIME(boost::gregorian::date(1970,1,1)); 

FsMutationsTableTailer::FsMutationsTableTailer(Ndb* ndb, Ndb* ndbRecovery,
    const int poll_maxTimeToWait, const Barrier barrier) : RCTableTailer(ndb,
        ndbRecovery, new FsMutationsLogTable(), poll_maxTimeToWait, barrier) {
  mQueue = new CFSq();
  mCurrentPriorityQueue = new FSpq();
  //    mTimeTakenForEventsToArrive = 0;
  //    mNumOfEvents = 0;
  //    mPrintEveryNEvents = 0;
}

void FsMutationsTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, FsMutationRow pre, FsMutationRow row) {
  mLock.lock();
  mCurrentPriorityQueue->push(row);
  int size = mCurrentPriorityQueue->size();
  mLock.unlock();

  LOG_DEBUG("push inode [" << row.getINodeName() << "] to queue[" << size <<
  "], Op [" << FsOpTypeToStr(row.mOperation) << "]");
  
  //    ptime t = EPOCH_TIME + boost::posix_time::milliseconds(row.mTimestamp);
  //    mTimeTakenForEventsToArrive += Utils::getTimeDiffInMilliseconds(t, row.mEventCreationTime);
  //    mNumOfEvents++;
  //    mPrintEveryNEvents++;
  //    if(mPrintEveryNEvents>=10000){
  //        double avgArrival = mTimeTakenForEventsToArrive / mNumOfEvents;
  //        LOG_INFO("Average Arrival Time=" << avgArrival << " msec");
  //        mPrintEveryNEvents = 0;
  //    }
}

void FsMutationsTableTailer::barrierChanged() {
  FSpq* pq = NULL;
  mLock.lock();
  if (!mCurrentPriorityQueue->empty()) {
    pq = mCurrentPriorityQueue;
    mCurrentPriorityQueue = new FSpq();
  }
  mLock.unlock();

  if (pq != NULL) {
    LOG_TRACE("--------------------------------------NEW BARRIER (" << pq->size() << " events )------------------- ");
    pushToQueue(pq);
  }
}

FsMutationRow FsMutationsTableTailer::consume() {
  FsMutationRow row;
  mQueue->wait_and_pop(row);
  LOG_DEBUG(" pop inode [" << row.mInodeId << "] from queue \n" << row.to_string());
  return row;
}


void FsMutationsTableTailer::pushToQueue(FSpq* curr) {
  while (!curr->empty()) {
    mQueue->push(curr->top());
    curr->pop();
  }
  delete curr;
}

FsMutationsTableTailer::~FsMutationsTableTailer() {
  delete mQueue;
}

