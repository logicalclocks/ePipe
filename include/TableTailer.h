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
 * File:   TableTailer.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef TABLETAILER_H
#define TABLETAILER_H

#include "Utils.h"
#include "tables/DBWatchTable.h"

enum Barrier {
  EPOCH = 0,
  GCI = 1
};

template<typename TableRow>
class TableTailer {
public:
  TableTailer(Ndb* ndb, DBWatchTable<TableRow>* table, const int poll_maxTimeToWait, const Barrier mBarrier);

  void start(bool recovery);
  void waitToFinish();
  virtual ~TableTailer();

protected:
  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, TableRow pre, TableRow row) = 0;
  virtual void barrierChanged();
  virtual void recover();

  Ndb* mNdbConnection;

private:
  void createListenerEvent();
  void removeListenerEvent();
  void waitForEvents();
  void run();
  const char* getEventName(NdbDictionary::Event::TableEvent event);
  Uint64 getGCI(Uint64 epoch);
  void checkIfBarrierReached(Uint64 epoch);

  bool mStarted;
  boost::thread mThread;

  const string mEventName;
  DBWatchTable<TableRow>* mTable;
  const int mPollMaxTimeToWait;
  const Barrier mBarrier;

  Uint64 mLastReportedBarrier;
};

template<typename TableRow>
TableTailer<TableRow>::TableTailer(Ndb* ndb, DBWatchTable<TableRow>* table, const int poll_maxTimeToWait, const Barrier barrier) : mNdbConnection(ndb), mStarted(false),
mEventName(Utils::concat("tail-", table->getName())), mTable(table), mPollMaxTimeToWait(poll_maxTimeToWait), mBarrier(barrier),
mLastReportedBarrier(0) {
}

template<typename TableRow>
void TableTailer<TableRow>::start(bool recovery) {
  if (mStarted) {
    return;
  }

  if (recovery) {
    LOG_INFO("start with recovery for " << mTable->getName());
    recover();
  }

  createListenerEvent();
  mThread = boost::thread(&TableTailer::run, this);
  mStarted = true;
}

template<typename TableRow>
void TableTailer<TableRow>::recover() {
  mTable->getAllSortedByRecoveryIndex(mNdbConnection);
  while (mTable->next()) {
    TableRow row = mTable->currRow();
    handleEvent(NdbDictionary::Event::TE_INSERT, row, row);
  }
}

template<typename TableRow>
void TableTailer<TableRow>::waitToFinish() {
  if (mStarted) {
    mThread.join();
  }
}

template<typename TableRow>
void TableTailer<TableRow>::run() {
  try {
    waitForEvents();
  } catch (boost::thread_interrupted&) {
    LOG_ERROR("Thread is stopped");
    return;
  }
}

template<typename TableRow>
void TableTailer<TableRow>::createListenerEvent() {
  NdbDictionary::Dictionary *myDict = mNdbConnection->getDictionary();
  if (!myDict) LOG_NDB_API_ERROR(mNdbConnection->getNdbError());

  const NdbDictionary::Table *table = myDict->getTable(mTable->getName().c_str());
  if (!table) LOG_NDB_API_ERROR(myDict->getNdbError());

  NdbDictionary::Event myEvent(mEventName.c_str(), *table);

  for (int i = 0; i < mTable->getNoEvents(); i++) {
    myEvent.addTableEvent(mTable->getEvent(i));
  }

  myEvent.addEventColumns(mTable->getNoColumns(), mTable->getColumns());
  //myEvent.mergeEvents(merge_events);

  // Add event to database
  if (myDict->createEvent(myEvent) == 0)
    myEvent.print();
  else if (myDict->getNdbError().classification ==
          NdbError::SchemaObjectExists) {
    LOG_DEBUG("Event creation failed, event exists, dropping Event...");
    if (myDict->dropEvent(mEventName.c_str())) LOG_NDB_API_ERROR(myDict->getNdbError());
    // try again
    // Add event to database
    if (myDict->createEvent(myEvent)) LOG_NDB_API_ERROR(myDict->getNdbError());
  } else
    LOG_NDB_API_ERROR(myDict->getNdbError());
}

template<typename TableRow>
void TableTailer<TableRow>::removeListenerEvent() {
  NdbDictionary::Dictionary *myDict = mNdbConnection->getDictionary();
  if (!myDict) LOG_NDB_API_ERROR(mNdbConnection->getNdbError());
  // remove event from database
  if (myDict->dropEvent(mEventName.c_str())) LOG_NDB_API_ERROR(myDict->getNdbError());
}

template<typename TableRow>
void TableTailer<TableRow>::waitForEvents() {
  NdbEventOperation* op;
  LOG_INFO("create EventOperation for [" << mEventName << "]");
  if ((op = mNdbConnection->createEventOperation(mEventName.c_str())) == NULL)
    LOG_NDB_API_ERROR(mNdbConnection->getNdbError());

  NdbRecAttr * recAttr[mTable->getNoColumns()];
  NdbRecAttr * recAttrPre[mTable->getNoColumns()];

  // primary keys should always be a part of the result
  for (int i = 0; i < mTable->getNoColumns(); i++) {
    recAttr[i] = op->getValue(mTable->getColumn(i).c_str());
    recAttrPre[i] = op->getPreValue(mTable->getColumn(i).c_str());
  }

  LOG_INFO("Execute");
  // This starts changes to "start flowing"
  if (op->execute())
    LOG_NDB_API_ERROR(op->getNdbError());
  while (true) {
    int r = mNdbConnection->pollEvents2(mPollMaxTimeToWait);
    if (r > 0) {
      while ((op = mNdbConnection->nextEvent2())) {
        NdbDictionary::Event::TableEvent event = op->getEventType2();

        if (event != NdbDictionary::Event::TE_EMPTY) {
          LOG_TRACE("Got Event [" << event << "," << getEventName(event) << "] Epoch " << op->getEpoch() << " GCI " << getGCI(op->getEpoch()));
        }
        switch (event) {
          case NdbDictionary::Event::TE_INSERT:
          case NdbDictionary::Event::TE_DELETE:
          case NdbDictionary::Event::TE_UPDATE:
          {
            checkIfBarrierReached(op->getEpoch());

            handleEvent(event, mTable->getRow(recAttrPre), mTable->getRow(recAttr));
            break;
          }
          default:
            break;
        }

      }
    }
    //        boost::this_thread::sleep(boost::posix_time::milliseconds(mPollMaxTimeToWait));
    checkIfBarrierReached(mNdbConnection->getHighestQueuedEpoch());
  }

}

template<typename TableRow>
const char* TableTailer<TableRow>::getEventName(NdbDictionary::Event::TableEvent event) {
  switch (event) {
    case NdbDictionary::Event::TE_INSERT:
      return "INSERT";
    case NdbDictionary::Event::TE_DELETE:
      return "DELETE";
    case NdbDictionary::Event::TE_UPDATE:
      return "UPDATE";
    case NdbDictionary::Event::TE_DROP:
      return "DROP";
    case NdbDictionary::Event::TE_ALTER:
      return "ALTER";
    case NdbDictionary::Event::TE_CREATE:
      return "CREATE";
    case NdbDictionary::Event::TE_GCP_COMPLETE:
      return "GCP_COMPLETE";
    case NdbDictionary::Event::TE_CLUSTER_FAILURE:
      return "CLUSTER_FAILURE";
    case NdbDictionary::Event::TE_STOP:
      return "STOP";
    case NdbDictionary::Event::TE_NODE_FAILURE:
      return "NODE_FAILURE";
    case NdbDictionary::Event::TE_SUBSCRIBE:
      return "SUBSCRIBE";
    case NdbDictionary::Event::TE_UNSUBSCRIBE:
      return "UNSUBSCRIBE";
    case NdbDictionary::Event::TE_EMPTY:
      return "EMPTY";
    case NdbDictionary::Event::TE_INCONSISTENT:
      return "INCONSISTENT";
    case NdbDictionary::Event::TE_OUT_OF_MEMORY:
      return "OUT_OF_MEMORY";
    case NdbDictionary::Event::TE_ALL:
      return "ALL";
  }
  return "UNKOWN";
}

template<typename TableRow>
void TableTailer<TableRow>::barrierChanged() {
  //do nothing
}

template<typename TableRow>
Uint64 TableTailer<TableRow>::getGCI(Uint64 epoch) {
  return (epoch & 0xffffffff00000000) >> 32;
}

template<typename TableRow>
void TableTailer<TableRow>::checkIfBarrierReached(Uint64 epoch) {
  Uint64 currentBarrier = 0;
  if (mBarrier == EPOCH) {
    currentBarrier = epoch;
  } else if (mBarrier == GCI) {
    currentBarrier = getGCI(epoch);
  }

  if (mLastReportedBarrier == 0) {
    mLastReportedBarrier = currentBarrier;
  } else if (mLastReportedBarrier != currentBarrier) {
    barrierChanged();
    mLastReportedBarrier = currentBarrier;
    LOG_TRACE("************************** NEW BARRIER [" << currentBarrier << "] ************ ");
  }
}

template<typename TableRow>
TableTailer<TableRow>::~TableTailer() {
  delete mNdbConnection;
}
#endif /* TABLETAILER_H */

