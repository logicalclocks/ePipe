/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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

#ifndef TABLETAILER_H
#define TABLETAILER_H

#include "Utils.h"
#include "tables/DBWatchTable.h"
#include <mutex>
#include <condition_variable>

enum Barrier {
  EPOCH = 0,
  GCI = 1
};

template<typename TableRow>
class TableTailer {
public:
  TableTailer(Ndb* ndb, Ndb* recoveryNdb, DBWatchTable<TableRow>* table, const
  int poll_maxTimeToWait, const Barrier barrier);

  TableTailer(Ndb* ndb, DBWatchTable<TableRow>* table, const
  int poll_maxTimeToWait, const Barrier barrier);

  void start();
  void waitToFinish();
  virtual ~TableTailer();

protected:
  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, TableRow pre, TableRow row) = 0;
  virtual void barrierChanged();

  Ndb* mNdbConnection;

private:
  void createListenerEvent();
  void removeListenerEvent();
  void waitForEvents();
  void run();
  void recover();
  const char* getEventName(NdbDictionary::Event::TableEvent event);
  Uint64 getGCI(Uint64 epoch);
  void checkIfBarrierReached(Uint64 epoch);
  bool deferEvent(Uint64 epoch, NdbDictionary::Event::TableEvent event,
      TableRow pre, TableRow row);
  void processEvent(Uint64 epoch, NdbDictionary::Event::TableEvent event,
      TableRow pre, TableRow row);
  int processDeferredEvents();

  bool mStarted;
  boost::thread mThread;
  boost::thread mRecoveryThread;

  const std::string mEventName;
  DBWatchTable<TableRow>* mTable;
  const int mPollMaxTimeToWait;
  const Barrier mBarrier;

  Uint64 mLastReportedBarrier;

  Ndb* mNdbRecoveryConnection;
  bool mUnderRecovery;

  Uint64 mFirstEpochToWatch;
  std::mutex mFirstEpochMutex;
  std::condition_variable mFirstEpochCond;

  struct DeferredEvent{
    NdbDictionary::Event::TableEvent mEventType;
    TableRow mPre;
    TableRow mRow;
  };

  bool mStartProcessingDeferredEvents;
  Uint64 mLastEpochInRecovery;

  boost::unordered_map<Uint64, std::queue<DeferredEvent>> mEventsDuringRecovery;
  boost::unordered_set<std::string> mEventsPKDuringRecovery;
  boost::unordered_set<Uint64> mEpochsDuringRecovery;

  const char* getEventState(NdbEventOperation::State state);

};

template<typename TableRow>
TableTailer<TableRow>::TableTailer(Ndb* ndb, Ndb* recoveryNdb, DBWatchTable<TableRow>* table,
    const int poll_maxTimeToWait, const Barrier barrier) : mNdbConnection(ndb), mStarted(false),
mEventName(Utils::concat("tail-", table->getName())), mTable(table),
mPollMaxTimeToWait(poll_maxTimeToWait), mBarrier(barrier),
mLastReportedBarrier(0), mNdbRecoveryConnection(recoveryNdb), mUnderRecovery(false),
    mFirstEpochToWatch(0), mStartProcessingDeferredEvents(false),
    mLastEpochInRecovery(0) {
}

template<typename TableRow>
TableTailer<TableRow>::TableTailer(Ndb* ndb, DBWatchTable<TableRow>* table, const
int poll_maxTimeToWait, const Barrier barrier) : TableTailer(ndb, nullptr, table, poll_maxTimeToWait, barrier){
  
}

template<typename TableRow>
void TableTailer<TableRow>::start() {
  if (mStarted) {
    return;
  }

  mUnderRecovery = mNdbRecoveryConnection != nullptr;
  createListenerEvent();
  mThread = boost::thread(&TableTailer::run, this);

  if(mUnderRecovery) {
    mRecoveryThread = boost::thread(&TableTailer::recover, this);
    LOG_INFO("start with recovery for " << mTable->getName());
  }else{
    LOG_INFO("start without recovery for " << mTable->getName());
  }

  mStarted = true;
}

template<typename TableRow>
void TableTailer<TableRow>::recover() {
  std::unique_lock<std::mutex> lk(mFirstEpochMutex);
  LOG_DEBUG("Waiting for the firstEpoch to start recovery for " << mTable->getName());
  mFirstEpochCond.wait(lk, [this]{return mFirstEpochToWatch != 0;});
  LOG_DEBUG(mTable->getName() << " recovery started for events before epoch "
  << mFirstEpochToWatch);

  ptime t1 = Utils::getCurrentTime();

  EpochsRowsMap<TableRow> re = mTable->getAllForRecovery
      (mNdbRecoveryConnection);

  int eventsApplied=0;
  int eventsToAdd=0;
  int alreadyExistsingEvents=0;

  for (std::vector<Uint64>::iterator it = re.mEpochs->begin(); it !=
      re.mEpochs->end(); it++) {
    Uint64 epoch = *it;
    std::queue<TableRow>* rows = re.mRowsByEpoch->at(epoch);
    if(epoch >= mFirstEpochToWatch){
      while(!rows->empty()){
        TableRow row = rows->front();
        //event is not in our deferred queue
        if(deferEvent(epoch, NdbDictionary::Event::TE_INSERT, row, row)){
          eventsToAdd++;
        }else{
          alreadyExistsingEvents++;
        }
        rows->pop();
      }
    }else{
      while(!rows->empty()){
        TableRow row = rows->front();
        deferEvent(epoch, NdbDictionary::Event::TE_INSERT, row, row);
        rows->pop();
        eventsApplied++;
      }
    }
    delete rows;
  }

  delete re.mEpochs;
  delete re.mRowsByEpoch;

  mStartProcessingDeferredEvents = true;

  ptime t2 = Utils::getCurrentTime();
  LOG_INFO(mTable->getName() << " recovery done in " <<
  Utils::getTimeDiffInMilliseconds(t1, t2) << " msec : " << eventsApplied
  << " old events recovered, " << alreadyExistsingEvents
  << " already captured events, " << eventsToAdd
  << " concurrent events during recovery, " <<
  (eventsApplied + eventsToAdd)
  << " total number of events added to deferred events.");
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
  if (!myDict) LOG_NDB_API_FATAL(mTable->getName(), mNdbConnection->getNdbError());

  const NdbDictionary::Table *table = myDict->getTable(mTable->getName().c_str());
  if (!table) LOG_NDB_API_FATAL(mTable->getName(), myDict->getNdbError());

  NdbDictionary::Event myEvent(mEventName.c_str(), *table);

  for (evtvec_size_type i = 0; i < mTable->getNoEvents(); i++) {
    myEvent.addTableEvent(mTable->getEvent(i));
  }

  myEvent.addEventColumns(mTable->getNoColumns(), mTable->getColumns());
  //myEvent.mergeEvents(merge_events);

  // Add event to database
  if (myDict->createEvent(myEvent) == 0) {
    myEvent.print();
  } else if (myDict->getNdbError().classification ==
             NdbError::SchemaObjectExists) {
    LOG_DEBUG("Event creation failed, event exists, dropping Event...");
    if (myDict->dropEvent(mEventName.c_str(), 1)) {
      LOG_NDB_API_FATAL(mTable->getName(), myDict->getNdbError());
    }
    // try again
    // Add event to database
    if (myDict->createEvent(myEvent)) {
      LOG_NDB_API_FATAL(mTable->getName(), myDict->getNdbError());
    }
  } else {
    LOG_NDB_API_FATAL(mTable->getName(), myDict->getNdbError());
  }
}

template<typename TableRow>
void TableTailer<TableRow>::removeListenerEvent() {
  NdbDictionary::Dictionary *myDict = mNdbConnection->getDictionary();
  if (!myDict) LOG_NDB_API_FATAL(mTable->getName(), mNdbConnection->getNdbError());
  // remove event from database
  if (myDict->dropEvent(mEventName.c_str())) LOG_NDB_API_FATAL(mTable->getName(), myDict->getNdbError());
}

template<typename TableRow>
void TableTailer<TableRow>::waitForEvents() {
  NdbDictionary::Dictionary *myDict = mNdbConnection->getDictionary();
  NdbEventOperation* op;
  LOG_INFO("create EventOperation for [" << mEventName << "]");
  if ((op = mNdbConnection->createEventOperation(mEventName.c_str())) == NULL)
    LOG_NDB_API_FATAL(mTable->getName(), mNdbConnection->getNdbError());

  NdbRecAttr * recAttr[mTable->getNoColumns()];
  NdbRecAttr * recAttrPre[mTable->getNoColumns()];

  // primary keys should always be a part of the result
  for (strvec_size_type i = 0; i < mTable->getNoColumns(); i++) {
    recAttr[i] = op->getValue(mTable->getColumn(i).c_str());
    recAttrPre[i] = op->getPreValue(mTable->getColumn(i).c_str());
  }

  LOG_INFO("Execute");
  ptime lastConnectionCheckTime = Utils::getCurrentTime();
  // This starts changes to "start flowing"
  if (op->execute())
    LOG_NDB_API_FATAL(mTable->getName(), op->getNdbError());
  while (true) {
    LOG_TRACE("xxx --- pollEvents " << getEventState(op->getState()));
    int r = mNdbConnection->pollEvents2(mPollMaxTimeToWait);
    LOG_TRACE("xxx --- got events "<< r << " " << getEventState(op->getState()));

    if (mFirstEpochToWatch == 0) {
      std::unique_lock<std::mutex> lk(mFirstEpochMutex);
      mFirstEpochToWatch = mNdbConnection->getHighestQueuedEpoch();
      lk.unlock();
      LOG_DEBUG(mTable->getName() << " firstEpoch to watch "
                                  << mFirstEpochToWatch);
      mFirstEpochCond.notify_all();
    }

    if (mStartProcessingDeferredEvents) {
      if(mLastEpochInRecovery == 0 && !mEpochsDuringRecovery.empty()){
        std::vector<Uint64> orderedEpochs;
        orderedEpochs.insert(orderedEpochs.end(), mEpochsDuringRecovery.begin(), mEpochsDuringRecovery.end());
        std::sort(orderedEpochs.begin(), orderedEpochs.end());
        mLastEpochInRecovery = orderedEpochs[orderedEpochs.size() - 1];
      }

      if(mNdbConnection->getHighestQueuedEpoch() > mLastEpochInRecovery) {
        ptime t1 = Utils::getCurrentTime();
        int deferredEventsProcessed = processDeferredEvents();
        ptime t2 = Utils::getCurrentTime();
        LOG_INFO(mTable->getName()
                     << " processing deferred events and recovered events in "
                     << Utils::getTimeDiffInMilliseconds(t1, t2)
                     << " msec " << deferredEventsProcessed
                     << " events processed");
      }else{
        LOG_INFO(mTable->getName()
        << " defer events since the seen epoch during recovery (" <<
        mLastEpochInRecovery << ") is higher than the current received epoch ("
        << mNdbConnection->getHighestQueuedEpoch() << ")");
      }
    }

    if (r > 0) {
      while ((op = mNdbConnection->nextEvent2())) {
        NdbDictionary::Event::TableEvent event = op->getEventType2();

        if (event != NdbDictionary::Event::TE_EMPTY) {
          LOG_TRACE("Got Event [" << event << "," << getEventName(event) << "] Epoch " << op->getEpoch() << " GCI " << getGCI(op->getEpoch()));
        }
        switch (event) {
          case NdbDictionary::Event::TE_INSERT:
          case NdbDictionary::Event::TE_DELETE:
          case NdbDictionary::Event::TE_UPDATE: {

            if (mUnderRecovery) {
              deferEvent(op->getEpoch(), event, mTable->getRow(recAttrPre),
                  mTable->getRow(recAttr));
            } else {
              processEvent(op->getEpoch(), event, mTable->getRow(recAttrPre),
                  mTable->getRow(recAttr));
            }
            break;
          }
          default:
            break;
        }

      }
    } /*else if (r == 0){
      // no events so try to connect to database to ensure it is alive
      ptime curr = Utils::getCurrentTime(); 
      if(Utils::getTimeDiffInMilliseconds(lastConnectionCheckTime, curr) > (mPollMaxTimeToWait * 5)){
        NdbDictionary::Dictionary::List myList;
        if(myDict->listIndexes(myList, mTable->getName().c_str())){
          LOG_NDB_API_FATAL(mTable->getName(), myDict->getNdbError());
        }
        LOG_INFO("XXX -- got list of indexes -  " << myList.count << " for " << mTable->getName());
        lastConnectionCheckTime = curr;
      }
    } else {
      LOG_NDB_API_FATAL(mTable->getName(), op->getNdbError());
    }*/
    //        boost::this_thread::sleep(boost::posix_time::milliseconds(mPollMaxTimeToWait));
    checkIfBarrierReached(mNdbConnection->getHighestQueuedEpoch());
  }

}

template<typename TableRow>
const char* TableTailer<TableRow>::getEventState(NdbEventOperation::State state) {
  switch (state) {
    case NdbEventOperation::State::EO_CREATED:
      return "EO_CREATED";
    case NdbEventOperation::State::EO_EXECUTING:
      return "EO_EXECUTING";
    case NdbEventOperation::State::EO_DROPPED:
      return "EO_EXECUTING";
    case NdbEventOperation::State::EO_ERROR:
      return "EO_ERROR";
  }
  return "UNKOWN";
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
  } else if (currentBarrier > mLastReportedBarrier) {
    barrierChanged();
    mLastReportedBarrier = currentBarrier;
    LOG_TRACE("************************** NEW BARRIER [" << currentBarrier << "] ************ ");
  }
}

template<typename TableRow>
bool TableTailer<TableRow>::deferEvent(Uint64 epoch,
    NdbDictionary::Event::TableEvent event, TableRow pre, TableRow row) {
  //event already exists
  if(mEventsPKDuringRecovery.find(mTable->getPKStr(row)) !=
     mEventsPKDuringRecovery.end()){
    return false;
  }

  if(mEventsDuringRecovery.find(epoch) == mEventsDuringRecovery
      .end()){
    mEventsDuringRecovery[epoch] = std::queue<DeferredEvent>();
  }

  mEventsDuringRecovery[epoch].push({event, pre, row});
  mEventsPKDuringRecovery.insert(mTable->getPKStr(row));
  mEpochsDuringRecovery.insert(epoch);
  return true;
}

template<typename TableRow>
void TableTailer<TableRow>::processEvent(Uint64 epoch,
    NdbDictionary::Event::TableEvent event, TableRow pre, TableRow row) {
  checkIfBarrierReached(epoch);
  handleEvent(event, pre, row);
}

template<typename TableRow>
int TableTailer<TableRow>::processDeferredEvents() {
  int deferredEvents = 0;
  std::vector<Uint64> orderedEpochs;
  orderedEpochs.insert(orderedEpochs.end(), mEpochsDuringRecovery.begin(), mEpochsDuringRecovery.end());
  std::sort(orderedEpochs.begin(), orderedEpochs.end());

  for(auto& epoch : orderedEpochs){
    std::queue<DeferredEvent>& q = mEventsDuringRecovery[epoch];
    while(!q.empty()){
      DeferredEvent e = q.front();
      processEvent(epoch, e.mEventType, e.mPre, e.mRow);
      q.pop();
      deferredEvents++;
    }
  }
  mEventsPKDuringRecovery.clear();
  mEventsDuringRecovery.clear();
  mEpochsDuringRecovery.clear();
  mUnderRecovery = false;
  mStartProcessingDeferredEvents = false;
  return deferredEvents;
}

template<typename TableRow>
TableTailer<TableRow>::~TableTailer() {
  delete mNdbConnection;
}
#endif /* TABLETAILER_H */

