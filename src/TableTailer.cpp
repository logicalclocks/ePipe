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
 * File:   TableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#include "TableTailer.h"

using namespace Utils;
using namespace Utils::NdbC;

TableTailer::TableTailer(Ndb* ndb, const WatchTable table, const int poll_maxTimeToWait, const Barrier barrier) : mNdbConnection(ndb), mStarted(false),
        mEventName(concat("tail-", table.mTableName)), mTable(table), mPollMaxTimeToWait(poll_maxTimeToWait), mBarrier(barrier),
        mLastReportedBarrier(0){
}

void TableTailer::start(bool recovery) {
    if (mStarted) {
        return;
    }
    
    if(recovery){
        LOG_INFO("start with recovery for " << mTable.mTableName);
        recover();
    }
    
    createListenerEvent();
    mThread = boost::thread(&TableTailer::run, this);
    mStarted = true;
}

void TableTailer::recover() {
    const NdbDictionary::Dictionary* database = getDatabase(mNdbConnection);
    const NdbDictionary::Index* index = getIndex(database, mTable.mTableName, mTable.mRecoveryIndex);
    
    NdbTransaction* transaction = startNdbTransaction(mNdbConnection);
    NdbIndexScanOperation* scanOp = getNdbIndexScanOperation(transaction, index);
    
    scanOp->readTuples(NdbOperation::LM_CommittedRead, NdbScanOperation::SF_OrderBy);
    
    NdbRecAttr * row[mTable.mNoColumns];
    
    for (int i = 0; i < mTable.mNoColumns; i++) {
        row[i] = scanOp->getValue(mTable.mColumnNames[i].c_str());
    }

    executeTransaction(transaction, NdbTransaction::Commit);
    
    while (scanOp->nextResult(true) == 0) {
        handleEvent(NdbDictionary::Event::TE_INSERT, NULL, row);
    }
    
    transaction->close();
}

void TableTailer::waitToFinish(){
    if(mStarted){
        mThread.join();
    }
}

void TableTailer::run() {
    try {
        waitForEvents();
    } catch (boost::thread_interrupted&) {
        LOG_ERROR("Thread is stopped");
        return;
    }
}

void TableTailer::createListenerEvent() {
    NdbDictionary::Dictionary *myDict = mNdbConnection->getDictionary();
    if (!myDict) LOG_NDB_API_ERROR(mNdbConnection->getNdbError());

    const NdbDictionary::Table *table = myDict->getTable(mTable.mTableName.c_str());
    if (!table) LOG_NDB_API_ERROR(myDict->getNdbError());

    NdbDictionary::Event myEvent(mEventName.c_str(), *table);
    
    for(int i=0; i< mTable.mNoEvents; i++){
        myEvent.addTableEvent(mTable.mWatchEvents[i]);
    }
    const char* columns[mTable.mNoColumns];
    for(int i=0; i< mTable.mNoColumns; i++){
        columns[i] = mTable.mColumnNames[i].c_str();
    }
    myEvent.addEventColumns(mTable.mNoColumns, columns);
    //myEvent.mergeEvents(merge_events);

    // Add event to database
    if (myDict->createEvent(myEvent) == 0)
        myEvent.print();
    else if (myDict->getNdbError().classification ==
            NdbError::SchemaObjectExists) {
        LOG_ERROR("Event creation failed, event exists, dropping Event...");
        if (myDict->dropEvent(mEventName.c_str())) LOG_NDB_API_ERROR(myDict->getNdbError());
        // try again
        // Add event to database
        if (myDict->createEvent(myEvent)) LOG_NDB_API_ERROR(myDict->getNdbError());
    } else
        LOG_NDB_API_ERROR(myDict->getNdbError());
}

void TableTailer::removeListenerEvent() {
    NdbDictionary::Dictionary *myDict = mNdbConnection->getDictionary();
    if (!myDict) LOG_NDB_API_ERROR(mNdbConnection->getNdbError());
    // remove event from database
    if (myDict->dropEvent(mEventName.c_str())) LOG_NDB_API_ERROR(myDict->getNdbError());
}

void TableTailer::waitForEvents() {
    NdbEventOperation* op;
    LOG_INFO("create EventOperation for [" << mEventName << "]");
    if ((op = mNdbConnection->createEventOperation(mEventName.c_str())) == NULL)
        LOG_NDB_API_ERROR(mNdbConnection->getNdbError());

    NdbRecAttr * recAttr[mTable.mNoColumns];
    NdbRecAttr * recAttrPre[mTable.mNoColumns];

    // primary keys should always be a part of the result
    for (int i = 0; i < mTable.mNoColumns; i++) {
        recAttr[i] = op->getValue(mTable.mColumnNames[i].c_str());
        recAttrPre[i] = op->getPreValue(mTable.mColumnNames[i].c_str());
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
                
               if(event != NdbDictionary::Event::TE_EMPTY){
                   LOG_TRACE("Got Event [" << event << ","  << getEventName(event) << "] Epoch " << op->getEpoch() << " GCI " << getGCI(op->getEpoch()));
                }
                switch (event) {
                    case NdbDictionary::Event::TE_INSERT:
                    case NdbDictionary::Event::TE_DELETE:
                    case NdbDictionary::Event::TE_UPDATE:
                    {
                        checkIfBarrierReached(op->getEpoch());
                        handleEvent(event, recAttrPre, recAttr);
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

const char* TableTailer::getEventName(NdbDictionary::Event::TableEvent event) {
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

void TableTailer::barrierChanged(){
    //do nothing
}

Uint64 TableTailer::getGCI(Uint64 epoch) {
    return (epoch & 0xffffffff00000000) >> 32;
}

void TableTailer::checkIfBarrierReached(Uint64 epoch) {
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

TableTailer::~TableTailer() {
    delete mNdbConnection;
}

