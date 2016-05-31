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
#include "Utils.h"

using namespace Utils;

TableTailer::TableTailer(Ndb* ndb, const WatchTable table, const int poll_maxTimeToWait) :  mStarted(false), mNdbConnection(ndb),
        mEventName(concat("tail-", table.mTableName)), mTable(table), mPollMaxTimeToWait(poll_maxTimeToWait){
}

void TableTailer::start() {
    if (mStarted) {
        return;
    }

    createListenerEvent();
    mThread = boost::thread(&TableTailer::run, this);
    mStarted = true;
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

    const NdbDictionary::Table *table = myDict->getTable(mTable.mTableName);
    if (!table) LOG_NDB_API_ERROR(myDict->getNdbError());

    NdbDictionary::Event myEvent(mEventName, *table);
    
    for(int i=0; i< mTable.mNoEvents; i++){
        myEvent.addTableEvent(mTable.mWatchEvents[i]);
    }
    myEvent.addEventColumns(mTable.mNoColumns, mTable.mColumnNames);
    //myEvent.mergeEvents(merge_events);

    // Add event to database
    if (myDict->createEvent(myEvent) == 0)
        myEvent.print();
    else if (myDict->getNdbError().classification ==
            NdbError::SchemaObjectExists) {
        LOG_ERROR("Event creation failed, event exists, dropping Event...");
        if (myDict->dropEvent(mEventName)) LOG_NDB_API_ERROR(myDict->getNdbError());
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
    if (myDict->dropEvent(mEventName)) LOG_NDB_API_ERROR(myDict->getNdbError());
}

void TableTailer::waitForEvents() {
    NdbEventOperation* op;
    LOG_INFO("create EventOperation for [" << mEventName << "]");
    if ((op = mNdbConnection->createEventOperation(mEventName)) == NULL)
        LOG_NDB_API_ERROR(mNdbConnection->getNdbError());

    NdbRecAttr * recAttr[mTable.mNoColumns];
    NdbRecAttr * recAttrPre[mTable.mNoColumns];

    // primary keys should always be a part of the result
    for (int i = 0; i < mTable.mNoColumns; i++) {
        recAttr[i] = op->getValue(mTable.mColumnNames[i]);
        recAttrPre[i] = op->getPreValue(mTable.mColumnNames[i]);
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
                    LOG_TRACE("Got Event [" << event << ","  << getEventName(event) << "] Epoch " << op->getEpoch());
                }
                switch (event) {
                    case NdbDictionary::Event::TE_INSERT:
                    case NdbDictionary::Event::TE_DELETE:
                    case NdbDictionary::Event::TE_UPDATE:
                        if(correctResult(event, recAttr)){
                            handleEvent(event, recAttrPre, recAttr);
                        }
                        break;
                    default:
                        break;
                }

            }
        }
        boost::this_thread::sleep(boost::posix_time::milliseconds(mPollMaxTimeToWait));
    }

}

bool TableTailer::correctResult(NdbDictionary::Event::TableEvent event, NdbRecAttr* values[]){
    for(int col=0; col<mTable.mNoColumns; col++){
        if(values[col]->isNULL() != 0 && event != NdbDictionary::Event::TE_DELETE ){
            LOG_ERROR("Error at column " << mTable.mColumnNames[col] << " " << values[col]->isNULL() << getEventName(event));
            return false;
        }
    }
    return true;
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

TableTailer::~TableTailer() {
    delete mNdbConnection;
    delete mEventName;
}

