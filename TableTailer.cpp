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

TableTailer::TableTailer(Ndb* ndb, const char* eventTableName, const char** eventColumnNames, const int noEventColumns,
        const bool *eventColumnIsBlob, const NdbDictionary::Event::TableEvent* watchEventTypes, const int numOfEventsTypesToWatch, const int poll_maxTimeToWait) : mNdbConnection(ndb),
        mEventTableName(eventTableName),mEventColumnNames(eventColumnNames), mEventName(concat("tail-", eventTableName)), mNoEventColumns(noEventColumns),
        mEventColumnIsBlob(eventColumnIsBlob), mWatchEventTypes(watchEventTypes), mNumOfEventsTypesToWatch(numOfEventsTypesToWatch), mStarted(false), mPollMaxTimeToWait(poll_maxTimeToWait) {
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
        LOG_ERROR() << "Thread is stopped";
        return;
    }
}

void TableTailer::createListenerEvent() {
    NdbDictionary::Dictionary *myDict = mNdbConnection->getDictionary();
    if (!myDict) LOG_NDB_API_ERROR(mNdbConnection->getNdbError());

    const NdbDictionary::Table *table = myDict->getTable(mEventTableName);
    if (!table) LOG_NDB_API_ERROR(myDict->getNdbError());

    NdbDictionary::Event myEvent(mEventName, *table);
    
    for(int i=0; i< mNumOfEventsTypesToWatch; i++){
        myEvent.addTableEvent(mWatchEventTypes[i]);
    }
    
    myEvent.addEventColumns(mNoEventColumns, mEventColumnNames);
    //myEvent.mergeEvents(merge_events);

    // Add event to database
    if (myDict->createEvent(myEvent) == 0)
        myEvent.print();
    else if (myDict->getNdbError().classification ==
            NdbError::SchemaObjectExists) {
        LOG_ERROR() << "Event creation failed, event exists, dropping Event...";
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
    LOG_INFO() << "create EventOperation for [" << mEventName << "]";
    if ((op = mNdbConnection->createEventOperation(mEventName)) == NULL)
        LOG_NDB_API_ERROR(mNdbConnection->getNdbError());

    int blobColumns = 0;
    for (int i = 0; i < mNoEventColumns; i++) {
        if(mEventColumnIsBlob[i]){
            blobColumns++;
        }
    }
    
    NdbRecAttr * recAttr[mNoEventColumns - blobColumns];
    NdbRecAttr * recAttrPre[mNoEventColumns - blobColumns];

    NdbBlob* blobAttr[blobColumns];
    NdbBlob* blobAttrPre[blobColumns];
    
    int blobIndex = 0;
    int nonBlobIndex = 0;
    
    // primary keys should always be a part of the result
    for (int i = 0; i < mNoEventColumns; i++) {
        if(mEventColumnIsBlob[i]){
            blobAttr[blobIndex] = op->getBlobHandle(mEventColumnNames[i]);
            blobAttrPre[blobIndex] = op->getPreBlobHandle(mEventColumnNames[i]);
            blobIndex++;
        }else{
            recAttr[nonBlobIndex] = op->getValue(mEventColumnNames[i]);
            recAttrPre[nonBlobIndex] = op->getPreValue(mEventColumnNames[i]);
            nonBlobIndex++;
        }
    }

    LOG_INFO() << "Execute";
    // This starts changes to "start flowing"
    if (op->execute())
        LOG_NDB_API_ERROR(op->getNdbError());
    while (true) {
        int r = mNdbConnection->pollEvents(mPollMaxTimeToWait);
        if (r > 0) {
            while ((op = mNdbConnection->nextEvent())) {
                switch (op->getEventType()) {
                    case NdbDictionary::Event::TE_INSERT:
                    case NdbDictionary::Event::TE_DELETE:
                    case NdbDictionary::Event::TE_UPDATE:
                        if(correctResult(recAttr)){
                            handleEvent(op->getEventType(), recAttrPre, recAttr, blobAttrPre, blobAttr);
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

bool TableTailer::correctResult(NdbRecAttr* values[]){
    for(int col=0; col<mNoEventColumns; col++){
        if(values[col]->isNULL() != 0){
            LOG_ERROR() << "Error at column " << mEventColumnNames[col];
            return false;
        }
    }
    return true;
}

TableTailer::~TableTailer() {
    delete mNdbConnection;
}

