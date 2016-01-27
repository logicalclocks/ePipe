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

TableTailer::TableTailer(Ndb* ndb, const char* eventTableName, const char** eventColumnNames,
        const int noEventColumns, const NdbDictionary::Event::TableEvent watchEventType) : mNdbConnection(ndb),
mEventTableName(eventTableName), mEventName(concat("tail-", eventTableName)),
mEventColumnNames(eventColumnNames), mNoEventColumns(noEventColumns), mWatchEventType(watchEventType), mStarted(false) {
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
        cout << "Thread is stopped" << endl;
        return;
    }
}

void TableTailer::createListenerEvent() {
    NdbDictionary::Dictionary *myDict = mNdbConnection->getDictionary();
    if (!myDict) APIERROR(mNdbConnection->getNdbError());

    const NdbDictionary::Table *table = myDict->getTable(mEventTableName);
    if (!table) APIERROR(myDict->getNdbError());

    NdbDictionary::Event myEvent(mEventName, *table);
    myEvent.addTableEvent(mWatchEventType);
    myEvent.addEventColumns(mNoEventColumns, mEventColumnNames);
    //myEvent.mergeEvents(merge_events);

    // Add event to database
    if (myDict->createEvent(myEvent) == 0)
        myEvent.print();
    else if (myDict->getNdbError().classification ==
            NdbError::SchemaObjectExists) {
        printf("Event creation failed, event exists\n");
        printf("dropping Event...\n");
        if (myDict->dropEvent(mEventName)) APIERROR(myDict->getNdbError());
        // try again
        // Add event to database
        if (myDict->createEvent(myEvent)) APIERROR(myDict->getNdbError());
    } else
        APIERROR(myDict->getNdbError());
}

void TableTailer::removeListenerEvent() {
    NdbDictionary::Dictionary *myDict = mNdbConnection->getDictionary();
    if (!myDict) APIERROR(mNdbConnection->getNdbError());
    // remove event from database
    if (myDict->dropEvent(mEventName)) APIERROR(myDict->getNdbError());
}

void TableTailer::waitForEvents() {
    NdbEventOperation* op;
    printf("create EventOperation for %s \n", mEventName);
    if ((op = mNdbConnection->createEventOperation(mEventName)) == NULL)
        APIERROR(mNdbConnection->getNdbError());

    NdbRecAttr * recAttr[mNoEventColumns];
    NdbRecAttr * recAttrPre[mNoEventColumns];

    // primary keys should always be a part of the result
    for (int i = 0; i < mNoEventColumns; i++) {
        recAttr[i] = op->getValue(mEventColumnNames[i]);
        recAttrPre[i] = op->getPreValue(mEventColumnNames[i]);
    }

    // set up the callbacks
    printf("execute\n");
    // This starts changes to "start flowing"
    if (op->execute())
        APIERROR(op->getNdbError());
    while (true) {
        int r = mNdbConnection->pollEvents(POLL_EVENTS_TIMEOUT);
        if (r > 0) {
            while ((op = mNdbConnection->nextEvent())) {
                switch (op->getEventType()) {
                    case NdbDictionary::Event::TE_INSERT:
                    case NdbDictionary::Event::TE_DELETE:
                    case NdbDictionary::Event::TE_UPDATE:
                        if(correctResult(recAttr)){
                            handleEvent(op->getEventType(), recAttrPre, recAttr);
                        }
                        break;
                    default:
                        break;
                }

            }
        }
        boost::this_thread::sleep(boost::posix_time::microseconds(POLL_EVENTS_TIMEOUT));
    }

}

bool TableTailer::correctResult(NdbRecAttr* values[]){
    for(int col=0; col<mNoEventColumns; col++){
        if(values[col]->isNULL() != 0){
            printf("Error at %s \n", mEventColumnNames[col]);
            return false;
        }
    }
    return true;
}

TableTailer::~TableTailer() {
    delete mNdbConnection;
}

