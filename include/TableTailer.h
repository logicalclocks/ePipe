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

class TableTailer {
public:
    TableTailer(Ndb* ndb, const char *eventTableName, const char **eventColumnNames, const int noEventColumnNames,
            const NdbDictionary::Event::TableEvent* watchEventTypes, const int numOfEventsTypesToWatch, const int poll_maxTimeToWait);
    void start();
    void waitToFinish();
    virtual ~TableTailer();

protected:
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) = 0;

private:
    void createListenerEvent();
    void removeListenerEvent();
    void waitForEvents();
    void run();
    bool correctResult(NdbRecAttr* values[]);
    
    bool mStarted;
    boost::thread mThread;
    
    Ndb* mNdbConnection;
    const char* mEventTableName;
    const char** mEventColumnNames;
    const char* mEventName;
    const int mNoEventColumns;
    const NdbDictionary::Event::TableEvent* mWatchEventTypes;
    const int mNumOfEventsTypesToWatch;
    const int mPollMaxTimeToWait;
};

#endif /* TABLETAILER_H */

