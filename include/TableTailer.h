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
#include "Recovery.h"

enum Operation{
    ADD = 0,
    DELETE = 1
};

struct WatchTable{
    const char* mTableName;
    const char** mColumnNames;
    const int mNoColumns;
    const NdbDictionary::Event::TableEvent* mWatchEvents;
    const int mNoEvents;
};

class TableTailer {
public:
    TableTailer(Ndb* ndb, const WatchTable table, const int poll_maxTimeToWait);
    
    void start(int recoverFromId = -1);
    void waitToFinish();
    virtual ~TableTailer();

protected:
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) = 0;

private:
    void recover(int recoverFromId);
    void recoverAll();
    void createListenerEvent();
    void removeListenerEvent();
    void waitForEvents();
    void run();
    bool correctResult(NdbDictionary::Event::TableEvent event, NdbRecAttr* values[]);
    const char* getEventName(NdbDictionary::Event::TableEvent event);
    
    bool mStarted;
    boost::thread mThread;
    
    Ndb* mNdbConnection;
    const char* mEventName;
    const WatchTable mTable;
    const int mPollMaxTimeToWait;
};

#endif /* TABLETAILER_H */

