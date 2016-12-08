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

struct WatchTable{
    const string mTableName;
    const string* mColumnNames;
    const int mNoColumns;
    const NdbDictionary::Event::TableEvent* mWatchEvents;
    const int mNoEvents;
    const string mRecoveryIndex;
    const string mRecoveryColumn;
};

class TableTailer {
public:
    TableTailer(Ndb* ndb, const WatchTable table, const int poll_maxTimeToWait);
    
    void start(bool recovery);
    void waitToFinish();
    virtual ~TableTailer();

protected:
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) = 0;
    Ndb* mNdbConnection;
    
private:
    void recover(int recoverFromId);
    void createListenerEvent();
    void removeListenerEvent();
    void waitForEvents();
    void run();
    const char* getEventName(NdbDictionary::Event::TableEvent event);
    
    bool mStarted;
    boost::thread mThread;
    
    const string mEventName;
    const WatchTable mTable;
    const int mPollMaxTimeToWait;
};

#endif /* TABLETAILER_H */

