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

#include "common.h"

class TableTailer {
public:
    TableTailer(Ndb* ndb, const char *eventTableName,
            const char **eventColumnNames, const int noEventColumnNames, 
            const NdbDictionary::Event::TableEvent watchEventType);
    void start();
    virtual ~TableTailer();

protected:
    virtual void handleValue(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]) = 0;

private:
    void createListenerEvent();
    void removeListenerEvent();
    void run();

    Ndb* mNdbConnection;
    const NdbDictionary::Event::TableEvent mWatchEventType;
    const char* mEventName;
    const char* mEventTableName;
    const char** mEventColumnNames;
    const int mNoEventColumns;
};

#endif /* TABLETAILER_H */

