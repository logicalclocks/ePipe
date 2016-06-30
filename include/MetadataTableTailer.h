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
 * File:   MetadataTableTailer.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef METADATATABLETAILER_H
#define METADATATABLETAILER_H

#include "RCTableTailer.h"
#include "ConcurrentPriorityQueue.h"

struct MetadataEntry {
     int mId;
     int mFieldId;
     int mTupleId;
     string mMetadata;
     Operation mOperation;
     
     ptime mEventCreationTime;
     
     string to_string(){
        stringstream stream;
        stream << "-------------------------" << endl;
        stream << "Id = " << mId << endl;
        stream << "FieldId = " << mFieldId << endl;
        stream << "TupleId = " << mTupleId << endl;
        stream << "Data = " << mMetadata << endl;
        stream << "Operation = " << mOperation << endl;
        stream << "-------------------------" << endl;
        return stream.str();
     }
};

struct MetadataEntryComparator
{
    bool operator()(const MetadataEntry &r1, const MetadataEntry &r2) const
    {
        return r1.mId > r2.mId;
    }
};

typedef ConcurrentPriorityQueue<MetadataEntry, MetadataEntryComparator> Cmq;
typedef vector<MetadataEntry> Mq;

class MetadataTableTailer : public RCTableTailer<MetadataEntry> {
public:
    MetadataTableTailer(Ndb* ndb, const int poll_maxTimeToWait);
    MetadataEntry consume();
    virtual ~MetadataTableTailer();
    
private:
    static const WatchTable TABLE;
    
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);
    Cmq* mQueue;
};

#endif /* METADATATABLETAILER_H */

