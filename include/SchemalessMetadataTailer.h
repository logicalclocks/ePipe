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
 * File:   SchemalessMetadataTailer.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef SCHEMALESSMETADATATAILER_H
#define SCHEMALESSMETADATATAILER_H

#include "RCTableTailer.h"
#include "ConcurrentPriorityQueue.h"

struct SchemalessMetadataEntry {
    int mId;
    int mINodeId;
    int mPartitionId;
    int mParentId;
    string mInodeName;
    string mJSONData;
    Operation mOperation;
    ptime mEventCreationTime;

    string to_string() {
        stringstream stream;
        stream << "-------------------------" << endl;
        stream << "Id = " << mId << endl;
        stream << "INodeId = " << mParentId << endl;
        stream << "PartitionId = " << mParentId << endl;
        stream << "ParentId = " << mParentId << endl;
        stream << "InodeName = " << mInodeName << endl;
        stream << "Operation = " << mOperation << endl;
        stream << "Data = " << mJSONData << endl;
        stream << "-------------------------" << endl;
        return stream.str();
    }
};

struct SchemalessMetadataEntryComparator
{
    bool operator()(const SchemalessMetadataEntry &r1, const SchemalessMetadataEntry &r2) const
    {
        return r1.mId > r2.mId;
    }
};

typedef ConcurrentPriorityQueue<SchemalessMetadataEntry, SchemalessMetadataEntryComparator> CSmq;
typedef vector<SchemalessMetadataEntry> Smq;

class SchemalessMetadataTailer : public RCTableTailer<SchemalessMetadataEntry> {
public:
    SchemalessMetadataTailer(Ndb* ndb, const int poll_maxTimeToWait);
    virtual ~SchemalessMetadataTailer();
    
    SchemalessMetadataEntry consume();
private:
    static const WatchTable TABLE;
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);
    
    CSmq* mQueue;
};

#endif /* SCHEMALESSMETADATATAILER_H */

