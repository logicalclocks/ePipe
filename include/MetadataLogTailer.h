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
 * File:   MetadataLogTailer.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef METADATALOGTAILER_H
#define METADATALOGTAILER_H

#include "RCTableTailer.h"
#include "ConcurrentPriorityQueue.h"
#include "HopsworksOpsLogTailer.h"

struct MetadataLogEntry {
     int mId;
     int mMetaPK1;
     int mMetaPK2;
     int mMetaPK3;
     OperationType mMetaOpType;
     MetadataType mMetaType;
     
     ptime mEventCreationTime;
     
     string to_string(){
        stringstream stream;
        stream << "-------------------------" << endl;
        stream << "Id = " << mId << endl;
        stream << "MetaType = " << Utils::MetadataTypeToStr(mMetaType) << endl;
        stream << "MetaPK1 = " << mMetaPK1 << endl;
        stream << "MetaPK2 = " << mMetaPK2 << endl;
        stream << "MetaPK3 = " << mMetaPK3 << endl;
        stream << "MetaOpType = " << Utils::OperationTypeToStr(mMetaOpType) << endl;
        stream << "-------------------------" << endl;
        return stream.str();
     }
};

struct HopsworksMetaLogEntryComparator
{
    bool operator()(const MetadataLogEntry &r1, const MetadataLogEntry &r2) const
    {
        return r1.mId > r2.mId;
    }
};

typedef ConcurrentPriorityQueue<MetadataLogEntry, HopsworksMetaLogEntryComparator> Cmq;
typedef vector<MetadataLogEntry> Mq;

class MetadataLogTailer : public RCTableTailer<MetadataLogEntry> {
public:
    MetadataLogTailer(Ndb* ndb, const int poll_maxTimeToWait);
    MetadataLogEntry consumeMultiQueue(int queue_id);
    MetadataLogEntry consume();
    virtual ~MetadataLogTailer();
private:
    static const WatchTable TABLE;

    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);
    Cmq* mSchemaBasedQueue;
    Cmq* mSchemalessQueue;
};

#endif /* METADATALOGTAILER_H */

