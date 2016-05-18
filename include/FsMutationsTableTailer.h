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
 * File:   MutationsTableTailer.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef MUTATIONSTABLETAILER_H
#define MUTATIONSTABLETAILER_H

#include "TableTailer.h"
#include "ConcurrentPriorityQueue.h"
#include "ConcurrentUnorderedSet.h"
#include "ProjectDatasetINodeCache.h"

struct FsMutationRow {
     int mDatasetId;
     int mParentId;
     int mInodeId;
     string mInodeName;
     long mTimestamp;
     Operation mOperation;
};

struct FsMutationRowEqual {
    
    bool operator()(const FsMutationRow &lhs, const FsMutationRow &rhs) const {
        return lhs.mDatasetId == rhs.mDatasetId && lhs.mParentId == rhs.mParentId
                && lhs.mInodeName == rhs.mInodeName && lhs.mInodeId == rhs.mInodeId;
    }
};

struct FsMutationRowHash {

    std::size_t operator()(const FsMutationRow &a) const {
        std::size_t seed = 0;
        boost::hash_combine(seed, a.mDatasetId);
        boost::hash_combine(seed, a.mParentId);
        boost::hash_combine(seed, a.mInodeName);
        boost::hash_combine(seed, a.mInodeId);
        
        return seed;
    }
};

struct FsMutationRowComparator
{
    bool operator()(const FsMutationRow &r1, const FsMutationRow &r2) const
    {
        return r1.mTimestamp > r2.mTimestamp;
    }
};

typedef ConcurrentPriorityQueue<FsMutationRow, FsMutationRowComparator> Cpq;
typedef ConcurrentUnorderedSet<FsMutationRow,FsMutationRowHash,FsMutationRowEqual> Cus;

struct Cus_Cus{
    Cus* added;
    Cus* deleted;
};

class FsMutationsTableTailer : public TableTailer {
public:
    FsMutationsTableTailer(Ndb* ndb, const int poll_maxTimeToWait, ProjectDatasetINodeCache* cache);
    FsMutationRow consume();
    virtual ~FsMutationsTableTailer();    
private:
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);
    Cpq* mQueue;
    ProjectDatasetINodeCache* mPDICache;
};

#endif /* MUTATIONSTABLETAILER_H */

