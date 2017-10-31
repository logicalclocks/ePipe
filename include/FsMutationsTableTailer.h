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

#include "RCTableTailer.h"
#include "ConcurrentPriorityQueue.h"
#include "ProjectDatasetINodeCache.h"
#include "ConcurrentQueue.h"
struct FsMutationPK{
    int mDatasetId;
    int mInodeId;
    int mLogicalTime;
    FsMutationPK (int datasetId, int inodeId, int logicalTime){
        mDatasetId = datasetId;
        mInodeId = inodeId;
        mLogicalTime = logicalTime;
    }
};

struct FsMutationRow {
     int mDatasetId;
     int mInodeId;
     int mPartitionId;
     int mParentId;
     string mInodeName;
     int mLogicalTime;
     OperationType mOperation;
     
     ptime mEventCreationTime;
     
     FsMutationPK getPK(){
         return FsMutationPK(mDatasetId, mInodeId, mLogicalTime);
     }
     string to_string(){
        stringstream stream;
        stream << "-------------------------" << endl;
        stream << "DatasetId = " << mDatasetId << endl;
        stream << "InodeId = " << mInodeId << endl;
        stream << "PartitionId = " << mPartitionId << endl;
        stream << "ParentId = " << mParentId << endl;
        stream << "InodeName = " << mInodeName << endl;
        stream << "LogicalTime = " << mLogicalTime << endl;
        stream << "Operation = " << Utils::OperationTypeToStr(mOperation) << endl;
        stream << "-------------------------" << endl;
        return stream.str();
     }
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
        boost::hash_combine(seed, a.mPartitionId);
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
        if(r1.mInodeId == r2.mInodeId){
            return r1.mLogicalTime > r2.mLogicalTime;
        }else{
            return r1.mInodeId > r2.mInodeId;
        }
    }
};

//typedef ConcurrentPriorityQueue<FsMutationRow, FsMutationRowComparator> CFSpq;
typedef vector<FsMutationRow> Fmq;
typedef ConcurrentQueue<FsMutationRow> CFSq;
typedef boost::heap::priority_queue<FsMutationRow,  boost::heap::compare<FsMutationRowComparator> > FSpq;
typedef vector<FsMutationPK> FPK;

class FsMutationsTableTailer : public RCTableTailer<FsMutationRow>  {
public:
    FsMutationsTableTailer(Ndb* ndb, const int poll_maxTimeToWait, const Barrier barrier, ProjectDatasetINodeCache* cache);
    FsMutationRow consume();
    virtual ~FsMutationsTableTailer();
    
    static void removeLogs(Ndb* conn, FPK& pks);
private:
    static const WatchTable TABLE;
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);
    void barrierChanged();
    void recover();
    void readRowFromNdbRecAttr(FsMutationRow &row, NdbRecAttr* value[]);
    void pushToQueue(FSpq* curr);
    CFSq* mQueue;
    FSpq* mCurrentPriorityQueue;
    boost::mutex mLock;
    
    ProjectDatasetINodeCache* mPDICache;
//    double mTimeTakenForEventsToArrive;
//    long mNumOfEvents;
//    int mPrintEveryNEvents;
    
};

#endif /* MUTATIONSTABLETAILER_H */

