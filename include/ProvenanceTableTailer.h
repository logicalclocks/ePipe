/*
 * Copyright (C) 2018 Hops.io
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
 * File:   ProvenanceTableTailer.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef PROVENANCETABLETAILER_H
#define PROVENANCETABLETAILER_H

#include "RCTableTailer.h"
#include "ConcurrentPriorityQueue.h"
#include "ConcurrentQueue.h"

struct ProvenancePK{
    int mInodeId;
    int mUserId;
    string mAppId;
    int mLogicalTime;
    ProvenancePK (int inodeId, int userId, string appId, int logicalTime){
        mInodeId = inodeId;
        mUserId = userId;
        mAppId = appId;
        mLogicalTime = logicalTime;
    }
    
    string to_string(){
        stringstream out;
        out << mInodeId << "-" << mUserId << "-" << mLogicalTime << "-" << mAppId;
        return out.str();
    }
};


struct ProvenanceRow {
    int mInodeId;
    int mUserId;
    string mAppId;
    int mLogicalTime;
    int mPartitionId;
    int mParentId;
    string mProjectName;
    string mDatasetName;
    string mInodeName;
    string mUserName;
    int mLogicalTimeBatch;
    long mTimestamp;
    long mTimestampBatch;
    short mOperation;

    ptime mEventCreationTime;

    ProvenancePK getPK(){
        return ProvenancePK(mInodeId, mUserId, mAppId, mLogicalTime);
    }
    string to_string(){
        stringstream stream;
        stream << "-------------------------" << endl;
        stream << "InodeId = " << mInodeId << endl;
        stream << "UserId = " << mUserId << endl;
        stream << "AppId = " << mAppId << endl;
        stream << "LogicalTime = " << mLogicalTime << endl;
        stream << "PartitionId = " << mPartitionId << endl;
        stream << "ParentId = " << mParentId << endl;
        stream << "ProjectName = " << mProjectName << endl;
        stream << "DatasetName = " << mDatasetName << endl;
        stream << "InodeName = " << mInodeName << endl;
        stream << "UserName = " << mUserName << endl;
        stream << "LogicalTimeBatch = " << mLogicalTimeBatch << endl;
        stream << "Timestamp = " << mTimestamp << endl;
        stream << "TimestampBatch = " << mTimestampBatch << endl;
        stream << "Operation = " << mOperation << endl;
        stream << "-------------------------" << endl;
        return stream.str();
    }
};


struct ProvenanceRowEqual {

    bool operator()(const ProvenanceRow &lhs, const ProvenanceRow &rhs) const {
        return lhs.mInodeId == rhs.mInodeId && lhs.mUserId == rhs.mUserId
               && lhs.mAppId == rhs.mAppId && lhs.mLogicalTime == rhs.mLogicalTime;
    }
};

struct ProvenanceRowHash {

    std::size_t operator()(const ProvenanceRow &a) const {
        std::size_t seed = 0;
        boost::hash_combine(seed, a.mInodeId);
        boost::hash_combine(seed, a.mUserId);
        boost::hash_combine(seed, a.mAppId);
        boost::hash_combine(seed, a.mLogicalTime);
        return seed;
    }
};

struct ProvenanceRowComparator
{
    bool operator()(const ProvenanceRow &r1, const ProvenanceRow &r2) const
    {
        if(r1.mInodeId == r2.mInodeId){
            return r1.mLogicalTime > r2.mLogicalTime;
        }else{
            return r1.mInodeId > r2.mInodeId;
        }
    }
};

typedef ConcurrentQueue<ProvenanceRow> CPRq;
typedef boost::heap::priority_queue<ProvenanceRow,  boost::heap::compare<ProvenanceRowComparator> > PRpq;
typedef vector<ProvenancePK> PKeys;
typedef vector<ProvenanceRow> Pq;
class ProvenanceTableTailer : public RCTableTailer<ProvenanceRow>{

public:
    ProvenanceTableTailer(Ndb* ndb, const int poll_maxTimeToWait, const Barrier barrier);
    ProvenanceRow consume();
    virtual ~ProvenanceTableTailer();
    
    static void removeLogs(Ndb* conn, PKeys& pks);
private:
    static const WatchTable TABLE;
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);
    void barrierChanged();
    
    void recover();
    
    void readRowFromNdbRecAttr(ProvenanceRow &row, NdbRecAttr* value[]);
    void pushToQueue(PRpq* curr);

    CPRq *mQueue;
    PRpq* mCurrentPriorityQueue;
    boost::mutex mLock;

};


#endif //PROVENANCETABLETAILER_H
