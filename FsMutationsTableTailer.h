/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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

struct FsMutationRow {
     int mDatasetId;
     int mParentId;
     int mInodeId;
     string mInodeName;
     long mTimestamp;
     char mOperation;
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

enum Operation{
    ADD = 0,
    DELETE = 1
};

struct FsMutationRowComparator
{
    bool operator()(const FsMutationRow &r1, const FsMutationRow &r2) const
    {
        return r1.mTimestamp > r2.mTimestamp;
    }
};

typedef ConcurrentPriorityQueue<FsMutationRow, FsMutationRowComparator> Cpq;

class FsMutationsTableTailer : public TableTailer{
public:
    FsMutationsTableTailer(Ndb* ndb);
    FsMutationRow consume();
    virtual ~FsMutationsTableTailer();    
private:
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);
    Cpq* mQueue;
};

#endif /* MUTATIONSTABLETAILER_H */

