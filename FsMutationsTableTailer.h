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
     int mLogicalTime;
     char mOperation;
};

struct FsMutationRowComparator
{
    bool operator()(const FsMutationRow &r1, const FsMutationRow &r2) const
    {
        return r1.mLogicalTime > r2.mLogicalTime;
    }
};

class FsMutationsTableTailer : public TableTailer{
public:
    FsMutationsTableTailer(Ndb* ndb);
    FsMutationRow consume();
    virtual ~FsMutationsTableTailer();    
private:
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);
    ConcurrentPriorityQueue<FsMutationRow, FsMutationRowComparator>* mQueue;
};

#endif /* MUTATIONSTABLETAILER_H */

