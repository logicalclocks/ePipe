/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Recovery.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 * Created on June 14, 2016, 1:29 PM
 */

#ifndef RECOVERY_H
#define RECOVERY_H
#include "Utils.h"

struct RecoveryIndeces{
    int mProjectIndex;
    int mDatasetIndex;
    int mMetadataIndex;
    int mMutationsIndex;
    
    RecoveryIndeces(){
        mProjectIndex = -1;
        mDatasetIndex = -1;
        mMetadataIndex= -1;
        mMutationsIndex = -1;
    }
};

class Recovery {
public:
    static RecoveryIndeces getRecoveryIndeces(Ndb* connection);
    static void checkpointProject(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, int projectId);
    static void checkpointDataset(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, int datasetId);
    static void checkpointMetadata(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, int metadataId);
private:
    static void checkpoint(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, int colId, int value);
};

#endif /* RECOVERY_H */

