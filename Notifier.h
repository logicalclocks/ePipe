/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Notifier.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef NOTIFIER_H
#define NOTIFIER_H

#include "NdbDataReader.h"
#include "FsMutationsBatcher.h"
#include "MetadataTableTailer.h"

class Notifier {
public:
    Notifier(const char* connection_string, const char* database_name, const char* meta_database_name,
            const int time_before_issuing_ndb_reqs, const int batch_size, 
            const int poll_maxTimeToWait, const int num_ndb_readers);
    void start();
    virtual ~Notifier();
    
private:
    const char* mDatabaseName;
    const char* mMetaDatabaseName;
    
    Ndb_cluster_connection *mClusterConnection;
    
    const int mTimeBeforeIssuingNDBReqs;
    const int mBatchSize;
    const int mPollMaxTimeToWait;
    const int mNumNdbReaders;
    
    FsMutationsTableTailer* mFsMutationsTableTailer;
    NdbDataReader* mNdbDataReader;
    FsMutationsBatcher* mFsMutationsBatcher;
    
    MetadataTableTailer* mMetadataTableTailer;
    
    Ndb* create_ndb_connection(const char* database);
    Ndb_cluster_connection* connect_to_cluster(const char *connection_string);
    
    void setup();
};

#endif /* NOTIFIER_H */

