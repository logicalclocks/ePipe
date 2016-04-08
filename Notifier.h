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

#include "FsMutationsTableTailer.h"
#include "NdbDataReader.h"

class Notifier {
public:
    Notifier(const char* connection_string, const char* database_name, 
            const int time_before_issuing_ndb_reqs, const int batch_size, 
            const int poll_maxTimeToWait, const int num_ndb_readers);
    void start();
    virtual ~Notifier();
private:
    const char* mDatabaseName;
    const int mTimeBeforeIssuingNDBReqs;
    const int mBatchSize;
    const int mPollMaxTimeToWait;
    const int mNumNdbReaders;
    
    Ndb_cluster_connection *mClusterConnection;
    
    FsMutationsTableTailer* mFsMutationsTable;
    NdbDataReader* mNdbDataReader;
    
    Cus* mAddOperations;    
    Cus* mDeleteOperations;    
    boost::mutex mLock;

    bool mTimerProcessing;
    boost::thread mTimerThread;
    
    void setup_tailer();
    void setup_ndb_reader();
    void start_timer();
    void timer_thread();
    void timer_expired();
    void process_batch();
    
    Ndb_cluster_connection* connect_to_cluster(const char *connection_string);
    Ndb* create_ndb_connection();
};

#endif /* NOTIFIER_H */

