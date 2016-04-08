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
#include "ConcurrentUnorderedSet.h"

typedef ConcurrentUnorderedSet<FsMutationRow,FsMutationRowHash,FsMutationRowEqual> Cus;

class Notifier {
public:
    Notifier(const char* connection_string, const char* database_name, 
            const int time_before_issuing_ndb_reqs, const int batch_size, const int poll_maxTimeToWait);
    void start();
    virtual ~Notifier();
private:
    const char* mDatabaseName;
    const int mTimeBeforeIssuingNDBReqs;
    const int mBatchSize;
    const int mPollMaxTimeToWait;
    
    Ndb_cluster_connection *mClusterConnection;
    
    FsMutationsTableTailer* mFsMutationsTable;
    Cus* mAddOperations;    
    Cus* mDeleteOperations;    
    
    bool mTimerStarted;
    boost::thread mTimerThread;
    
    void start_timer_if_possible();
    void timer_thread();
    void timer_expired();
    
    
    Ndb_cluster_connection* connect_to_cluster(const char *connection_string);
    Ndb* create_ndb_connection();
};

#endif /* NOTIFIER_H */

