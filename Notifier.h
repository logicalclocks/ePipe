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

class Notifier {
public:
    Notifier(const char* connection_string, const char* database_name);
    void start();
    virtual ~Notifier();
private:
    Ndb_cluster_connection *mClusterConnection;
    const char* mDatabaseName;
    
    FsMutationsTableTailer* mFsMutationsTable;
    
    void runFsMutationsTableTailer();
    
    Ndb_cluster_connection* connect_to_cluster(const char *connection_string);
    Ndb* create_ndb_connection();
};

#endif /* NOTIFIER_H */

