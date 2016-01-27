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
 * File:   Notifier.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "Notifier.h"

Notifier::Notifier(const char* connection_string, const char* database_name) : mDatabaseName(database_name) {
    mClusterConnection = connect_to_cluster(connection_string);
    printf("Connection Established.\n\n");
}

void Notifier::start() {
    Ndb* ndb1 = create_ndb_connection();
    mFsMutationsTable = new FsMutationsTableTailer(ndb1);
    mFsMutationsTable->start();
    while (true) {
        FsMutationRow row = mFsMutationsTable->consume();
        printf("-------------------------\n");
        printf("DatasetId = (%i) \n", row.mDatasetId);
        printf("InodeId = (%i) \n", row.mInodeId);
        printf("ParentId = (%i) \n", row.mParentId);
        printf("InodeName = (%s) \n", row.mInodeName.c_str());
        printf("LogicalTime = (%i) \n", row.mLogicalTime);
        printf("Operation = (%i) \n", row.mOperation);
        printf("-------------------------\n");
    }
}

void Notifier::runFsMutationsTableTailer() {
   
}

Notifier::~Notifier() {
    delete mClusterConnection;
    ndb_end(2);
}

Ndb_cluster_connection* Notifier::connect_to_cluster(const char *connection_string) {
    Ndb_cluster_connection* c;

    if (ndb_init())
        exit(EXIT_FAILURE);

    c = new Ndb_cluster_connection(connection_string);

    if (c->connect(RETRIES, DELAY_BETWEEN_RETRIES, VERBOSE)) {
        fprintf(stderr, "Unable to connect to cluster.\n\n");
        exit(EXIT_FAILURE);
    }

    if (c->wait_until_ready(WAIT_UNTIL_READY, WAIT_UNTIL_READY) < 0) {
        fprintf(stderr, "Cluster was not ready.\n\n");
        exit(EXIT_FAILURE);
    }

    return c;
}

Ndb* Notifier::create_ndb_connection() {
    Ndb* ndb = new Ndb(mClusterConnection, mDatabaseName);
    if (ndb->init() == -1) {
        APIERROR(ndb->getNdbError());
    }

    return ndb;
}


