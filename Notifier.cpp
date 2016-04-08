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

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

Notifier::Notifier(const char* connection_string, const char* database_name,
        const int time_before_issuing_ndb_reqs, const int batch_size, const int poll_maxTimeToWait)
: mDatabaseName(database_name), mTimeBeforeIssuingNDBReqs(time_before_issuing_ndb_reqs), mBatchSize(batch_size), mPollMaxTimeToWait(poll_maxTimeToWait) {
    mClusterConnection = connect_to_cluster(connection_string);
    printf("Connection Established.\n\n");
    mTimerStarted = false;
    mAddOperations = new Cus();
    mDeleteOperations = new Cus();
}

void Notifier::start() {
    Ndb* ndb1 = create_ndb_connection();
    mFsMutationsTable = new FsMutationsTableTailer(ndb1, mPollMaxTimeToWait);
    mFsMutationsTable->start();

    while (true) {
        start_timer_if_possible();
        FsMutationRow row = mFsMutationsTable->consume();

        LOG_DEBUG() << "-------------------------";
        LOG_DEBUG() << "DatasetId = " << row.mDatasetId;
        LOG_DEBUG() << "InodeId = " << row.mInodeId;
        LOG_DEBUG() << "ParentId = " << row.mParentId;
        LOG_DEBUG() << "InodeName = " << row.mInodeName;
        LOG_DEBUG() << "Timestamp = " << row.mTimestamp;
        LOG_DEBUG() << "Operation = " << row.mOperation;
        LOG_DEBUG() << "-------------------------";

        if (row.mOperation == DELETE) {
            mDeleteOperations->add(row);
        } else if (row.mOperation == ADD) {
            mAddOperations->add(row);
        } else {

            LOG_ERROR() << "Unknown Operation code " << row.mOperation;
        }

    }

}

void Notifier::start_timer_if_possible() {
    if (!mTimerStarted) {

        mTimerThread = boost::thread(&Notifier::timer_thread, this);
        mTimerStarted = true;
    }
}

void Notifier::timer_thread() {

    boost::asio::io_service io;
    boost::asio::deadline_timer timer(io, boost::posix_time::milliseconds(mTimeBeforeIssuingNDBReqs));
    timer.async_wait(boost::bind(&Notifier::timer_expired, this));
    io.run();
    mTimerStarted = false;
}

void Notifier::timer_expired() {

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

        LOG_NDB_API_ERROR(ndb->getNdbError());
    }

    return ndb;
}

Notifier::~Notifier() {
    delete mClusterConnection;
    ndb_end(2);
}
