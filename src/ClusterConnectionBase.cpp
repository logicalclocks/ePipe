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
 * File:   ClusterConnectionBase.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#include "ClusterConnectionBase.h"

ClusterConnectionBase::ClusterConnectionBase(const char* connection_string,
        const char* database_name, const char* meta_database_name, const char* hive_meta_database_name)
: mDatabaseName(database_name), mMetaDatabaseName(meta_database_name),
mHiveMetaDatabaseName(hive_meta_database_name) {
  mClusterConnection = connect_to_cluster(connection_string);
}

Ndb* ClusterConnectionBase::create_ndb_connection(const char* database) {
  Ndb* ndb = new Ndb(mClusterConnection, database);
  if (ndb->init() == -1) {

    LOG_NDB_API_ERROR(ndb->getNdbError());
  }

  return ndb;
}

Ndb_cluster_connection* ClusterConnectionBase::connect_to_cluster(const char *connection_string) {
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

ClusterConnectionBase::~ClusterConnectionBase() {
  delete mClusterConnection;
}
