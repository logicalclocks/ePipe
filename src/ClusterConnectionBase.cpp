/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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
    LOG_NDB_API_FATAL(database, ndb->getNdbError());
  }

  return ndb;
}

Ndb_cluster_connection* ClusterConnectionBase::connect_to_cluster(const char *connection_string) {
  Ndb_cluster_connection* c;

  if (ndb_init()){
    LOG_FATAL("Failed to intialize NDB.\n\n");
  }

  c = new Ndb_cluster_connection(connection_string);

  c->set_name("ePipe");
  
  if (c->connect(RETRIES, DELAY_BETWEEN_RETRIES, VERBOSE)) {
    LOG_FATAL("Unable to connect to cluster.\n\n");
  }

  if (c->wait_until_ready(WAIT_UNTIL_READY, WAIT_UNTIL_READY) < 0) {
     LOG_FATAL("Cluster was not ready.\n\n");
  }
  
  LOG_INFO("Succefully connected to NDB cluster with NodeId " << c->node_id());

  return c;
}

ClusterConnectionBase::~ClusterConnectionBase() {
  delete mClusterConnection;
}
