/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * ePipe is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * ePipe is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef CLUSTERCONNECTIONBASE_H
#define CLUSTERCONNECTIONBASE_H
#include "Utils.h"

class ClusterConnectionBase {
public:
  ClusterConnectionBase(const char* connection_string, const char* database_name,
          const char* meta_database_name, const char* hive_meta_database_name);
  virtual ~ClusterConnectionBase();

protected:
  const char* mDatabaseName;
  const char* mMetaDatabaseName;
  const char* mHiveMetaDatabaseName;
  Ndb* create_ndb_connection(const char* database);

private:
  Ndb_cluster_connection *mClusterConnection;
  Ndb_cluster_connection* connect_to_cluster(const char *connection_string);
};

#endif /* CLUSTERCONNECTIONBASE_H */

