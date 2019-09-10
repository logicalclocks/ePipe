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

#ifndef REINDEXER_H
#define REINDEXER_H
#include "ClusterConnectionBase.h"
#include "ProjectsElasticSearch.h"

class Reindexer : public ClusterConnectionBase {
public:
  Reindexer(const char* connection_string, const char* database_name,
          const char* meta_database_name, const char* hive_meta_database_name,
          const std::string elastic_addr, const std::string index, int
          elastic_batch_size, int elastic_issue_time, int lru_cap);
  virtual ~Reindexer();

  void run();
private:
  ProjectsElasticSearch* mElasticSearch;
  const int mLRUCap;
};

#endif /* REINDEXER_H */

