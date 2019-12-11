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

#ifndef REINDEXER_H
#define REINDEXER_H
#include "ClusterConnectionBase.h"
#include "ProjectsElasticSearch.h"

class Reindexer : public ClusterConnectionBase {
public:
  Reindexer(const char* connection_string, const char* database_name,
          const char* meta_database_name, const char* hive_meta_database_name,
          const HttpClientConfig elastic_client_config, const std::string index, int
          elastic_batch_size, int elastic_issue_time, int lru_cap);
  virtual ~Reindexer();

  void run();
private:
  ProjectsElasticSearch* mElasticSearch;
  const int mLRUCap;
};

#endif /* REINDEXER_H */

