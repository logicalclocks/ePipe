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

#ifndef PROVENANCEELASTICSEARCH_H
#define PROVENANCEELASTICSEARCH_H

#include "ElasticSearchBase.h"
#include "ProvenanceTableTailer.h"

typedef Bulk<PKeys> PBulk;

class ProvenanceElasticSearch : public ElasticSearchBase<PKeys> {
public:
  ProvenanceElasticSearch(std::string elastic_addr, std::string index,
          int time_to_wait_before_inserting, int bulk_size, const bool stats,
          SConn conn);
  virtual ~ProvenanceElasticSearch();
private:
  const std::string mIndex;
  const bool mStats;

  std::string mElasticBulkAddr;

  SConn mConn;

  virtual void process(std::vector<PBulk>* bulks);

};

#endif /* PROVENANCEELASTICSEARCH_H */

