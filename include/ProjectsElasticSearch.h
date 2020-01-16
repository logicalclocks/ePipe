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

#ifndef PROJECTSELASTICSEARCH_H
#define PROJECTSELASTICSEARCH_H

#include "ElasticSearchBase.h"
#include "FsMutationsTableTailer.h"
#include "MetricsMovingCounters.h"

class ProjectsElasticSearch : public ElasticSearchBase{
public:
  ProjectsElasticSearch(const HttpClientConfig elastic_client_config,
      std::string index, int time_to_wait_before_inserting, int bulk_size,
          const bool stats, MConn conn);

  void addDataset(Int64 inodeId, std::string json);
  void addProject(Int64 inodeId, std::string json);
  void removeDataset(Int64 inodeId);
  void removeProject(Int64 inodeId);

  void addDoc(Int64 inodeId, std::string json);
  void deleteDoc(Int64 inodeId);
  void deleteSchemaForINode(Int64 inodeId, std::string json);

  virtual ~ProjectsElasticSearch();
private:
  const std::string mIndex;
  std::string mElasticBulkAddr;
  MConn mConn;

  virtual void process(std::vector<eBulk>* bulks);

  bool bulkRequest(eEvent& event);
};

#endif /* PROJECTSELASTICSEARCH_H */

