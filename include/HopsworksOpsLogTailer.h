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

#ifndef HOPSWORKSOPSLOGTAILER_H
#define HOPSWORKSOPSLOGTAILER_H

#include "TableTailer.h"
#include "ProjectsElasticSearch.h"
#include "tables/HopsworksOpsLogTable.h"
#include "tables/ProjectTable.h"
#include "tables/DatasetTable.h"
#include "tables/MetaTemplateTable.h"

class HopsworksOpsLogTailer : public TableTailer<HopsworksOpRow> {
public:
  HopsworksOpsLogTailer(Ndb* ndb, Ndb* ndbRecovery, const int
  poll_maxTimeToWait, const Barrier barrier,
          ProjectsElasticSearch* elastic, const int lru_cap);

  virtual ~HopsworksOpsLogTailer();
private:
  HopsworksOpsLogTable mHopsworksLogTable;
  virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, HopsworksOpRow pre, HopsworksOpRow row);

  bool handleDataset(int opId, HopsworksOpType opType, Int64 datasetINodeId, int projectId);
  bool handleUpsertDataset(int opId, HopsworksOpType opType, Int64 datasetINodeId, int projectId);
  bool handleDeleteDataset(Int64 datasetINodeId);

  bool handleProject(int projectId, Int64 inodeId, HopsworksOpType opType);
  bool handleDeleteProject(int projectId);
  bool handleUpsertProject(int projectId, Int64 inodeId, HopsworksOpType opType);

  bool handleSchema(int schemaId, HopsworksOpType opType, Int64 inodeId);
  bool handleSchemaDelete(int schemaId, Int64 inodeId);

  ProjectsElasticSearch* mElasticSearch;

  ProjectTable mProjectTable;
  DatasetTable mDatasetTable;
  MetaTemplateTable mTemplateTable;
};

#endif /* HOPSWORKSOPSLOGTAILER_H */

