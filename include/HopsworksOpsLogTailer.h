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
 * File:   HopsworksOpsLogTailer.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef HOPSWORKSOPSLOGTAILER_H
#define HOPSWORKSOPSLOGTAILER_H

#include "TableTailer.h"
#include "ProjectDatasetINodeCache.h"
#include "ElasticSearch.h"
#include "SchemaCache.h"

class HopsworksOpsLogTailer : public TableTailer{
public:
    HopsworksOpsLogTailer(Ndb* ndb, const int poll_maxTimeToWait, const Barrier barrier, 
            ElasticSearch* elastic, ProjectDatasetINodeCache* cache, SchemaCache* schemaCache);
    
    virtual ~HopsworksOpsLogTailer();
private:
    static const WatchTable TABLE;
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);
    
    bool handleDataset(int opId, OperationType opType, int datasetId, int projectId);
    bool handleUpsertDataset(int opId, OperationType opType, int datasetId, int projectId);
    bool handleDeleteDataset(int datasetId, int projectId);
    string createDatasetJSONUpSert(int porjectId, NdbRecAttr* value[]);
    
    bool handleProject(int projectId, OperationType opType);
    bool handleDeleteProject(int projectId);
    bool handleUpsertProject(int projectId, OperationType opType);
    string createProjectJSONUpSert(NdbRecAttr* value[]);
    
    bool handleSchema(int schemaId, OperationType opType, int datasetId, int projectId, int inodeId);
    bool handleSchemaDelete(int schemaId, int datasetId, int projectId, int inodeId);
    string createSchemaDeleteJSON(string schema);
    
    void removeLog(int pk);
    
    ElasticSearch* mElasticSearch;
    ProjectDatasetINodeCache* mPDICache;
    SchemaCache* mSchemaCache;
};

#endif /* HOPSWORKSOPSLOGTAILER_H */

