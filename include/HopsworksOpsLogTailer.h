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

class HopsworksOpsLogTailer : public TableTailer{
public:
    HopsworksOpsLogTailer(Ndb* ndb, const int poll_maxTimeToWait, ElasticSearch* elastic,
            ProjectDatasetINodeCache* cache);
    
    static void refreshCache(MConn connection, UISet inodes, ProjectDatasetINodeCache* cache);
    static UISet refreshDatasetIds(SConn connection, UISet inodes, ProjectDatasetINodeCache* cache);
    static void refreshProjectIds(SConn connection, UISet datasets, ProjectDatasetINodeCache* cache);
    static void refreshProjectIds(const NdbDictionary::Dictionary* database, NdbTransaction* transaction,
            UISet dataset_ids, ProjectDatasetINodeCache* cache);
    
    virtual ~HopsworksOpsLogTailer();
private:
    static const WatchTable TABLE;
    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);
    
    void handleDataset(int opId, OperationType opType, int datasetId, int projectId);
    void handleUpsertDataset(int opId, OperationType opType, int datasetId, int projectId);
    void handleDeleteDataset(int datasetId, int projectId);
    string createDatasetJSONUpSert(int porjectId, NdbRecAttr* value[]);
    
    void handleProject(int projectId, OperationType opType);
    void handleDeleteProject(int projectId);
    void handleUpsertProject(int projectId, OperationType opType);
    string createProjectJSONUpSert(NdbRecAttr* value[]);
    
    static void readINodeToDatasetLookup(const NdbDictionary::Dictionary* inodesDatabase,
        NdbTransaction* inodesTransaction, UISet inodes_ids, UISet& datasets_to_read, ProjectDatasetINodeCache* cache);
    
    void checkpoint(int pk);
    
    ElasticSearch* mElasticSearch;
    ProjectDatasetINodeCache* mPDICache;
};

#endif /* HOPSWORKSOPSLOGTAILER_H */

