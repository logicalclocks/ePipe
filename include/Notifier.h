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
 * File:   Notifier.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef NOTIFIER_H
#define NOTIFIER_H

#include "FsMutationsBatcher.h"
#include "MetadataBatcher.h"
#include "ProjectTableTailer.h"
#include "DatasetTableTailer.h"

class Notifier {
public:
    Notifier(const char* connection_string, const char* database_name, const char* meta_database_name,
            const int time_before_issuing_ndb_reqs, const int batch_size, 
            const int poll_maxTimeToWait, const int num_ndb_readers, const string elastic_addr,
            const bool hopsworks, const string elastic_index, const string elasttic_project_type, 
            const string elastic_dataset_type, const string elastic_inode_type);
    void start();
    virtual ~Notifier();
    
private:
    const char* mDatabaseName;
    const char* mMetaDatabaseName;
    
    Ndb_cluster_connection *mClusterConnection;
    
    const int mTimeBeforeIssuingNDBReqs;
    const int mBatchSize;
    const int mPollMaxTimeToWait;
    const int mNumNdbReaders;
    const string mElasticAddr;
    const bool mHopsworksEnabled;
    const string mElasticIndex;
    const string mElastticProjectType;
    const string mElasticDatasetType;
    const string mElasticInodeType;
            
    FsMutationsTableTailer* mFsMutationsTableTailer;
    FsMutationsDataReader* mFsMutationsDataReader;
    FsMutationsBatcher* mFsMutationsBatcher;
    
    MetadataTableTailer* mMetadataTableTailer;
    MetadataReader* mMetadataReader;
    MetadataBatcher* mMetadataBatcher;
    
    ProjectDatasetINodeCache* mPDICache;
    ProjectTableTailer* mProjectTableTailer;
    DatasetTableTailer* mDatasetTableTailer;
    
    Ndb* create_ndb_connection(const char* database);
    Ndb_cluster_connection* connect_to_cluster(const char *connection_string);
    
    void setup();
};

#endif /* NOTIFIER_H */

