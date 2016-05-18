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
 * File:   Notifier.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "Notifier.h"

Notifier::Notifier(const char* connection_string, const char* database_name, const char* meta_database_name,
        const int time_before_issuing_ndb_reqs, const int batch_size, const int poll_maxTimeToWait, 
        const int num_ndb_readers, const string elastic_ip, const bool hopsworks, const string elastic_index, 
        const string elasttic_project_type, const string elastic_dataset_type, const string elastic_inode_type)
: mDatabaseName(database_name), mMetaDatabaseName(meta_database_name), mTimeBeforeIssuingNDBReqs(time_before_issuing_ndb_reqs), mBatchSize(batch_size), 
        mPollMaxTimeToWait(poll_maxTimeToWait), mNumNdbReaders(num_ndb_readers), mElasticAddr(elastic_ip), mHopsworksEnabled(hopsworks),
        mElasticIndex(elastic_index), mElastticProjectType(elasttic_project_type), mElasticDatasetType(elastic_dataset_type), mElasticInodeType(elastic_inode_type){
    mClusterConnection = connect_to_cluster(connection_string);
    setup();
}

void Notifier::start() {
    mFsMutationsDataReader->start();
    mMetadataReader->start();
    
    mFsMutationsTableTailer->start();
    mMetadataTableTailer->start();
    
    mFsMutationsBatcher->start();
    mMetadataBatcher->start();

    if (mHopsworksEnabled) {
        mProjectTableTailer->start();
        mDatasetTableTailer->start();
    }
    
    mFsMutationsBatcher->waitToFinish();
    mMetadataBatcher->waitToFinish();
}

void Notifier::setup() {
    mPDICache = new ProjectDatasetINodeCache();
    
    Ndb* mutations_tailer_connection = create_ndb_connection(mDatabaseName);
    mFsMutationsTableTailer = new FsMutationsTableTailer(mutations_tailer_connection, mPollMaxTimeToWait, mPDICache);
    
    Ndb** mutations_connections = new Ndb*[mNumNdbReaders];
    for(int i=0; i< mNumNdbReaders; i++){
        mutations_connections[i] = create_ndb_connection(mDatabaseName);
    }
    
    
    mFsMutationsDataReader = new FsMutationsDataReader(mutations_connections, mNumNdbReaders, 
            mElasticAddr, mHopsworksEnabled, mElasticIndex, mElasticInodeType, mPDICache);
    mFsMutationsBatcher = new FsMutationsBatcher(mFsMutationsTableTailer, mFsMutationsDataReader, 
            mTimeBeforeIssuingNDBReqs, mBatchSize);
    
    
    Ndb* metadata_tailer_connection = create_ndb_connection(mMetaDatabaseName);
    mMetadataTableTailer = new MetadataTableTailer(metadata_tailer_connection, mPollMaxTimeToWait);
    
    MConn* metadata_connections = new MConn[mNumNdbReaders];
    for(int i=0; i< mNumNdbReaders; i++){
        metadata_connections[i].inodeConnection =  create_ndb_connection(mDatabaseName);
        metadata_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
    }
     
    mMetadataReader = new MetadataReader(metadata_connections, mNumNdbReaders, mElasticAddr, 
             mHopsworksEnabled, mElasticIndex, mElasticInodeType, mPDICache);
    mMetadataBatcher = new MetadataBatcher(mMetadataTableTailer, mMetadataReader, mTimeBeforeIssuingNDBReqs, mBatchSize);

    if (mHopsworksEnabled) {
        Ndb* project_tailer_connection = create_ndb_connection(mMetaDatabaseName);
        mProjectTableTailer = new ProjectTableTailer(project_tailer_connection, mPollMaxTimeToWait,
                mElasticAddr, mElasticIndex, mElastticProjectType, mPDICache);
        
        Ndb* dataset_tailer_connection = create_ndb_connection(mMetaDatabaseName);
        mDatasetTableTailer = new DatasetTableTailer(dataset_tailer_connection, mPollMaxTimeToWait,
                mElasticAddr, mElasticIndex, mElasticDatasetType, mPDICache);
    }
    
}

Ndb_cluster_connection* Notifier::connect_to_cluster(const char *connection_string) {
    Ndb_cluster_connection* c;

    if (ndb_init())
        exit(EXIT_FAILURE);

    c = new Ndb_cluster_connection(connection_string);

    if (c->connect(RETRIES, DELAY_BETWEEN_RETRIES, VERBOSE)) {
        fprintf(stderr, "Unable to connect to cluster.\n\n");
        exit(EXIT_FAILURE);
    }

    if (c->wait_until_ready(WAIT_UNTIL_READY, WAIT_UNTIL_READY) < 0) {

        fprintf(stderr, "Cluster was not ready.\n\n");
        exit(EXIT_FAILURE);
    }

    return c;
}

Ndb* Notifier::create_ndb_connection(const char* database) {
    Ndb* ndb = new Ndb(mClusterConnection, database);
    if (ndb->init() == -1) {

        LOG_NDB_API_ERROR(ndb->getNdbError());
    }

    return ndb;
}

Notifier::~Notifier() {
    delete mClusterConnection;
    delete mFsMutationsTableTailer;
    delete mFsMutationsDataReader;
    delete mFsMutationsBatcher;
    delete mMetadataTableTailer;
    delete mMetadataReader;
    delete mMetadataBatcher;
    
    ndb_end(2);
}
