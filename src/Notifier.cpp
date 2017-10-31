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
        const TableUnitConf mutations_tu, const TableUnitConf metadata_tu, const TableUnitConf schemaless_tu,
        const int poll_maxTimeToWait, const string elastic_ip, const bool hopsworks, const string elastic_index, 
        const string elasttic_project_type, const string elastic_dataset_type, const string elastic_inode_type,
        const int elastic_batch_size, const int elastic_issue_time, const int lru_cap, const bool recovery, 
        const bool stats, MetadataType metadata_type, Barrier barrier)
: mDatabaseName(database_name), mMetaDatabaseName(meta_database_name), mMutationsTU(mutations_tu), mMetadataTU(metadata_tu), 
        mSchemalessTU(schemaless_tu), mPollMaxTimeToWait(poll_maxTimeToWait), mElasticAddr(elastic_ip), mHopsworksEnabled(hopsworks),
        mElasticIndex(elastic_index), mElastticProjectType(elasttic_project_type), mElasticDatasetType(elastic_dataset_type), 
        mElasticInodeType(elastic_inode_type), mElasticBatchsize(elastic_batch_size), mElasticIssueTime(elastic_issue_time), mLRUCap(lru_cap), 
        mRecovery(recovery), mStats(stats), mMetadataType(metadata_type), mBarrier(barrier) {
    mClusterConnection = connect_to_cluster(connection_string);
    setup();
}

void Notifier::start() {
    LOG_INFO("ePipe starting...");
    ptime t1 = getCurrentTime();
    
    mFsMutationsDataReader->start();
    if(mMetadataType == Schemabased || mMetadataType == Both){
        mSchemabasedMetadataReader->start();
    }
    if(mMetadataType == Schemaless || mMetadataType == Both){
        mSchemalessMetadataReader->start();
    }    
    mElasticSearch->start();
       
    if (mHopsworksEnabled) {
        mhopsworksOpsLogTailer->start(mRecovery);
    }
    
    mFsMutationsBatcher->start();
    mFsMutationsTableTailer->start(mRecovery);
    
    if(mMetadataType == Schemabased || mMetadataType == Schemaless || mMetadataType == Both){
        mMetadataLogTailer->start(mRecovery);
    }
    
    if (mMetadataType == Schemabased || mMetadataType == Both) {
        mSchemabasedMetadataBatcher->start();
    }

    if (mMetadataType == Schemaless || mMetadataType == Both) {
        mSchemalessMetadataBatcher->start();
    }
    
    ptime t2 = getCurrentTime();
    LOG_INFO("ePipe started in " << getTimeDiffInMilliseconds(t1, t2) << " msec");
    mFsMutationsBatcher->waitToFinish();
    mSchemabasedMetadataBatcher->waitToFinish();
    mElasticSearch->waitToFinish();
}

void Notifier::setup() {
    mPDICache = new ProjectDatasetINodeCache(mLRUCap);
    mSchemaCache = new SchemaCache(mLRUCap);
    
    MConn ndb_connections_elastic;
    ndb_connections_elastic.metadataConnection = create_ndb_connection(mMetaDatabaseName);
    ndb_connections_elastic.inodeConnection = create_ndb_connection(mDatabaseName);
    
    mElasticSearch = new ElasticSearch(mElasticAddr, mElasticIndex, mElastticProjectType,
            mElasticDatasetType, mElasticInodeType, mElasticIssueTime, mElasticBatchsize, 
            mStats, ndb_connections_elastic);
    
    Ndb* mutations_tailer_connection = create_ndb_connection(mDatabaseName);
    mFsMutationsTableTailer = new FsMutationsTableTailer(mutations_tailer_connection, mPollMaxTimeToWait, mBarrier, mPDICache);
    
    MConn* mutations_connections = new MConn[mMutationsTU.mNumReaders];
    for(int i=0; i< mMutationsTU.mNumReaders; i++){
        mutations_connections[i].inodeConnection =  create_ndb_connection(mDatabaseName);
        mutations_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
    }
    
    mFsMutationsDataReader = new FsMutationsDataReader(mutations_connections, mMutationsTU.mNumReaders, 
           mHopsworksEnabled, mElasticSearch, mPDICache, mLRUCap);
    mFsMutationsBatcher = new FsMutationsBatcher(mFsMutationsTableTailer, mFsMutationsDataReader, 
            mMutationsTU.mWaitTime, mMutationsTU.mBatchSize);
    

    Ndb* metadata_tailer_connection = create_ndb_connection(mMetaDatabaseName);
    mMetadataLogTailer = new MetadataLogTailer(metadata_tailer_connection, mPollMaxTimeToWait, mBarrier);
    
    if (mMetadataType == Schemabased || mMetadataType == Both) {

        MConn* metadata_connections = new MConn[mMetadataTU.mNumReaders];
        for (int i = 0; i < mMetadataTU.mNumReaders; i++) {
            metadata_connections[i].inodeConnection = create_ndb_connection(mDatabaseName);
            metadata_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
        }

        mSchemabasedMetadataReader = new SchemabasedMetadataReader(metadata_connections, mMetadataTU.mNumReaders,
                mHopsworksEnabled, mElasticSearch, mPDICache, mSchemaCache);
        mSchemabasedMetadataBatcher = new SchemabasedMetadataBatcher(mMetadataLogTailer, mSchemabasedMetadataReader,
                mMetadataTU.mWaitTime, mMetadataTU.mBatchSize);
    }
        
    if (mMetadataType == Schemaless || mMetadataType == Both) {
        
        MConn* s_metadata_connections = new MConn[mSchemalessTU.mNumReaders];
        for (int i = 0; i < mSchemalessTU.mNumReaders; i++) {
            s_metadata_connections[i].inodeConnection = create_ndb_connection(mDatabaseName);
            s_metadata_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
        }

        mSchemalessMetadataReader = new SchemalessMetadataReader(s_metadata_connections, mSchemalessTU.mNumReaders,
                mHopsworksEnabled, mElasticSearch, mPDICache);
        mSchemalessMetadataBatcher = new SchemalessMetadataBatcher(mMetadataLogTailer,
                mSchemalessMetadataReader, mSchemalessTU.mWaitTime, mSchemalessTU.mBatchSize);
    }
    
    if (mHopsworksEnabled) {
        Ndb* ops_log_tailer_connection = create_ndb_connection(mMetaDatabaseName);
        mhopsworksOpsLogTailer = new HopsworksOpsLogTailer(ops_log_tailer_connection, mPollMaxTimeToWait, mBarrier,
                mElasticSearch, mPDICache, mSchemaCache);
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
    delete mMetadataLogTailer;
    delete mSchemabasedMetadataReader;
    delete mSchemabasedMetadataBatcher;
    delete mSchemalessMetadataReader;
    delete mSchemalessMetadataBatcher;
    ndb_end(2);
}
