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
#include "ProvenanceBatcher.h"

Notifier::Notifier(const char* connection_string, const char* database_name, const char* meta_database_name,
        const TableUnitConf mutations_tu, const TableUnitConf schemabased_tu, const TableUnitConf schemaless_tu, TableUnitConf provenance_tu,
        const int poll_maxTimeToWait, const string elastic_ip, const bool hopsworks, const string elastic_index, const string elastic_provenance_index,
        const string elasttic_project_type, const string elastic_dataset_type, const string elastic_inode_type, const string elastic_provenance_type,
        const int elastic_batch_size, const int elastic_issue_time, const int lru_cap, const bool recovery, 
        const bool stats, Barrier barrier)
: mDatabaseName(database_name), mMetaDatabaseName(meta_database_name), mMutationsTU(mutations_tu), mSchemabasedTU(schemabased_tu), 
        mSchemalessTU(schemaless_tu), mProvenanceTU(provenance_tu), mPollMaxTimeToWait(poll_maxTimeToWait), mElasticAddr(elastic_ip), mHopsworksEnabled(hopsworks),
        mElasticIndex(elastic_index), mElasticProvenanceIndex(elastic_provenance_index), mElastticProjectType(elasttic_project_type), mElasticDatasetType(elastic_dataset_type), 
        mElasticInodeType(elastic_inode_type), mElasticProvenanceType(elastic_provenance_type), mElasticBatchsize(elastic_batch_size), mElasticIssueTime(elastic_issue_time), mLRUCap(lru_cap), 
        mRecovery(recovery), mStats(stats), mBarrier(barrier) {
    mClusterConnection = connect_to_cluster(connection_string);
    setup();
}

void Notifier::start() {
    LOG_INFO("ePipe starting...");
    ptime t1 = getCurrentTime();
    
    if(mMutationsTU.isEnabled()){
        mFsMutationsDataReader->start();
        mFsMutationsBatcher->start();
        mFsMutationsTableTailer->start(mRecovery);
    }
    
    if(mSchemabasedTU.isEnabled()){
        mSchemabasedMetadataReader->start();
        mSchemabasedMetadataBatcher->start();
        mMetadataLogTailer->start(mRecovery);
    }
    
    if(mSchemalessTU.isEnabled()){
        mSchemalessMetadataReader->start();
        mSchemalessMetadataBatcher->start();
        mMetadataLogTailer->start(mRecovery);
    }
    
    if(mMutationsTU.isEnabled() || mSchemabasedTU.isEnabled() 
            || mSchemalessTU.isEnabled() || mHopsworksEnabled){
        mProjectsElasticSearch->start();
    }
       
    if (mHopsworksEnabled) {
        mhopsworksOpsLogTailer->start(mRecovery);
    }
    
    if(mProvenanceTU.isEnabled()){
        mProvenancElasticSearch->start();
        mProvenanceDataReader->start();
        mProvenanceBatcher->start();
        mProvenanceTableTailer->start(mRecovery);
    }
    
    ptime t2 = getCurrentTime();
    LOG_INFO("ePipe started in " << getTimeDiffInMilliseconds(t1, t2) << " msec");
    
    if(mMutationsTU.isEnabled()){
        mFsMutationsBatcher->waitToFinish();
        mFsMutationsTableTailer->waitToFinish();
    }
    
    if(mSchemabasedTU.isEnabled()){
        mSchemabasedMetadataBatcher->waitToFinish();
        mMetadataLogTailer->waitToFinish();
    }
   
    if(mSchemalessTU.isEnabled()){
        mSchemalessMetadataBatcher->waitToFinish();
        mMetadataLogTailer->waitToFinish();
    }
    
    if(mMutationsTU.isEnabled() || mSchemabasedTU.isEnabled()
            || mSchemalessTU.isEnabled() || mHopsworksEnabled){
        mProjectsElasticSearch->waitToFinish();
    }
    
    if (mHopsworksEnabled) {
        mhopsworksOpsLogTailer->waitToFinish();
    }
    
    if(mProvenanceTU.isEnabled()){
        mProvenanceBatcher->waitToFinish();
        mProvenanceTableTailer->waitToFinish();
        mProvenancElasticSearch->waitToFinish();
    }
}

void Notifier::setup() {
    mPDICache = new ProjectDatasetINodeCache(mLRUCap);
    mSchemaCache = new SchemaCache(mLRUCap);

    if (mMutationsTU.isEnabled() || mSchemabasedTU.isEnabled()
            || mSchemalessTU.isEnabled() || mHopsworksEnabled) {
        MConn ndb_connections_elastic;
        ndb_connections_elastic.metadataConnection = create_ndb_connection(mMetaDatabaseName);
        ndb_connections_elastic.inodeConnection = create_ndb_connection(mDatabaseName);

        mProjectsElasticSearch = new ProjectsElasticSearch(mElasticAddr, mElasticIndex, mElastticProjectType,
                mElasticDatasetType, mElasticInodeType, mElasticIssueTime, mElasticBatchsize,
                mStats, ndb_connections_elastic);
    }
  

    if (mMutationsTU.isEnabled()) {
        Ndb* mutations_tailer_connection = create_ndb_connection(mDatabaseName);
        mFsMutationsTableTailer = new FsMutationsTableTailer(mutations_tailer_connection, mPollMaxTimeToWait, mBarrier, mPDICache);

        MConn* mutations_connections = new MConn[mMutationsTU.mNumReaders];
        for (int i = 0; i < mMutationsTU.mNumReaders; i++) {
            mutations_connections[i].inodeConnection = create_ndb_connection(mDatabaseName);
            mutations_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
        }

        mFsMutationsDataReader = new FsMutationsDataReader(mutations_connections, mMutationsTU.mNumReaders,
                mHopsworksEnabled, mProjectsElasticSearch, mPDICache, mLRUCap);
        mFsMutationsBatcher = new FsMutationsBatcher(mFsMutationsTableTailer, mFsMutationsDataReader,
                mMutationsTU.mWaitTime, mMutationsTU.mBatchSize);
    }
   
    
    if(mSchemabasedTU.isEnabled() || mSchemalessTU.isEnabled()){
        Ndb* metadata_tailer_connection = create_ndb_connection(mMetaDatabaseName);
        mMetadataLogTailer = new MetadataLogTailer(metadata_tailer_connection, mPollMaxTimeToWait, mBarrier);
    }
        
    if (mSchemabasedTU.isEnabled()) {

        MConn* metadata_connections = new MConn[mSchemabasedTU.mNumReaders];
        for (int i = 0; i < mSchemabasedTU.mNumReaders; i++) {
            metadata_connections[i].inodeConnection = create_ndb_connection(mDatabaseName);
            metadata_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
        }

        mSchemabasedMetadataReader = new SchemabasedMetadataReader(metadata_connections, mSchemabasedTU.mNumReaders,
                mHopsworksEnabled, mProjectsElasticSearch, mPDICache, mSchemaCache);
        mSchemabasedMetadataBatcher = new SchemabasedMetadataBatcher(mMetadataLogTailer, mSchemabasedMetadataReader,
                mSchemabasedTU.mWaitTime, mSchemabasedTU.mBatchSize);
    }
        
    if (mSchemalessTU.isEnabled()) {
        
        MConn* s_metadata_connections = new MConn[mSchemalessTU.mNumReaders];
        for (int i = 0; i < mSchemalessTU.mNumReaders; i++) {
            s_metadata_connections[i].inodeConnection = create_ndb_connection(mDatabaseName);
            s_metadata_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
        }

        mSchemalessMetadataReader = new SchemalessMetadataReader(s_metadata_connections, mSchemalessTU.mNumReaders,
                mHopsworksEnabled, mProjectsElasticSearch, mPDICache);
        mSchemalessMetadataBatcher = new SchemalessMetadataBatcher(mMetadataLogTailer,
                mSchemalessMetadataReader, mSchemalessTU.mWaitTime, mSchemalessTU.mBatchSize);
    }
    
    if (mHopsworksEnabled) {
        Ndb* ops_log_tailer_connection = create_ndb_connection(mMetaDatabaseName);
        mhopsworksOpsLogTailer = new HopsworksOpsLogTailer(ops_log_tailer_connection, mPollMaxTimeToWait, mBarrier,
                mProjectsElasticSearch, mPDICache, mSchemaCache);
    }

    if (mProvenanceTU.isEnabled()) {
        Ndb* ndb_elastic_provenance_conn = create_ndb_connection(mDatabaseName);
        mProvenancElasticSearch = new ProvenanceElasticSearch(mElasticAddr,
                mElasticProvenanceIndex, mElasticProvenanceType, mElasticIssueTime,
                mElasticBatchsize, mStats, ndb_elastic_provenance_conn);


        Ndb* provenance_tailer_connection = create_ndb_connection(mDatabaseName);
        mProvenanceTableTailer = new ProvenanceTableTailer(provenance_tailer_connection, mPollMaxTimeToWait, mBarrier);

        SConn* provenance_connections = new SConn[mProvenanceTU.mNumReaders];
        for (int i = 0; i < mProvenanceTU.mNumReaders; i++) {
            provenance_connections[i] = create_ndb_connection(mDatabaseName);
        }
        mProvenanceDataReader = new ProvenanceDataReader(provenance_connections, mProvenanceTU.mNumReaders,
                mHopsworksEnabled, mProvenancElasticSearch, mPDICache);
        mProvenanceBatcher = new ProvenanceBatcher(mProvenanceTableTailer, mProvenanceDataReader,
                mProvenanceTU.mWaitTime, mProvenanceTU.mBatchSize);
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
