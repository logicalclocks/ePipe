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

#include "Notifier.h"
#include "ProvenanceBatcher.h"

Notifier::Notifier(const char* connection_string, const char* database_name,
    const char* meta_database_name, const char* hive_meta_database_name,
        const TableUnitConf mutations_tu, const TableUnitConf schemabased_tu,
        const TableUnitConf provenance_tu, const int poll_maxTimeToWait,
        const std::string elastic_ip, const bool hopsworks, const std::string
        elastic_index, const std::string elastic_provenance_index, const int
        elastic_batch_size, const int elastic_issue_time, const int lru_cap, const bool recovery,
        const bool stats, Barrier barrier, const bool hiveCleaner, const
        std::string metricsServer)
: ClusterConnectionBase(connection_string, database_name, meta_database_name,
    hive_meta_database_name), mMutationsTU(mutations_tu), mSchemabasedTU
    (schemabased_tu), mProvenanceTU (provenance_tu), mPollMaxTimeToWait
    (poll_maxTimeToWait), mElasticAddr(elastic_ip), mHopsworksEnabled(hopsworks),
mElasticIndex(elastic_index), mElasticProvenanceIndex(elastic_provenance_index),
mElasticBatchsize(elastic_batch_size), mElasticIssueTime(elastic_issue_time), mLRUCap(lru_cap),
mRecovery(recovery), mStats(stats), mBarrier(barrier), mHiveCleaner
(hiveCleaner), mMetricsServer(metricsServer) {
  setup();
}

void Notifier::start() {
  LOG_INFO("ePipe starting...");
  ptime t1 = getCurrentTime();

  if (mMutationsTU.isEnabled()) {
    mFsMutationsDataReaders->start();
    mFsMutationsBatcher->start();
    mFsMutationsTableTailer->start();
  }

  if (mSchemabasedTU.isEnabled()) {
    mSchemabasedMetadataReaders->start();
    mSchemabasedMetadataBatcher->start();
    mMetadataLogTailer->start();
  }

  if (mMutationsTU.isEnabled() || mSchemabasedTU.isEnabled()
           || mHopsworksEnabled) {
    mProjectsElasticSearch->start();
  }

  if (mHopsworksEnabled) {
    mhopsworksOpsLogTailer->start();
  }

  if (mProvenanceTU.isEnabled()) {
    mProvenancElasticSearch->start();
    mProvenanceDataReaders->start();
    mProvenanceBatcher->start();
    mProvenanceTableTailer->start();
  }

  if(mHiveCleaner) {
    mTblsTailer->start();
    mSDSTailer->start();
    mPARTTailer->start();
    mIDXSTailer->start();
    mSkewedLocTailer->start();
    mSkewedValuesTailer->start();
  }

  ptime t2 = getCurrentTime();
  LOG_INFO("ePipe started in " << getTimeDiffInMilliseconds(t1, t2) << " msec");

  if(mStats) {
    mHttpServer->run();
  }

  if (mMutationsTU.isEnabled()) {
    mFsMutationsBatcher->waitToFinish();
    mFsMutationsTableTailer->waitToFinish();
  }

  if (mSchemabasedTU.isEnabled()) {
    mSchemabasedMetadataBatcher->waitToFinish();
    mMetadataLogTailer->waitToFinish();
  }

  if (mMutationsTU.isEnabled() || mSchemabasedTU.isEnabled()
           || mHopsworksEnabled) {
    mProjectsElasticSearch->waitToFinish();
  }

  if (mHopsworksEnabled) {
    mhopsworksOpsLogTailer->waitToFinish();
  }

  if (mProvenanceTU.isEnabled()) {
    mProvenanceBatcher->waitToFinish();
    mProvenanceTableTailer->waitToFinish();
    mProvenancElasticSearch->waitToFinish();
  }

  if(mHiveCleaner) {
    mTblsTailer->waitToFinish();
    mSDSTailer->waitToFinish();
    mPARTTailer->waitToFinish();
    mIDXSTailer->waitToFinish();
    mSkewedLocTailer->waitToFinish();
    mSkewedValuesTailer->waitToFinish();
  }
}

void Notifier::setup() {
  if (mMutationsTU.isEnabled() || mSchemabasedTU.isEnabled() ||
  mHopsworksEnabled) {
    MConn ndb_connections_elastic;
    ndb_connections_elastic.metadataConnection = create_ndb_connection(mMetaDatabaseName);
    ndb_connections_elastic.inodeConnection = create_ndb_connection(mDatabaseName);

    mProjectsElasticSearch = new ProjectsElasticSearch(mElasticAddr, mElasticIndex,
            mElasticIssueTime, mElasticBatchsize, mStats, ndb_connections_elastic);
  }


  if (mMutationsTU.isEnabled()) {
    Ndb* mutations_tailer_connection = create_ndb_connection(mDatabaseName);
    Ndb* mutations_tailer_recovery_connection = mRecovery ?
        create_ndb_connection(mDatabaseName) : nullptr;

    mFsMutationsTableTailer = new FsMutationsTableTailer(mutations_tailer_connection,
        mutations_tailer_recovery_connection, mPollMaxTimeToWait, mBarrier);

    MConn* mutations_connections = new MConn[mMutationsTU.mNumReaders];
    for (int i = 0; i < mMutationsTU.mNumReaders; i++) {
      mutations_connections[i].inodeConnection = create_ndb_connection(mDatabaseName);
      mutations_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
    }

    mFsMutationsDataReaders = new FsMutationsDataReaders(mutations_connections, mMutationsTU.mNumReaders,
            mHopsworksEnabled, mProjectsElasticSearch, mLRUCap);
    mFsMutationsBatcher = new FsMutationsBatcher(mFsMutationsTableTailer, mFsMutationsDataReaders,
            mMutationsTU.mWaitTime, mMutationsTU.mBatchSize);
  }

  if (mSchemabasedTU.isEnabled()) {

    Ndb* metadata_tailer_connection = create_ndb_connection(mMetaDatabaseName);
    Ndb* metadata_tailer_recovery_connection = mRecovery ? create_ndb_connection
        (mMetaDatabaseName) : nullptr;
    mMetadataLogTailer = new MetadataLogTailer(metadata_tailer_connection,
        metadata_tailer_recovery_connection,
        mPollMaxTimeToWait, mBarrier);

    MConn* metadata_connections = new MConn[mSchemabasedTU.mNumReaders];
    for (int i = 0; i < mSchemabasedTU.mNumReaders; i++) {
      metadata_connections[i].inodeConnection = create_ndb_connection(mDatabaseName);
      metadata_connections[i].metadataConnection = create_ndb_connection(mMetaDatabaseName);
    }

    mSchemabasedMetadataReaders = new SchemabasedMetadataReaders(metadata_connections, mSchemabasedTU.mNumReaders,
            mHopsworksEnabled, mProjectsElasticSearch, mLRUCap);
    mSchemabasedMetadataBatcher = new SchemabasedMetadataBatcher(mMetadataLogTailer, mSchemabasedMetadataReaders,
            mSchemabasedTU.mWaitTime, mSchemabasedTU.mBatchSize);
  }

  if (mHopsworksEnabled) {
    Ndb* ops_log_tailer_connection = create_ndb_connection(mMetaDatabaseName);
    Ndb* ops_log_tailer_recovery_connection = mRecovery ? create_ndb_connection
        (mMetaDatabaseName) : nullptr;
    mhopsworksOpsLogTailer = new HopsworksOpsLogTailer(ops_log_tailer_connection,
        ops_log_tailer_recovery_connection, mPollMaxTimeToWait, mBarrier,
            mProjectsElasticSearch, mLRUCap);
  }

  if (mProvenanceTU.isEnabled()) {
    Ndb* ndb_elastic_provenance_conn = create_ndb_connection(mDatabaseName);
    mProvenancElasticSearch = new ProvenanceElasticSearch(mElasticAddr,
            mElasticProvenanceIndex, mElasticIssueTime,
            mElasticBatchsize, mStats, ndb_elastic_provenance_conn);


    Ndb* provenance_tailer_connection = create_ndb_connection(mDatabaseName);
    Ndb* provenance_tailer_recovery_connection = mRecovery
        ? create_ndb_connection(mDatabaseName) : nullptr;

    mProvenanceTableTailer = new ProvenanceTableTailer(provenance_tailer_connection,
        provenance_tailer_recovery_connection, mPollMaxTimeToWait, mBarrier);

    SConn* provenance_connections = new SConn[mProvenanceTU.mNumReaders];
    for (int i = 0; i < mProvenanceTU.mNumReaders; i++) {
      provenance_connections[i] = create_ndb_connection(mDatabaseName);
    }
    mProvenanceDataReaders = new ProvenanceDataReaders(provenance_connections, mProvenanceTU.mNumReaders,
            mHopsworksEnabled, mProvenancElasticSearch);
    mProvenanceBatcher = new ProvenanceBatcher(mProvenanceTableTailer, mProvenanceDataReaders,
            mProvenanceTU.mWaitTime, mProvenanceTU.mBatchSize);
  }


  if(mHiveCleaner) {
    Ndb *tbls_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mTblsTailer = new TBLSTailer(tbls_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *sds_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mSDSTailer = new SDSTailer(sds_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *part_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mPARTTailer = new PARTTailer(part_tailer_connection, mPollMaxTimeToWait,
                               mBarrier);

    Ndb *idxs_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mIDXSTailer = new IDXSTailer(idxs_tailer_connection, mPollMaxTimeToWait,
                                 mBarrier);

    Ndb *skl_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mSkewedLocTailer = new SkewedLocTailer(skl_tailer_connection,
        mPollMaxTimeToWait, mBarrier);

    Ndb *skv_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mSkewedValuesTailer = new SkewedValuesTailer(skv_tailer_connection,
        mPollMaxTimeToWait, mBarrier);
  }

  if(mStats) {
    std::vector<MetricsProvider*> providers;
    if(mMutationsTU.isEnabled()){
      providers.push_back(mProjectsElasticSearch);
    }
    if(mProvenanceTU.isEnabled()){
      providers.push_back(mProjectsElasticSearch);
    }
    mMetricsProviders = new MetricsProviders(providers);
    mHttpServer = new HttpServer(mMetricsServer, *mMetricsProviders);
  }
}

Notifier::~Notifier() {
  delete mFsMutationsTableTailer;
  delete mFsMutationsDataReaders;
  delete mFsMutationsBatcher;
  delete mMetadataLogTailer;
  delete mSchemabasedMetadataReaders;
  delete mSchemabasedMetadataBatcher;
  ndb_end(2);
}
