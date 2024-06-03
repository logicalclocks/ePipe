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

Notifier::Notifier(const char* connection_string, const char* database_name,
    const char* meta_database_name, const char* hive_meta_database_name,
        const TableUnitConf mutations_tu, const int poll_maxTimeToWait,
        const HttpClientConfig elastic_client_config, const bool hopsworks,
        const std::string elastic_search_index,
        const int elastic_batch_size, const int elastic_issue_time,
        const int lru_cap, const bool recovery,
        const bool stats, Barrier barrier, const bool hiveCleaner, const std::string metricsServer)
: ClusterConnectionBase(connection_string, database_name, meta_database_name, hive_meta_database_name), 
    mMutationsTU(mutations_tu),
    mPollMaxTimeToWait(poll_maxTimeToWait),  mElasticClientConfig(elastic_client_config), mHopsworksEnabled(hopsworks),
    mElasticSearchIndex(elastic_search_index),
    mElasticBatchsize(elastic_batch_size), mElasticIssueTime(elastic_issue_time), mLRUCap(lru_cap),
    mRecovery(recovery), mStats(stats), mBarrier(barrier), mHiveCleaner(hiveCleaner), mMetricsServer(metricsServer) {
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

  if (mMutationsTU.isEnabled() || mHopsworksEnabled) {
    mProjectsElasticSearch->start();
  }

  if (mHopsworksEnabled) {
    mhopsworksOpsLogTailer->start();
  }

  if(mHiveCleaner) {
    mTblsTailer->start();
    mSDSTailer->start();
    mISCHEMATailer->start();
    mDBSTailer->start();
    mSERDESTailer->start();
    mCDSTailer->start();
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

  if (mMutationsTU.isEnabled() || mHopsworksEnabled) {
    mProjectsElasticSearch->waitToFinish();
  }

  if (mHopsworksEnabled) {
    mhopsworksOpsLogTailer->waitToFinish();
  }

  if(mHiveCleaner) {
    mTblsTailer->waitToFinish();
    mSDSTailer->waitToFinish();
    mISCHEMATailer->waitToFinish();
    mDBSTailer->waitToFinish();
    mSERDESTailer->waitToFinish();
    mCDSTailer->waitToFinish();
    mPARTTailer->waitToFinish();
    mIDXSTailer->waitToFinish();
    mSkewedLocTailer->waitToFinish();
    mSkewedValuesTailer->waitToFinish();
  }

  mConnectionChecker.join();
}

void Notifier::setup() {
  if (mMutationsTU.isEnabled() || mHopsworksEnabled) {
    MConn ndb_connections_elastic;
    ndb_connections_elastic.hopsworksConnection = create_ndb_connection(mMetaDatabaseName);
    ndb_connections_elastic.hopsConnection = create_ndb_connection(mDatabaseName);

    mProjectsElasticSearch = new ProjectsElasticSearch(mElasticClientConfig,
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
      mutations_connections[i].hopsConnection = create_ndb_connection(mDatabaseName);
      mutations_connections[i].hopsworksConnection = create_ndb_connection(mMetaDatabaseName);
    }

    mFsMutationsDataReaders = new FsMutationsDataReaders(mutations_connections, mMutationsTU.mNumReaders,
            mHopsworksEnabled, mProjectsElasticSearch, mLRUCap, mElasticSearchIndex);
    mFsMutationsBatcher = new FsMutationsBatcher(mFsMutationsTableTailer, mFsMutationsDataReaders,
            mMutationsTU.mWaitTime, mMutationsTU.mBatchSize);
  }

  if (mHopsworksEnabled) {
    Ndb* ops_log_tailer_connection = create_ndb_connection(mMetaDatabaseName);
    Ndb* ops_log_tailer_recovery_connection = mRecovery ? create_ndb_connection
        (mMetaDatabaseName) : nullptr;
    mhopsworksOpsLogTailer = new HopsworksOpsLogTailer(ops_log_tailer_connection,
        ops_log_tailer_recovery_connection, mPollMaxTimeToWait, mBarrier,
            mProjectsElasticSearch, mLRUCap, mElasticSearchIndex);
  }

  if(mHiveCleaner) {
    Ndb *tbls_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mTblsTailer = new TBLSTailer(tbls_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *sds_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mSDSTailer = new SDSTailer(sds_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *ischema_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mISCHEMATailer = new ISCHEMATailer(ischema_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *dbs_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mDBSTailer = new DBSTailer(dbs_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *serdes_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mSERDESTailer = new SERDESTailer(serdes_tailer_connection, mPollMaxTimeToWait,
        mBarrier);

    Ndb *cds_tailer_connection = create_ndb_connection(mHiveMetaDatabaseName);
    mCDSTailer = new CDSTailer(cds_tailer_connection, mPollMaxTimeToWait,
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
    mMetricsProviders = new MetricsProviders(providers);
    mHttpServer = new HttpServer(mMetricsServer, *mMetricsProviders);
  }

  mTestConnection = create_ndb_connection(mDatabaseName);
  mConnectionChecker = boost::thread(&Notifier::connectionCheck, this);
}

void Notifier::connectionCheck(){
  NdbDictionary::Dictionary *myDict = mTestConnection->getDictionary();
  NdbDictionary::Dictionary::List myList;
  while (true) {
    if(myDict->listIndexes(myList, "hdfs_metadata_log")){
      LOG_INFO("RonDB connection checker - failed to connect");
      LOG_NDB_API_FATAL("hdfs_metadata_log", myDict->getNdbError());
    }
    LOG_INFO("RonDB connection checker - success");
    boost::this_thread::sleep(boost::posix_time::milliseconds(mPollMaxTimeToWait * 20));
  }
}

Notifier::~Notifier() {
  delete mFsMutationsTableTailer;
  delete mFsMutationsDataReaders;
  delete mFsMutationsBatcher;
  ndb_end(2);
}
