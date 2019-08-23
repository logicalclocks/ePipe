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
#include "SchemabasedMetadataBatcher.h"
#include "ProjectsElasticSearch.h"
#include "HopsworksOpsLogTailer.h"
#include "MetadataLogTailer.h"
#include "ProvenanceBatcher.h"
#include "ClusterConnectionBase.h"
#include "hive/TBLSTailer.h"
#include "hive/SDSTailer.h"
#include "hive/PARTTailer.h"
#include "hive/IDXSTailer.h"
#include "hive/SkewedLocTailer.h"
#include "hive/SkewedValuesTailer.h"
#include "http/server/HttpServer.h"

class Notifier : public ClusterConnectionBase {
public:
  Notifier(const char* connection_string, const char* database_name,
          const char* meta_database_name, const char* hive_meta_database_name,
          const TableUnitConf mutations_tu, const TableUnitConf schemabased_tu, const TableUnitConf provenance_tu,
          const int poll_maxTimeToWait, const std::string elastic_addr, const bool hopsworks, const std::string elastic_index, const std::string elastic_provenance_index,
          const int elastic_batch_size, const int elastic_issue_time, const int lru_cap, const bool recovery, const bool stats,
          Barrier barrier, const bool hiveCleaner, const std::string
          metricsServer);
  void start();
  virtual ~Notifier();

private:

  const TableUnitConf mMutationsTU;
  const TableUnitConf mSchemabasedTU;
  const TableUnitConf mProvenanceTU;

  const int mPollMaxTimeToWait;
  const std::string mElasticAddr;
  const bool mHopsworksEnabled;
  const std::string mElasticIndex;
  const std::string mElasticProvenanceIndex;
  const int mElasticBatchsize;
  const int mElasticIssueTime;
  const int mLRUCap;
  const bool mRecovery;
  const bool mStats;
  const Barrier mBarrier;
  const bool mHiveCleaner;
  const std::string mMetricsServer;

  ProjectsElasticSearch* mProjectsElasticSearch;

  FsMutationsTableTailer* mFsMutationsTableTailer;
  FsMutationsDataReaders* mFsMutationsDataReaders;
  FsMutationsBatcher* mFsMutationsBatcher;

  MetadataLogTailer* mMetadataLogTailer;

  SchemabasedMetadataReaders* mSchemabasedMetadataReaders;
  SchemabasedMetadataBatcher* mSchemabasedMetadataBatcher;

  HopsworksOpsLogTailer* mhopsworksOpsLogTailer;

  ProvenanceTableTailer* mProvenanceTableTailer;
  ProvenanceDataReaders* mProvenanceDataReaders;
  ProvenanceBatcher* mProvenanceBatcher;

  ProvenanceElasticSearch* mProvenancElasticSearch;

  TBLSTailer* mTblsTailer;
  SDSTailer* mSDSTailer;
  PARTTailer* mPARTTailer;
  IDXSTailer* mIDXSTailer;
  SkewedLocTailer* mSkewedLocTailer;
  SkewedValuesTailer* mSkewedValuesTailer;

  HttpServer* mHttpServer;
  MetricsProviders* mMetricsProviders;
  void setup();
};

#endif /* NOTIFIER_H */

