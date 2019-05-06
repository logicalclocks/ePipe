/*
 * Copyright (C) 2018 Hops.io
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
 * File:   Reindexer.cpp
 * Author: Mahmoud Ismail <maism@kth.se>
 * 
 */

#include "Reindexer.h"

#include "tables/ProjectTable.h"
#include "tables/INodeTable.h"
#include "tables/DatasetTable.h"
#include "tables/SchemabasedMetadataTable.h"
#include "tables/SchemalessMetadataTable.h"
#include "tables/XAttrTable.h"

struct DatasetInodes {
  Int64 mDatasetId;
  int mTotalNumberOfInodes;

  DatasetInodes(Int64 datasetId, int totalInodes) {
    mDatasetId = datasetId;
    mTotalNumberOfInodes = totalInodes;
  }

  bool operator<(const DatasetInodes& o) const {
    return (mTotalNumberOfInodes < o.mTotalNumberOfInodes);
  }
};

struct DatasetInfo{
  int mProjectId;
  string mName;
  DatasetInfo(){
    
  }
  DatasetInfo(int projectId, string name){
    mProjectId = projectId;
    mName = name;
  }
};

typedef vector<DatasetInodes> DatasetInodesVec;

typedef boost::unordered_map<Int64, DatasetInfo> DatasetInfoMap;

Reindexer::Reindexer(const char* connection_string, const char* database_name,
        const char* meta_database_name, const string elastic_addr,
        const string index, int elastic_batch_size, int elastic_issue_time, int lru_cap)
: ClusterConnectionBase(connection_string, database_name, meta_database_name), mLRUCap(lru_cap) {
  mElasticSearch = new ProjectsElasticSearch(elastic_addr, index, elastic_issue_time,
          elastic_batch_size, false, MConn());
  LOG_INFO("Create Elasticsearch index at " << index);
}

void Reindexer::run() {
  ptime start = Utils::getCurrentTime();
  mElasticSearch->start();

  Ndb* metaConn = create_ndb_connection(mMetaDatabaseName);
  Ndb* conn = create_ndb_connection(mDatabaseName);

  ProjectTable projectsTable;
  INodeTable inodesTable(mLRUCap);
  DatasetTable datasetsTable(mLRUCap);
  SchemabasedMetadataTable schemaBasedTable(mLRUCap);
  SchemalessMetadataTable schemalessTable;
  XAttrTable xAttrTable;

  int projects = 0;
  int nonExistentProject = 0;
  projectsTable.getAll(metaConn);
  while (projectsTable.next()) {
    ProjectRow project = projectsTable.currRow();
    INodeRow projectInode = inodesTable.get(conn, project.mInodeParentId,
            project.mInodeName, project.mInodePartitionId);

    if (projectInode.is_equal(project)) {
      mElasticSearch->addDoc(projectInode.mId, project.to_create_json());
    } else {
      LOG_WARN("Project [" << project.mId << ", " << project.mInodeName
              << "] doesn't have an inode ");
      nonExistentProject++;
    }

    projects++;
  }
  LOG_INFO((projects - nonExistentProject) << " Projects added, " << nonExistentProject << " project don't exist");


  int totalDatasets = 0;
  DatasetInfoMap dsInfoMap;
  datasetsTable.getAll(metaConn);
  while (datasetsTable.next()) {
    DatasetRow dataset = datasetsTable.currRow();
    mElasticSearch->addDoc(dataset.mInodeId, dataset.to_create_json());
    dsInfoMap[dataset.mInodeId] = DatasetInfo(dataset.mProjectId, dataset.mInodeName);
    totalDatasets++;
  }

  LOG_INFO(totalDatasets << " Datasets added");

  int datasets = 0;
  int totalInodes = 0;

  DatasetInodesVec datasetStats;

  ULSet inodesWithXAttrs;

  for (DatasetInfoMap::iterator mapIt = dsInfoMap.begin(); mapIt != dsInfoMap.end(); ++mapIt) {
    Int64 datasetId = mapIt->first;
    int projectId = mapIt->second.mProjectId;
    LOG_INFO("Copy Dataset " << mapIt->second.mName  << " [" << datasetId << "]");

    IQueue dirs;
    dirs.push(datasetId);

    int datasetInodes = 0;
    while (!dirs.empty()) {
      Int64 dirInodeId = dirs.front();
      dirs.pop();
      LOG_DEBUG("Copy Dir " << dirInodeId << " : remaining " << dirs.size() << " dirs");
      INodeVec inodes = inodesTable.getByParentId(conn, dirInodeId);
      FSBulk bulk;
      stringstream out;
      for (INodeVec::iterator it = inodes.begin(); it != inodes.end(); ++it) {
        INodeRow inode = *it;
        if (inode.mIsDir) {
          dirs.push(inode.mId);
        }

        if(inode.mNumXAttrs > 0){
          inodesWithXAttrs.insert(inode.mId);
        }

        out << inode.to_create_json(datasetId, projectId) << endl;
        totalInodes++;
        datasetInodes++;
      }
      bulk.mJSON = out.str();
      mElasticSearch->addData(bulk);
    }
    datasetStats.push_back(DatasetInodes(datasetId, datasetInodes));
    datasets++;
    LOG_INFO("Dataset[" << datasetId << "] " << datasets << "/" << totalDatasets
            << " added with " << datasetInodes << " files/directories");
  }

  LOG_INFO(datasets << " Datasets added with " << totalInodes << " files/directories");

  std::sort(datasetStats.begin(), datasetStats.end());

  LOG_INFO("Datasets sorted with total number of files..");
  for (DatasetInodesVec::iterator it = datasetStats.begin(); it != datasetStats.end(); ++it) {
    DatasetInodes dsi = *it;
    LOG_INFO("Dataset [ " << dsi.mDatasetId << ", " << dsInfoMap[dsi.mDatasetId].mName << " ] has " << dsi.mTotalNumberOfInodes << " files/dirs.");
  }

  int numXAttrs = 0;
  int nonExistentXAttrs = 0;
  for(ULSet::iterator it = inodesWithXAttrs.begin(); it != inodesWithXAttrs
  .end(); ++it){
    Int64 inodeId = *it;
    FSBulk bulk;
    XAttrVec xattrs = xAttrTable.getByInodeId(conn, inodeId);
    for(XAttrVec::iterator xit = xattrs.begin(); xit != xattrs.end(); ++xit){
      XAttrRow xAttrRow = *xit;
      if(xAttrRow.mInodeId == inodeId){
        bulk.mJSON += FsMutationsHelper::to_upsert_json(xAttrRow);
      }else{
        LOG_WARN("XAttrs doesn't exists for ["
                     << inodeId << "] - " << xAttrRow.to_string());
        nonExistentXAttrs++;
      }
      mElasticSearch->addData(bulk);
      numXAttrs++;
    }
  }

  LOG_INFO((numXAttrs - nonExistentXAttrs) << " XAttrs added, "
  << nonExistentXAttrs << " doesn't exists");

  int extMetadata = 0;
  int nonExistentMetadata = 0;
  schemaBasedTable.getAll(metaConn);
  while (schemaBasedTable.next()) {
    SchemabasedMetadataEntry entry = schemaBasedTable.currRow(metaConn);
    INodeRow inode = inodesTable.getByInodeId(conn, entry.mTuple.mInodeId);
    if (inode.mId == entry.mTuple.mInodeId) {
      FSBulk bulk;
      bulk.mJSON = entry.to_create_json();
      mElasticSearch->addData(bulk);
    } else{
      LOG_WARN("SchemaBased extended metadata for non existent inode ["
              << entry.mTuple.mInodeId << "] - " << entry.to_string());
      nonExistentMetadata++;
    }
    extMetadata++;
  }

  LOG_INFO((extMetadata - nonExistentMetadata) << " SchemaBased extended metadata added, " 
          << nonExistentMetadata << " belong to non existent inode");

  extMetadata = 0;
  nonExistentMetadata = 0;
  schemalessTable.getAll(metaConn);
  while (schemalessTable.next()) {
    SchemalessMetadataEntry entry = schemalessTable.currRow();
    INodeRow inode = inodesTable.getByInodeId(conn, entry.mINodeId);
    if (inode.mId == entry.mINodeId) {
      FSBulk bulk;
      bulk.mJSON = entry.to_create_json();
      mElasticSearch->addData(bulk);
    }else{
      LOG_WARN("Schemaless metadata for non existent inode [" 
              << entry.mINodeId << "] - " << entry.to_string());
      nonExistentMetadata++;
    }
    extMetadata++;
  }

  LOG_INFO((extMetadata-nonExistentMetadata) << " Schemaless extended metadata added, " 
          << nonExistentMetadata << " belong to non existent inode");

  mElasticSearch->shutdown();
  mElasticSearch->waitToFinish();
  LOG_INFO((projects + datasets + totalInodes - nonExistentProject) << " documents were added to Elasticsearch "
          << "(" << (projects - nonExistentProject) << " projects, " << datasets << " datasets, " << totalInodes << " files/dirs)");
  LOG_INFO("Reindexing done in " << Utils::getTimeDiffInMilliseconds(start, Utils::getCurrentTime()) << " msec");
}

Reindexer::~Reindexer() {

}

