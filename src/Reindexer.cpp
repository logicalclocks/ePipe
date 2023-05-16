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

#include "Reindexer.h"

#include "tables/ProjectTable.h"
#include "tables/INodeTable.h"
#include "tables/DatasetTable.h"
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
  std::string mName;
  DatasetInfo(){
    
  }
  DatasetInfo(int projectId, std::string name){
    mProjectId = projectId;
    mName = name;
  }
};

typedef std::vector<DatasetInodes> DatasetInodesVec;

typedef boost::unordered_map<Int64, DatasetInfo> DatasetInfoMap;

Reindexer::Reindexer(const char* connection_string, const char* database_name,
        const char* meta_database_name, const char* hive_meta_database_name,
        const HttpClientConfig elastic_client_config, const std::string search_index, int
        elastic_batch_size, int elastic_issue_time, int lru_cap)
: ClusterConnectionBase(connection_string, database_name, meta_database_name, hive_meta_database_name),
  mSearchIndex(search_index), mLRUCap(lru_cap) {
  mElasticSearch = new ProjectsElasticSearch(elastic_client_config, elastic_issue_time,
          elastic_batch_size, false, MConn());
}

void Reindexer::run() {
  ptime start = Utils::getCurrentTime();
  mElasticSearch->start();

  Ndb* metaConn = create_ndb_connection(mMetaDatabaseName);
  Ndb* conn = create_ndb_connection(mDatabaseName);

  ProjectTable projectsTable(mLRUCap);
  INodeTable inodesTable(mLRUCap);
  DatasetTable datasetsTable(mLRUCap);
  XAttrTable xAttrTable;

  int projects = 0;
  int nonExistentProject = 0;
  projectsTable.getAll(metaConn);
  INodeRow projectsInode = inodesTable.getProjectsInode(conn);
  while (projectsTable.next()) {
    ProjectRow project = projectsTable.currRow();
    //assumption is project dirs have parentId = partitionId
    INodeRow projectInode = inodesTable.get(conn, projectsInode.mId, project.mProjectName, projectsInode.mId);
    if (project.mProjectName == projectInode.mName && projectsInode.mId == projectInode.mParentId) {
      eBulk bulk;
      bulk.push(Utils::getCurrentTime(), project.to_upsert_json(mSearchIndex, projectInode.mId));
      mElasticSearch->addData(bulk);
    } else {
      LOG_WARN("Project [" << project.mId << ", " << project.mProjectName
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
    eBulk bulk;
    bulk.push(Utils::getCurrentTime(), dataset.to_upsert_json(mSearchIndex));
    mElasticSearch->addData(bulk);
    dsInfoMap[dataset.mInodeId] = DatasetInfo(dataset.mProjectId, dataset.mInodeName);
    totalDatasets++;
  }

  LOG_INFO(totalDatasets << " Datasets added");

  int datasets = 0;
  int totalInodes = 0;

  DatasetInodesVec datasetStats;

  ULSet inodesWithXAttrs;
  ULSet datasetInodeIds;

  for (DatasetInfoMap::iterator mapIt = dsInfoMap.begin(); mapIt != dsInfoMap.end(); ++mapIt) {
    Int64 datasetInodeId = mapIt->first;
    int projectId = mapIt->second.mProjectId;
    LOG_INFO("Copy Dataset " << mapIt->second.mName  << " [" << datasetInodeId << "]");

    IQueue dirs;
    dirs.push(datasetInodeId);
    datasetInodeIds.insert(datasetInodeId);
    int datasetInodes = 0;
    while (!dirs.empty()) {
      Int64 dirInodeId = dirs.front();
      dirs.pop();
      LOG_DEBUG("Copy Dir " << dirInodeId << " : remaining " << dirs.size() << " dirs");
      //The partition id is the parent id for all files and directories under project subtree
      INodeVec inodes = inodesTable.getByParentId(conn, dirInodeId, dirInodeId);
      eBulk bulk;
      for (INodeVec::iterator it = inodes.begin(); it != inodes.end(); ++it) {
        INodeRow inode = *it;
        if (inode.mIsDir) {
          dirs.push(inode.mId);
        }

        if(inode.has_xattrs()){
          inodesWithXAttrs.insert(inode.mId);
        }

        bulk.push(Utils::getCurrentTime(), inode.to_create_json(mSearchIndex, datasetInodeId, projectId));
        totalInodes++;
        datasetInodes++;
      }
      mElasticSearch->addData(bulk);
    }
    datasetStats.push_back(DatasetInodes(datasetInodeId, datasetInodes));
    datasets++;
    LOG_INFO("Dataset[" << datasetInodeId << "] " << datasets << "/" << totalDatasets
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
  int nonExistentXAttrsForDatasets = 0;
  for(ULSet::iterator it = inodesWithXAttrs.begin(); it != inodesWithXAttrs
  .end(); ++it){
    Int64 inodeId = *it;
    eBulk bulk;
    XAttrVec xattrs = xAttrTable.getByInodeId(conn, inodeId);
    for(XAttrVec::iterator xit = xattrs.begin(); xit != xattrs.end(); ++xit){
      XAttrRow xAttrRow = *xit;
      if(xAttrRow.mInodeId == inodeId){
        bulk.push(Utils::getCurrentTime(), xAttrRow.to_upsert_json(mSearchIndex));
      }else{
        if(datasetInodeIds.find(inodeId) == datasetInodeIds.end()){
          LOG_WARN("XAttrs doesn't exists for [" << inodeId << "] - " << xAttrRow.to_string());
          nonExistentXAttrs++;
        }else{
          LOG_DEBUG("Dataset [" << inodeId << "] does not have Xattrs attached");
          nonExistentXAttrsForDatasets++;
        }
      }
      mElasticSearch->addData(bulk);
      numXAttrs++;
    }
  }

  LOG_INFO((numXAttrs - nonExistentXAttrs - nonExistentXAttrsForDatasets) << " XAttrs added, "
  << nonExistentXAttrs << " doesn't exists");

  mElasticSearch->shutdown();
  mElasticSearch->waitToFinish();
  LOG_INFO((projects + datasets + totalInodes - nonExistentProject) << " documents were added to Elasticsearch "
          << "(" << (projects - nonExistentProject) << " projects, " << datasets << " datasets, " << totalInodes << " files/dirs)");
  LOG_INFO("Reindexing done in " << Utils::getTimeDiffInMilliseconds(start, Utils::getCurrentTime()) << " msec");
}

Reindexer::~Reindexer() {

}

