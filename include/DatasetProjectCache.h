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

#ifndef DATASETPROJECTCACHE_H
#define DATASETPROJECTCACHE_H
#include "Cache.h"
#include "Utils.h"
#include "tables/DBTableBase.h"

/*
 * Key1(DatasetIId) -> Key2(ProjectId)- OneToOne relation - parent relation
 * Key2(ProjectId) -> Key1(DatasetIID) - OneToMany relation - child relation
*/
class DatasetProjectCache {
public:
  typedef boost::unordered_set<int> PCKSet;

  DatasetProjectCache(int lru_cap, const char* prefix) :
    mDatasets(lru_cap, prefix), mProjects(lru_cap, prefix), mDatasetValues(lru_cap, prefix){

  }
  void add(Int64 datasetIId, int projectId, std::string datasetName) {
    mDatasets.put(datasetIId, projectId);
    if (!mProjects.contains(projectId)) {
      mProjects.put(projectId, new PCKSet());
    }
    mProjects.get(projectId).get()->insert(datasetIId);
    mDatasetValues.put(datasetIId, datasetName);
    LOG_TRACE("Added Key[" << datasetIId << "," << projectId << "] and Value[" << datasetName << "]");
  }

  boost::optional<int> getParentProject(Int64 datasetIId) {
    return mDatasets.get(datasetIId);
  }

  PCKSet getChildrenDatasets(int projectId) {
    PCKSet keys;
    PCKSet* keysInCache = getKeysInternal(projectId);
    if (keysInCache != NULL) {
      keys.insert(keysInCache->begin(), keysInCache->end());
    }
    return keys;
  }

  boost::optional<std::string> getDatasetValue(Int64 datasetIId) {
    boost::optional<std::string> val = mDatasetValues.get(datasetIId);
    if(val) {
      LOG_INFO("dataset:" << datasetIId << " val:" << val.get());
    } else {
      LOG_INFO("dataset:" << datasetIId << " no val");
    }
    return val;
  }

  void removeDataset(Int64 datasetIId) {
    boost::optional<int> projectId = getParentProject(datasetIId);
    mDatasets.remove(datasetIId);
    if (projectId) {
      PCKSet* keysInCache = getKeysInternal(projectId.get());
      if (keysInCache != NULL) {
        keysInCache->erase(datasetIId);
      }
    }
    mDatasetValues.remove(datasetIId);
    LOG_TRACE("REMOVE Dataset[" << datasetIId << "]");
  }

  PCKSet removeProject(int projectId) {
    PCKSet keys = getChildrenDatasets(projectId);
    for (typename PCKSet::iterator it = keys.begin(); it != keys.end(); ++it) {
      Int64 datasetIId = *it;
      removeDataset(datasetIId);
    }
    return keys;
  }

  bool containsDataset(Int64 datasetIId) {
    return mDatasets.contains(datasetIId);
  }

private:
  Cache<Int64, int> mDatasets;
  Cache<int, PCKSet*> mProjects;
  Cache<Int64, std::string> mDatasetValues;

  PCKSet* getKeysInternal(int projectId) {
    boost::optional<PCKSet*> res = mProjects.get(projectId);
    if (!res) {
      LOG_TRACE("Datasets not in the cache for Project[" << projectId << "]");
      return NULL;
    }
    return *res;
  }
};

#endif /* DATASETPROJECTCACHE_H */

