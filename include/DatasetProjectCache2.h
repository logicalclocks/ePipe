/*
 * This file is part of ePipe
 * Copyright (C) 2023, Logical Clocks AB. All rights reserved
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

#ifndef DATASETPROJECTCACHE2_H
#define DATASETPROJECTCACHE2_H
#include "Cache.h"
#include "Utils.h"
#include "tables/DBTableBase.h"
#include "tables/INodeTable.h"
#include "tables/DatasetTable.h"
#include "tables/ProjectTable.h"

class DatasetProjectCache2 {
public:
  typedef boost::unordered_map<std::string, int> PCKMap;

  DatasetProjectCache2(int lru_cap, const char* prefix) :
          mDatasetInodes(lru_cap, prefix), mProjectInodes(lru_cap, prefix),
          mProjects(lru_cap, prefix), mProjectDatasets(lru_cap, prefix), mDatasets(lru_cap, prefix) {
  }

  bool loadDatasetFromInode(Int64 datasetInodeId, Ndb* hopsConnection, INodeTable& inodesTable, Ndb* hopsworksConnection,
                            ProjectTable& projectTable, DatasetTable& datasetTable, INodeRow projectsInode) {

    LOG_INFO("handling mutation - dataset load 1");
    INodeRow datasetInode = loadDatasetInode(datasetInodeId, hopsConnection, inodesTable);
    if(datasetInode.mId != datasetInodeId) {
      return false;
    }
    LOG_INFO("handling mutation - dataset load 2");
    Int64 projectInodeId = loadProjectInode(datasetInode, hopsConnection, inodesTable, projectsInode);
    if(projectInodeId == DONT_EXIST_INT64()) {
      return false;
    }
    if(projectInodeId != datasetInode.mParentId) {
      return false;
    }
    INodeRow projectInode = mProjectInodes.get(projectInodeId).get();
    LOG_INFO("handling mutation - dataset load 3");
    ProjectRow project = loadProjectFromName(projectInode.mName, hopsworksConnection, projectTable);
    if(project.mProjectName != projectInode.mName) {
      return false;
    }
    LOG_INFO("handling mutation - dataset load 4");
    DatasetRow dataset = loadDataset(datasetInode.mName, project.mId, hopsworksConnection, datasetTable);
    if(dataset.mDatasetName != datasetInode.mName) {
      return false;
    }
    return true;
  }

  DatasetRow loadDatasetFromId(int datasetId, Ndb* hopsworksConnection, DatasetTable& datasetTable) {
    boost::optional<DatasetRow> opt = mDatasets.get(datasetId);
    if(opt) {
      return opt.get();
    } else {
      DatasetRow dataset = datasetTable.get(hopsworksConnection, datasetId);
      mDatasets.put(datasetId, dataset);
      return dataset;
    }
  }

  ProjectRow loadProjectFromId(int projectId, Ndb* hopsworksConnection, ProjectTable& projectTable) {
    boost::optional<ProjectRow> opt = mProjects.get(projectId);
    if(opt) {
      return opt.get();
    } else {
      ProjectRow project = projectTable.get(hopsworksConnection, projectId);
      if(project.mId == projectId) {
        mProjects.put(project.mId, project);
        mProjectNames.put(project.mProjectName, project.mId);
      }
      return project;
    }
  }

  std::string getProjectName(Int64 datasetInodeId) {
    boost::optional<INodeRow> datasetInodeOpt = mDatasetInodes.get(datasetInodeId);
    if(datasetInodeOpt) {
      boost::optional<INodeRow> projectInodeOpt = mProjectInodes.get(datasetInodeOpt.get().mParentId);
      if(projectInodeOpt) {
        return projectInodeOpt.get().mName;
      }
    }
    return DONT_EXIST_STR();
  }

  int getProjectId(Int64 datasetInodeId) {
    boost::optional<INodeRow> datasetInodeOpt = mDatasetInodes.get(datasetInodeId);
    if(datasetInodeOpt) {
      boost::optional<INodeRow> projectInodeOpt = mProjectInodes.get(datasetInodeOpt.get().mParentId);
      if(projectInodeOpt) {
        boost::optional<int> projectOpt = mProjectNames.get(projectInodeOpt.get().mName);
        if(projectOpt) {
          return projectOpt.get();
        }
      }
    }
    return DONT_EXIST_INT();
  }

  std::string getDatasetName(Int64 datasetInodeId) {
    boost::optional<INodeRow> datasetInodeOpt = mDatasetInodes.get(datasetInodeId);
    if(datasetInodeOpt) {
      return datasetInodeOpt.get().mName;
    }
    return DONT_EXIST_STR();
  }

  void removeDatasetByInodeId(Int64 datasetInodeId) {
    boost::optional<INodeRow> datasetInodeOpt = mDatasetInodes.get(datasetInodeId);
    if(datasetInodeOpt) {
      //if possible, clean up DatasetRow too
      removeDatasetRow(datasetInodeOpt.get().mName, datasetInodeOpt.get().mParentId);
    }
    mDatasetInodes.remove(datasetInodeId);
  }

  void removeProjectByInodeId(Int64 projectInodeId) {
    boost::optional<INodeRow> projectInodeOpt = mProjectInodes.get(projectInodeId);
    if(projectInodeOpt) {
      //if possible, clean up ProjectRow and leftover DatasetRows
      removeProjectRowByName(projectInodeOpt.get().mName);
    }
    mProjectInodes.remove(projectInodeId);
  }

private:
  Cache<Int64, INodeRow> mDatasetInodes;
  Cache<Int64, INodeRow> mProjectInodes;
  Cache<std::string, int> mProjectNames;
  Cache<int, ProjectRow> mProjects;
  Cache<int, PCKMap*> mProjectDatasets;
  Cache<int, DatasetRow> mDatasets;

  INodeRow loadDatasetInode(Int64 inodeId, Ndb* hopsConnection, INodeTable& inodesTable) {
    boost::optional<INodeRow> opt = mDatasetInodes.get(inodeId);
    INodeRow inode;
    if(opt) {
      inode = opt.get();
    } else {
      inode = inodesTable.getByInodeId(hopsConnection, inodeId);
      //cleanup possible previous leftovers - DatasetRow
      removeDatasetRow(inode.mName, inode.mParentId);
      if(inode.mId == inodeId) {
        mDatasetInodes.put(inodeId, inode);
      }
    }
    return inode;
  }

  Int64 loadProjectInode(INodeRow datasetInode, Ndb* hopsConnection, INodeTable& inodesTable, INodeRow projectsInode) {
    boost::optional<INodeRow> opt = mProjectInodes.get(datasetInode.mParentId);
    INodeRow inode;
    if(opt) {
      inode = opt.get();
    } else {
      inode = inodesTable.getByInodeId(hopsConnection, datasetInode.mParentId);
      if(inode.mId == datasetInode.mParentId) {
        if(inode.mParentId != projectsInode.mId) {
          LOG_INFO("not a dataset:" << datasetInode.mName);
          return DONT_EXIST_INT64();
        }
        //cleanup possible previous leftovers - ProjectRow
        removeProjectRowByName(inode.mName);
        mProjectInodes.put(inode.mId, inode);
      }
    }
    return inode.mId;
  }

  ProjectRow loadProjectFromName(std::string name, Ndb* hopsworksConnection, ProjectTable& projectTable) {
    boost::optional<int> opt = mProjectNames.get(name);
    if(opt) {
      return loadProjectFromId(opt.get(), hopsworksConnection, projectTable);
    } else {
      ProjectRow project = projectTable.getByName(hopsworksConnection, name);
      if(project.mProjectName == name) {
        mProjects.put(project.mId, project);
        mProjectNames.put(project.mProjectName, project.mId);
      }
      return project;
    }
  }

  DatasetRow loadDataset(std::string name, int projectId, Ndb* hopsworksConnection, DatasetTable& datasetTable) {
    boost::optional<PCKMap*> opt = mProjectDatasets.get(projectId);
    DatasetRow dataset;
    PCKMap* projectDatasets;
    if(opt) {
      projectDatasets = opt.get();
    } else {
      projectDatasets = new PCKMap();
      mProjectDatasets.put(projectId, projectDatasets);
    }
    if(projectDatasets->find(name) == projectDatasets->end()) {
      dataset = datasetTable.get(hopsworksConnection, name, projectId);
      if(dataset.mDatasetName == name) {
        std::pair<std::string, int> newDatasetLink(name, dataset.mId);
        projectDatasets->insert(newDatasetLink);
        mDatasets.put(dataset.mId, dataset);
      }
    } else {
      int datasetId = projectDatasets->at(name);
      boost::optional<DatasetRow> datasetOpt = mDatasets.get(datasetId);
      if(datasetOpt) {
        dataset = datasetOpt.get();
      } else {
        dataset = datasetTable.get(hopsworksConnection, datasetId);
        if (dataset.mId == datasetId) {
          std::pair<std::string, int> newDatasetLink(name, dataset.mId);
          projectDatasets->erase(name);
          projectDatasets->insert(newDatasetLink);
          mDatasets.put(dataset.mId, dataset);
        }
      }
    }
    return dataset;
  }

  void removeDatasetRow(std::string datasetName, Int64 projectInodeId) {
    boost::optional<INodeRow> projectInodeOpt = mProjectInodes.get(projectInodeId);
    if(projectInodeOpt) {
      INodeRow projectInode = projectInodeOpt.get();
      boost::optional<int> projectNameOpt = mProjectNames.get(projectInode.mName);
      if(projectNameOpt) {
        boost::optional<ProjectRow> projectOpt = mProjects.get(projectNameOpt.get());
        if (projectOpt) {
          ProjectRow project = projectOpt.get();
          boost::optional<PCKMap *> projectDatasetsOpt = mProjectDatasets.get(project.mId);
          if (projectDatasetsOpt) {
            PCKMap *projectDatasets = projectDatasetsOpt.get();
            for (auto it = projectDatasets->begin(); it != projectDatasets->end(); it++) {
              mDatasets.remove(it->second);
            }
            projectDatasets->erase(datasetName);
          }
        }
      }
    }
  }

  void removeProjectRowByName(std::string projectName) {
    boost::optional<int> projectNameOpt = mProjectNames.get(projectName);
    if(projectNameOpt) {
      removeProjectRowById(projectNameOpt.get());
      mProjectNames.remove(projectName);
    }
  }

  void removeProjectRowById(int projectId) {
    boost::optional<ProjectRow> projectOpt = mProjects.get(projectId);
    if (projectOpt) {
      ProjectRow project = projectOpt.get();
      boost::optional<PCKMap *> projectDatasetsOpt = mProjectDatasets.get(project.mId);
      if (projectDatasetsOpt) {
        PCKMap *projectDatasets = projectDatasetsOpt.get();
        for (auto it = projectDatasets->begin(); it != projectDatasets->end(); it++) {
          mDatasets.remove(it->second);
        }
      }
      mProjectDatasets.remove(project.mId);
      mProjects.remove(project.mId);
    }
  }
};

class DPCache2 : public DatasetProjectCache2 {
public:

  DPCache2(int lru_cap, const char* prefix) : DatasetProjectCache2(lru_cap, prefix) {
  }
};

typedef CacheSingleton<DPCache2> DatasetProjectSCache2;

#endif /* DATASETPROJECTCACHE2_H */

