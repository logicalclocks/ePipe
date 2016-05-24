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
 * File:   DatasetProjectCache.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "ProjectDatasetINodeCache.h"

ProjectDatasetINodeCache::ProjectDatasetINodeCache(const int lru_cap) 
    : mINodeToDataset(lru_cap, "INodeToDataset"), mDatasetToINodes(lru_cap, "DatasetToINodes"),
        mDatasetToProject(lru_cap, "DatasetToProject"), mProjectToDataset(lru_cap, "ProjectToDatasets") {
}

void ProjectDatasetINodeCache::addINodeToDataset(int inodeId, int datasetId) {
    mINodeToDataset.put(inodeId, datasetId);
    
    if(!mDatasetToINodes.contains(datasetId)){
        mDatasetToINodes.put(datasetId, new UISet());
    }
    mDatasetToINodes.get(datasetId).get()->insert(inodeId);
    LOG_TRACE() << "ADD INode[" << inodeId << "] to Dataset[" << datasetId << "]";
}

void ProjectDatasetINodeCache::addDatasetToProject(int datasetId, int projectId) {
    mDatasetToProject.put(datasetId, projectId);
    
    if(!mProjectToDataset.contains(projectId)){
        mProjectToDataset.put(projectId, new UISet());
    }
    mProjectToDataset.get(projectId).get()->insert(datasetId);
    LOG_TRACE() << "ADD Dataset[" << datasetId << "] to Project[" << projectId << "]";
}

int ProjectDatasetINodeCache::getProjectId(int datasetId) {
    int projectId = -1;
    boost::optional<int> res = mDatasetToProject.get(datasetId);
    if(!res){
        LOG_TRACE() << "Project not in the cache for Dataset[" << datasetId << "]";
        return projectId;
    }
    projectId = *res;
    LOG_TRACE() << "GOT Project[" << projectId << "] for Dataset[" << datasetId << "]";
    return projectId;
}

int ProjectDatasetINodeCache::getDatasetId(int inodeId) {
    int datasetId = -1;
    boost::optional<int> res = mINodeToDataset.get(inodeId);
    if(!res){
        LOG_TRACE() << "Dataset not in the cache for INode[" << inodeId << "]";
        return datasetId;
    }
    datasetId = *res;
    LOG_TRACE() << "GOT Dataset[" << datasetId << "] for INode[" << inodeId << "]";
    return datasetId;
}

void ProjectDatasetINodeCache::removeINode(int inodeId) {
    mINodeToDataset.remove(inodeId);
    LOG_TRACE() << "REMOVE INode[" << inodeId << "]";
}

void ProjectDatasetINodeCache::removeProject(int projectId) {
    LOG_TRACE() << "REMOVE Project[" << projectId << "]";
    if (mProjectToDataset.contains(projectId)) {
        UISet* datasets = mProjectToDataset.remove(projectId).get();
        for (UISet::iterator it = datasets->begin(); it != datasets->end(); ++it) {
            removeDataset(*it);
        }
    }
}

void ProjectDatasetINodeCache::removeDataset(int datasetId) {
    mDatasetToProject.remove(datasetId);
    LOG_TRACE() << "REMOVE Dataset[" << datasetId << "]";
    if (mDatasetToINodes.contains(datasetId)) {
        UISet* inodes = mDatasetToINodes.remove(datasetId).get();
        for (UISet::iterator it = inodes->begin(); it != inodes->end(); ++it) {
            removeINode(*it);
        }
    }
}

bool ProjectDatasetINodeCache::containsDataset(int datasetId) {
    LOG_TRACE() << "CONTAINS Dataset[" << datasetId << "]";
    return mDatasetToProject.contains(datasetId);
}

bool ProjectDatasetINodeCache::containsINode(int inodeId) {
    LOG_TRACE() << "CONTAINS INode[" << inodeId << "]";
    return mINodeToDataset.contains(inodeId);
}

ProjectDatasetINodeCache::~ProjectDatasetINodeCache() {
}

