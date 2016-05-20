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

ProjectDatasetINodeCache::ProjectDatasetINodeCache() {
}

void ProjectDatasetINodeCache::addINodeToDataset(int inodeId, int datasetId) {
    mINodeToDataset.put(inodeId, datasetId);
    
    if(!mDatasetToINodes.contains(datasetId)){
        mDatasetToINodes.put(datasetId, new UISet());
    }
    mDatasetToINodes.get(datasetId)->insert(inodeId);
    LOG_TRACE() << "ADD INode[" << inodeId << "] to Dataset[" << datasetId << "]";
}

void ProjectDatasetINodeCache::addDatasetToProject(int datasetId, int projectId) {
    mDatasetToProject.put(datasetId, projectId);
    
    if(!mProjectToDataset.contains(projectId)){
        mProjectToDataset.put(projectId, new UISet());
    }
    mProjectToDataset.get(projectId)->insert(datasetId);
    LOG_TRACE() << "ADD Dataset[" << datasetId << "] to Project[" << projectId << "]";
}

int ProjectDatasetINodeCache::getProjectId(int datasetId) {
    //TODO: if not in the cache, read from dataset table in database?!
    int projectId = mDatasetToProject.get(datasetId);
    LOG_TRACE() << "GOT Project[" << projectId << "] for Dataset[" << datasetId << "]";
    return projectId;
}

int ProjectDatasetINodeCache::getDatasetId(int inodeId) {
    //TODO: if not in the cache revert back
    int datasetId = mINodeToDataset.get(inodeId);
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
        UISet* datasets = mProjectToDataset.remove(projectId);
        for (UISet::iterator it = datasets->begin(); it != datasets->end(); ++it) {
            removeDataset(*it);
        }
    }
}

void ProjectDatasetINodeCache::removeDataset(int datasetId) {
    mDatasetToProject.remove(datasetId);
    LOG_TRACE() << "REMOVE Dataset[" << datasetId << "]";
    if (mDatasetToINodes.contains(datasetId)) {
        UISet* inodes = mDatasetToINodes.remove(datasetId);
        for (UISet::iterator it = inodes->begin(); it != inodes->end(); ++it) {
            removeINode(*it);
        }
    }
}

ProjectDatasetINodeCache::~ProjectDatasetINodeCache() {
}

