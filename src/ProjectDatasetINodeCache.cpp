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
#include "Tables.h"

using namespace Utils::NdbC;

ProjectDatasetINodeCache::ProjectDatasetINodeCache(const int lru_cap) 
    : mINodeToDataset(lru_cap, "INodeToDataset"), mDatasetToINodes(lru_cap, "DatasetToINodes"),
        mDatasetToProject(lru_cap, "DatasetToProject"), mProjectToDataset(lru_cap, "ProjectToDatasets") {
}

void ProjectDatasetINodeCache::addINodeToDataset(int inodeId, int datasetId) {
    mINodeToDataset.put(inodeId, datasetId);
    mINodeToDataset.put(datasetId, datasetId);
    
    if(!mDatasetToINodes.contains(datasetId)){
        mDatasetToINodes.put(datasetId, new UISet());
    }
    mDatasetToINodes.get(datasetId).get()->insert(inodeId);
    LOG_TRACE("ADD INode[" << inodeId << "] to Dataset[" << datasetId << "]");
}

void ProjectDatasetINodeCache::addDatasetToProject(int datasetId, int projectId) {
    mDatasetToProject.put(datasetId, projectId);
    mINodeToDataset.put(datasetId, datasetId);
    
    if(!mProjectToDataset.contains(projectId)){
        mProjectToDataset.put(projectId, new UISet());
    }
    mProjectToDataset.get(projectId).get()->insert(datasetId);
    LOG_TRACE("ADD Dataset[" << datasetId << "] to Project[" << projectId << "]");
}

int ProjectDatasetINodeCache::getProjectId(int datasetId) {
    int projectId = -1;
    boost::optional<int> res = mDatasetToProject.get(datasetId);
    if(!res){
        LOG_TRACE("Project not in the cache for Dataset[" << datasetId << "]");
        return projectId;
    }
    projectId = *res;
    LOG_TRACE("GOT Project[" << projectId << "] for Dataset[" << datasetId << "]");
    return projectId;
}

int ProjectDatasetINodeCache::getDatasetId(int inodeId) {
    int datasetId = -1;
    boost::optional<int> res = mINodeToDataset.get(inodeId);
    if(!res){
        LOG_TRACE("Dataset not in the cache for INode[" << inodeId << "]");
        return datasetId;
    }
    datasetId = *res;
    LOG_TRACE("GOT Dataset[" << datasetId << "] for INode[" << inodeId << "]");
    return datasetId;
}

void ProjectDatasetINodeCache::removeINode(int inodeId) {
    mINodeToDataset.remove(inodeId);
    LOG_TRACE("REMOVE INode[" << inodeId << "]");
}

void ProjectDatasetINodeCache::removeProject(int projectId) {
    LOG_TRACE("REMOVE Project[" << projectId << "]");
    if (mProjectToDataset.contains(projectId)) {
        UISet* datasets = mProjectToDataset.get(projectId).get();
        for (UISet::iterator it = datasets->begin(); it != datasets->end(); ++it) {
            removeDataset(*it);
        }
        mProjectToDataset.remove(projectId);
    }
}

void ProjectDatasetINodeCache::removeDataset(int datasetId) {
    mDatasetToProject.remove(datasetId);
    LOG_TRACE("REMOVE Dataset[" << datasetId << "]");
    if (mDatasetToINodes.contains(datasetId)) {
        UISet* inodes = mDatasetToINodes.get(datasetId).get();
        for (UISet::iterator it = inodes->begin(); it != inodes->end(); ++it) {
            removeINode(*it);
        }
        mDatasetToINodes.remove(datasetId);
    }
}

bool ProjectDatasetINodeCache::containsDataset(int datasetId) {
    LOG_TRACE("CONTAINS Dataset[" << datasetId << "]");
    return mDatasetToProject.contains(datasetId);
}

bool ProjectDatasetINodeCache::containsINode(int inodeId) {
    LOG_TRACE("CONTAINS INode[" << inodeId << "]");
    return mINodeToDataset.contains(inodeId);
}



void ProjectDatasetINodeCache::refresh(MConn connection, UISet inodes){
    UISet datasets_ids = refreshDatasetIds(connection.inodeConnection, inodes);
    if(!datasets_ids.empty()){
        refreshProjectIds(connection.metadataConnection, datasets_ids);
    }
}

void ProjectDatasetINodeCache::refresh(SConn inodeConnection, const NdbDictionary::Dictionary* metaDatabase, 
        NdbTransaction* metaTransaction, UISet inodes) {
    UISet datasets_ids = refreshDatasetIds(inodeConnection, inodes);
    if(!datasets_ids.empty()){
        refreshProjectIds(metaDatabase, metaTransaction, datasets_ids);
    }
}

UISet ProjectDatasetINodeCache::refreshDatasetIds(SConn inode_connection, UISet inodes) {
    UISet inodes_ids_to_read;
    UISet dataset_ids;

    for (UISet::iterator it = inodes.begin(); it != inodes.end(); ++it) {

        int inodeId = *it;

        int datasetId = getDatasetId(inodeId);
        if (datasetId == -1) {
            inodes_ids_to_read.insert(inodeId);
            continue;
        }

        int projectId = getProjectId(datasetId);
        if (projectId == -1) {
            dataset_ids.insert(datasetId);
        }
    }


    if (!inodes_ids_to_read.empty()) {
        const NdbDictionary::Dictionary* inodeDatabase = getDatabase(inode_connection);
        NdbTransaction* inodeTransaction = startNdbTransaction(inode_connection);

        readINodeToDatasetLookup(inodeDatabase, inodeTransaction,
                inodes_ids_to_read, dataset_ids);

        executeTransaction(inodeTransaction, NdbTransaction::NoCommit);
        inode_connection->closeTransaction(inodeTransaction);
    }
    
    return dataset_ids;
}

void ProjectDatasetINodeCache::readINodeToDatasetLookup(const NdbDictionary::Dictionary* inodesDatabase,
        NdbTransaction* inodesTransaction, UISet inodes_ids, UISet& datasets_to_read) {

    UIRowMap inodesToDatasets = readTableWithIntPK(inodesDatabase, inodesTransaction,
            INODE_DATASET_LOOKUP, inodes_ids, INODE_DATASET_LOOKUP_COLS_TO_READ,
            NUM_INODE_DATASET_COLS, INODE_DATASET_LOOKUP_INODE_ID_COL);

    executeTransaction(inodesTransaction, NdbTransaction::NoCommit);

    for (UIRowMap::iterator it = inodesToDatasets.begin(); it != inodesToDatasets.end(); ++it) {
        int inodeId = it->second[INODE_DATASET_LOOKUP_INODE_ID_COL]->int32_value();
        if (it->first != inodeId) {
            //TODO: update elastic?!
            LOG_ERROR("INodeToDataset " << it->first << " doesn't exist, got inodeId "
                    << inodeId << " was expecting " << it->first);
            continue;
        }

        int datasetId = it->second[INODE_DATASET_LOOKUO_DATASET_ID_COL]->int32_value();

        addINodeToDataset(it->first, datasetId);
        if (!containsDataset(datasetId)) {
            datasets_to_read.insert(datasetId);
        }
    }

}

void ProjectDatasetINodeCache::refreshProjectIds(SConn connection, UISet datasets) {
    const NdbDictionary::Dictionary* metaDatabase = getDatabase(connection);
    NdbTransaction* metaTransaction = startNdbTransaction(connection);
    refreshProjectIds(metaDatabase, metaTransaction, datasets);
    connection->closeTransaction(metaTransaction);
}

void ProjectDatasetINodeCache::refreshProjectIds(const NdbDictionary::Dictionary* database,
        NdbTransaction* transaction, UISet dataset_ids) {    

    const NdbDictionary::Index * index= getIndex(database, DS, DS_COLS_TO_READ[DS_INODE_ID]);
    
    vector<NdbIndexScanOperation*> indexScanOps;
    UIRowMap rows;
    for (UISet::iterator it = dataset_ids.begin(); it != dataset_ids.end(); ++it) {
        NdbIndexScanOperation* op = getNdbIndexScanOperation(transaction, index);
        op->readTuples(NdbOperation::LM_CommittedRead);
       
        op->equal(DS_COLS_TO_READ[DS_INODE_ID], *it);
        
        NdbRecAttr* id_col = getNdbOperationValue(op, DS_COLS_TO_READ[DS_INODE_ID]);
        NdbRecAttr* proj_id_col = getNdbOperationValue(op, DS_COLS_TO_READ[DS_PROJECT_ID]);
        NdbRecAttr* shared_col = getNdbOperationValue(op, "shared");
        rows[*it].push_back(id_col);
        rows[*it].push_back(proj_id_col);
        rows[*it].push_back(shared_col);
        indexScanOps.push_back(op);
    }

    executeTransaction(transaction, NdbTransaction::NoCommit);

    int i = 0;
    for (UIRowMap::iterator it = rows.begin(); it != rows.end(); ++it, i++) {
        
        stringstream projs;
        UISet projectIds;
        while (indexScanOps[i]->nextResult(true) == 0) {
            if (it->first != it->second[0]->int32_value()) {
                LOG_ERROR("Dataset [" << it->first << "] doesn't exists");
                continue;
            }
            
            bool originalDs = it->second[2]->int8_value() == 0;
            
            if(!originalDs){
                continue;
            }
            
            int projectId = it->second[1]->int32_value();
            if(projectIds.empty()){
                addDatasetToProject(it->first, projectId);
            }
            projs << projectId << ",";
            projectIds.insert(projectId);
        }
        
        if(projectIds.size() > 1){
            LOG_ERROR("Got " << projectIds.size() << " rows of the original Dataset [" << it->first << "] in projects [" << projs.str() << "], only one was expected");
        }
    }
}

ProjectDatasetINodeCache::~ProjectDatasetINodeCache() {
}

