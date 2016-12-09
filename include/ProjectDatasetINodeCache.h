/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   DatasetProjectCache.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 * Created on May 17, 2016, 2:16 PM
 */

#ifndef PROJECTDATASETINODECACHE_H
#define PROJECTDATASETINODECACHE_H

#include "Cache.h"
#include "Utils.h"

class ProjectDatasetINodeCache {
public:
    ProjectDatasetINodeCache(const int lru_cap);
    void addINodeToDataset(int inodeId, int datasetId);
    void addDatasetToProject(int datasetId, int projectId);
    void removeINode(int inodeId);
    void removeProject(int projectId);
    void removeDataset(int datasetId);
    int getProjectId(int datasetId);
    int getDatasetId(int inodeId);

    bool containsINode(int inodeId);
    bool containsDataset(int datasetId);


    void refresh(MConn connection, UISet inodes);
    void refresh(SConn inodeConnection, const NdbDictionary::Dictionary* metaDatabase,
            NdbTransaction* metaTransaction, UISet inodes);

    void refreshProjectIds(SConn connection, UISet datasets);

    virtual ~ProjectDatasetINodeCache();
private:
    UISet refreshDatasetIds(SConn connection, UISet inodes);

    void refreshProjectIds(const NdbDictionary::Dictionary* database, NdbTransaction* transaction,
            UISet dataset_ids);
    void readINodeToDatasetLookup(const NdbDictionary::Dictionary* inodesDatabase,
            NdbTransaction* inodesTransaction, UISet inodes_ids, UISet& datasets_to_read);

    Cache<int, int> mINodeToDataset;
    Cache<int, UISet*> mDatasetToINodes;
    Cache<int, int> mDatasetToProject;
    Cache<int, UISet*> mProjectToDataset;
};

#endif /* PROJECTDATASETINODECACHE_H */

