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

#ifndef DATASETPROJECTCACHE_H
#define DATASETPROJECTCACHE_H

#include "Cache.h"


class DatasetProjectCache {
public:
    DatasetProjectCache();
    void addDatasetToProject(int datasetId, int projectId);
    void removeProject(int projectId);
    void removeDataset(int datasetId);
    int getProjectId(int datasetId);
    
    virtual ~DatasetProjectCache();
private:
    Cache<int, int> mDatasetToProject;
    Cache<int, UISet*> mProjectToDataset;
};

#endif /* DATASETPROJECTCACHE_H */

