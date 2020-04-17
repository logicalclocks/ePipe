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

#ifndef FSMUTATIONSDATAREADER_H
#define FSMUTATIONSDATAREADER_H

#include "FsMutationsTableTailer.h"
#include "ProjectsElasticSearch.h"
#include "tables/INodeTable.h"
#include "tables/DatasetTable.h"
#include "NdbDataReaders.h"
#include "tables/XAttrTable.h"
#include "FileProvenanceConstants.h"

class FsMutationsDataReader : public NdbDataReader<FsMutationRow, MConn> {
public:
  FsMutationsDataReader(MConn connection, const bool hopsworks, const int lru_cap,
          const std::string search_index, const std::string featurestore_index);
  virtual ~FsMutationsDataReader();
private:
  INodeTable mInodesTable;
  DatasetTable mDatasetTable;
  ProjectTable mProjectTable;
  XAttrTable mXAttrTable;
  FsMutationsLogTable mFSLogTable;
  std::string mSearchIndex;
  std::string mFeaturestoreIndex;

  virtual void processAddedandDeleted(Fmq* data_batch, eBulk& bulk);

  void createJSON(Fmq* pending, INodeMap& inodes, XAttrMap& xattrs, eBulk& bulk);
  std::string docHeader(std::string index, Int64 id);
  std::string featurestoreDoc(std::string docType, Int64 inodeId, std::string name, int version, int projectId, std::string projectName, Int64 datasetIId);
};

class FsMutationsDataReaders : public NdbDataReaders<FsMutationRow, MConn>{
public:
  FsMutationsDataReaders(MConn* connections, int num_readers, const bool hopsworks,
          ProjectsElasticSearch* elastic, const int lru_cap, const std::string search_index,
          const std::string featurestore_index) : NdbDataReaders(elastic){
    for(int i=0; i< num_readers; i++){
      FsMutationsDataReader* dr = new FsMutationsDataReader(connections[i], hopsworks, lru_cap, search_index, featurestore_index);
      dr->start(i, this);
      mDataReaders.push_back(dr);
    }
  }
};


#endif /* FSMUTATIONSDATAREADER_H */

