/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * ePipe is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * ePipe is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef FSMUTATIONSDATAREADER_H
#define FSMUTATIONSDATAREADER_H

#include "FsMutationsTableTailer.h"
#include "ProjectsElasticSearch.h"
#include "tables/INodeTable.h"
#include "tables/DatasetTable.h"
#include "NdbDataReaders.h"
#include "tables/XAttrTable.h"

class FsMutationsDataReader : public NdbDataReader<FsMutationRow, MConn> {
public:
  FsMutationsDataReader(MConn connection, const bool hopsworks, const int lru_cap);
  virtual ~FsMutationsDataReader();
private:
  INodeTable mInodesTable;
  DatasetTable mDatasetTable;
  XAttrTable mXAttrTable;
  FsMutationsLogTable mFSLogTable;

  virtual void processAddedandDeleted(Fmq* data_batch, eBulk& bulk);

  void createJSON(Fmq* pending, INodeMap& inodes, XAttrMap& xattrs, eBulk&
  bulk);
};

class FsMutationsDataReaders : public NdbDataReaders<FsMutationRow, MConn>{
public:
  FsMutationsDataReaders(MConn* connections, int num_readers, const bool hopsworks,
          ProjectsElasticSearch* elastic, const int lru_cap) : NdbDataReaders(elastic){
    for(int i=0; i< num_readers; i++){
      FsMutationsDataReader* dr = new FsMutationsDataReader(connections[i], hopsworks, lru_cap);
      dr->start(i, this);
      mDataReaders.push_back(dr);
    }
  }
};


#endif /* FSMUTATIONSDATAREADER_H */

