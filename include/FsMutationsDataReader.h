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
 * File:   FsMutationsDataReader.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef FSMUTATIONSDATAREADER_H
#define FSMUTATIONSDATAREADER_H

#include "FsMutationsTableTailer.h"
#include "ProjectsElasticSearch.h"
#include "tables/INodeTable.h"
#include "tables/DatasetTable.h"
#include "NdbDataReaders.h"
#include "tables/HopsworksUserTable.h"

class FsMutationsDataReader : public NdbDataReader<FsMutationRow, MConn, FSKeys> {
public:
  FsMutationsDataReader(MConn connection, const bool hopsworks, const int lru_cap);
  virtual ~FsMutationsDataReader();
private:
  INodeTable mInodesTable;
  DatasetTable mDatasetTable;
  HopsworksUserTable mHopsworksUserTable;

  virtual void processAddedandDeleted(Fmq* data_batch, FSBulk& bulk);

  void createJSON(Fmq* pending, INodeMap& inodes, FSBulk& bulk);
};

class FsMutationsDataReaders : public NdbDataReaders<FsMutationRow, MConn, FSKeys>{
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

