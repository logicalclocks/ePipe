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

#ifndef PROVENANCEDATAREADER_H
#define PROVENANCEDATAREADER_H

#include "NdbDataReaders.h"
#include "ProvenanceElasticSearch.h"

class ProvenanceDataReader : public NdbDataReader<ProvenanceRow, SConn, PKeys> {
public:
  ProvenanceDataReader(SConn connection, const bool hopsworks);
  virtual ~ProvenanceDataReader();
private:
  virtual void processAddedandDeleted(Pq* data_batch, PBulk& bulk);
};

class ProvenanceDataReaders :  public NdbDataReaders<ProvenanceRow, SConn, PKeys>{
  public:
    ProvenanceDataReaders(SConn* connections, int num_readers,const bool hopsworks,
          ProvenanceElasticSearch* elastic) : NdbDataReaders(elastic){
      for(int i=0; i<num_readers; i++){
        ProvenanceDataReader* dr = new ProvenanceDataReader(connections[i], hopsworks);
        dr->start(i, this);
        mDataReaders.push_back(dr);
      }
    }
};

#endif /* PROVENANCEDATAREADER_H */

