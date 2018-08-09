/*
 * Copyright (C) 2018 Hops.io
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
 * File:   ProvenanceDataReader.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef PROVENANCEDATAREADER_H
#define PROVENANCEDATAREADER_H

#include "NdbDataReaders.h"
#include "ProvenanceElasticSearch.h"

class ProvenanceDataReader : public NdbDataReader<ProvenanceRow, SConn, PKeys> {
public:
  ProvenanceDataReader(SConn connection, const bool hopsworks,
          ProvenanceElasticSearch* elastic);
  virtual ~ProvenanceDataReader();
private:
  virtual void processAddedandDeleted(Pq* data_batch, PBulk& bulk);
};

class ProvenanceDataReaders :  public NdbDataReaders<ProvenanceRow, SConn, PKeys>{
  public:
    ProvenanceDataReaders(SConn* connections, int num_readers,const bool hopsworks,
          ProvenanceElasticSearch* elastic){
      NdbDataReaders();
      for(int i=0; i<num_readers; i++){
        mDataReaders.push_back(new ProvenanceDataReader(connections[i], hopsworks, elastic));
      }
    }
};

#endif /* PROVENANCEDATAREADER_H */

