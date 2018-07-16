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
 * File:   MetadataReader.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef SCHEMABASEDMETADATAREADER_H
#define SCHEMABASEDMETADATAREADER_H

#include "MetadataLogTailer.h"
#include "NdbDataReaders.h"
#include <boost/lexical_cast.hpp>
#include "ProjectsElasticSearch.h"
#include "tables/SchemabasedMetadataTable.h"

class SchemabasedMetadataReader : public NdbDataReader<MetadataLogEntry, MConn, FSKeys> {
public:
  SchemabasedMetadataReader(MConn connection, const bool hopsworks,
          ProjectsElasticSearch* elastic, const int lru_cap);
  virtual ~SchemabasedMetadataReader();
private:
  virtual void processAddedandDeleted(MetaQ* data_batch, FSBulk& bulk);
  void createJSON(SchemabasedMq* data_batch, FSBulk& bulk);

  SchemabasedMetadataTable mSchemabasedTable;
};

class SchemabasedMetadataReaders : public NdbDataReaders<MetadataLogEntry, MConn, FSKeys>{
  public:
    SchemabasedMetadataReaders(MConn* connections, int num_readers, const bool hopsworks,
          ProjectsElasticSearch* elastic, const int lru_cap){
      NdbDataReaders();
      for(int i=0; i<num_readers; i++){
        mDataReaders.push_back(new SchemabasedMetadataReader(connections[i], hopsworks, elastic, lru_cap));
      }
    }
};
#endif /* SCHEMABASEDMETADATAREADER_H */

