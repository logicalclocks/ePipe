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

#ifndef SCHEMABASEDMETADATAREADER_H
#define SCHEMABASEDMETADATAREADER_H

#include "MetadataLogTailer.h"
#include "NdbDataReaders.h"
#include <boost/lexical_cast.hpp>
#include "ProjectsElasticSearch.h"
#include "tables/SchemabasedMetadataTable.h"
#include "tables/MetadataLogTable.h"

class SchemabasedMetadataReader : public NdbDataReader<MetadataLogEntry, MConn> {
public:
  SchemabasedMetadataReader(MConn connection, const bool hopsworks, const int lru_cap);
  virtual ~SchemabasedMetadataReader();
private:
  virtual void processAddedandDeleted(MetaQ* data_batch, eBulk& bulk);
  void createJSON(SchemabasedMq* data_batch, eBulk& bulk);

  SchemabasedMetadataTable mSchemabasedTable;
  MetadataLogTable mMetadataLogTable;
};

class SchemabasedMetadataReaders : public NdbDataReaders<MetadataLogEntry, MConn>{
  public:
    SchemabasedMetadataReaders(MConn* connections, int num_readers, const bool hopsworks,
          ProjectsElasticSearch* elastic, const int lru_cap) : NdbDataReaders(elastic){
      for(int i=0; i<num_readers; i++){
        SchemabasedMetadataReader* dr = new SchemabasedMetadataReader(connections[i], hopsworks, lru_cap);
        dr->start(i, this);
        mDataReaders.push_back(dr);
      }
    }
};
#endif /* SCHEMABASEDMETADATAREADER_H */

