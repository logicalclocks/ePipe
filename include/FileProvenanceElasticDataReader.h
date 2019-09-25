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

#ifndef FILEPROVENANCEELASTICDATAREADER_H
#define FILEPROVENANCEELASTICDATAREADER_H

#include "NdbDataReaders.h"
#include "FileProvenanceTableTailer.h"
#include "boost/date_time.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "tables/XAttrTable.h"
#include "tables/FileProvenanceXAttrBufferTable.h"
#include "tables/INodeTable.h"
#include "FileProvenanceConstants.h"
#include "FileProvenanceElastic.h"

typedef boost::tuple<std::list<std::string>, FileProvenancePK, boost::optional<FPXAttrBufferPK> > ProcessRowResult;

class FileProvenanceElasticDataReader : public NdbDataReader<FileProvenanceRow, SConn> {
public:
  FileProvenanceElasticDataReader(SConn connection, const bool hopsworks);
  virtual ~FileProvenanceElasticDataReader();
private:
  FileProvenanceLogTable mFileLogTable;
  FileProvenanceXAttrBufferTable mXAttrBuffer;

  void processAddedandDeleted(Pq* data_batch, eBulk& bulk);
  ProcessRowResult process_row(FileProvenanceRow row);
  FPXAttrBufferRow readBufferedXAttr(FPXAttrBufferPK xattrBufferKey);
  FileProvenanceConstants::ProvOpStoreType readProvType(FileProvenanceRow row);
  boost::optional<FPXAttrBufferRow> getProvCore(FPXAttrVersionsK versionsKey);
};

class FileProvenanceElasticDataReaders :  public NdbDataReaders<FileProvenanceRow, SConn>{
  public:
    FileProvenanceElasticDataReaders(SConn* connections, int num_readers,const bool hopsworks,
          TimedRestBatcher* restEndpoint) :
    NdbDataReaders(restEndpoint){
      for(int i=0; i<num_readers; i++){
        FileProvenanceElasticDataReader* dr = new FileProvenanceElasticDataReader(connections[i], hopsworks);
        dr->start(i, this);
        mDataReaders.push_back(dr);
      }
    }
};

#endif /* FILEPROVENANCEELASTICDATAREADER_H */

