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

#ifndef APPPROVENANCEELASTICDATAREADER_H
#define APPPROVENANCEELASTICDATAREADER_H

#include "NdbDataReaders.h"
#include "AppProvenanceTableTailer.h"
#include "boost/optional.hpp"
#include "boost/date_time.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"
#include "FileProvenanceConstants.h"
#include "AppProvenanceElastic.h"
#include "tables/AppProvenanceLogTable.h"

class AppProvenanceElasticDataReader : public NdbDataReader<AppProvenanceRow, SConn> {
public:
  AppProvenanceElasticDataReader(SConn connection, const bool hopsworks);
  virtual ~AppProvenanceElasticDataReader();
private:
  AppProvenanceLogTable mAppLogTable;
  void processAddedandDeleted(AppPq* data_batch, eBulk& bulk);
};

class AppProvenanceElasticDataReaders :  public NdbDataReaders<AppProvenanceRow, SConn>{
  public:
    AppProvenanceElasticDataReaders(SConn* connections, int num_readers,const bool hopsworks,
          TimedRestBatcher* restEndpoint) :
    NdbDataReaders(restEndpoint){
      for(int i=0; i<num_readers; i++){
        AppProvenanceElasticDataReader* dr = new AppProvenanceElasticDataReader(connections[i], hopsworks);
        dr->start(i, this);
        mDataReaders.push_back(dr);
      }
    }
};

#endif /* APPPROVENANCEELASTICDATAREADER_H */

