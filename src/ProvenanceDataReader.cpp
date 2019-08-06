/*
 * Copyright (C) 2018 Logical Clocks AB
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
 * File:   ProvenanceDataReader.cpp
 * Author: Mahmoud Ismail <maism@kth.se>
 * 
 */

#include "ProvenanceDataReader.h"

ProvenanceDataReader::ProvenanceDataReader(SConn connection, const bool hopsworks)
: NdbDataReader(connection, hopsworks) {
}

void ProvenanceDataReader::processAddedandDeleted(Pq* data_batch, PBulk& bulk) {
  std::vector<ptime> arrivalTimes(data_batch->size());
  std::stringstream out;
  int i = 0;
  for (Pq::iterator it = data_batch->begin(); it != data_batch->end(); ++it, i++) {
    ProvenanceRow row = *it;
    arrivalTimes[i] = row.mEventCreationTime;
    ProvenancePK rowPK = row.getPK();
    bulk.mPKs.push_back(rowPK);

    out << row.to_create_json() << std::endl;
  }

  bulk.mArrivalTimes = arrivalTimes;
  bulk.mJSON = out.str();
}

ProvenanceDataReader::~ProvenanceDataReader() {
  
}

