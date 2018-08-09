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
 * File:   NdbDataReader.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef NDBDATAREADER_H
#define NDBDATAREADER_H

#include "Cache.h"
#include "Utils.h"
#include "ElasticSearchBase.h"

using namespace Utils;

template<typename Data, typename Conn, typename Keys>
class NdbDataReader {
public:
  NdbDataReader(Conn connection, const bool hopsworks,
          ElasticSearchBase<Keys>* elastic);
  void processBatch(vector<Data>* data_batch);
  virtual ~NdbDataReader();

protected:
  Conn mNdbConnection;
  const bool mHopsworksEnalbed;
  virtual void processAddedandDeleted(vector<Data>* data_batch, Bulk<Keys>& bulk) = 0;
  
 private:
  ElasticSearchBase<Keys>* mElasticSearch;
};

template<typename Data, typename Conn, typename Keys>
NdbDataReader<Data, Conn, Keys>::NdbDataReader(Conn connection,
        const bool hopsworks, ElasticSearchBase<Keys>* elastic)
: mNdbConnection(connection), mHopsworksEnalbed(hopsworks), mElasticSearch(elastic) {
}

template<typename Data, typename Conn, typename Keys>
void NdbDataReader<Data, Conn, Keys>::processBatch(vector<Data>* data_batch) {
  Bulk<Keys> bulk;
  bulk.mStartProcessing = getCurrentTime();

  if (!data_batch->empty()) {
    processAddedandDeleted(data_batch, bulk);
  }

  bulk.mEndProcessing = getCurrentTime();

  if (!bulk.mJSON.empty()) {
    sort(bulk.mArrivalTimes.begin(), bulk.mArrivalTimes.end());
    mElasticSearch->addData(bulk);
  }

  LOG_INFO("[thread-" << boost::this_thread::get_id() << "] processing batch of size [" << data_batch->size() << "] took "
          << getTimeDiffInMilliseconds(bulk.mStartProcessing, bulk.mEndProcessing) << " msec");
}

template<typename Data, typename Conn, typename Keys>
NdbDataReader<Data, Conn, Keys>::~NdbDataReader() {

}

#endif /* NDBDATAREADER_H */

