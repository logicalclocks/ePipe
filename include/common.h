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
 * File:   common.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <istream>
#include <ostream>
#include <string>
#include <NdbApi.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/pthread/mutex.hpp>
#include <boost/thread/pthread/condition_variable_fwd.hpp>
#include <boost/unordered/unordered_set.hpp>
#include <boost/unordered/unordered_map.hpp>
#include "vector"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/document.h"
#include "queue"


typedef std::queue<int> IQueue;
typedef std::vector<int> IVec;
typedef std::vector<std::string> StrVec;
typedef boost::unordered_set<int> UISet;

//long
typedef boost::unordered_set<Int64> ULSet;
typedef std::vector<Int64> LVec;

//Constansts

#define RETRIES 5
#define DELAY_BETWEEN_RETRIES 3
#define VERBOSE 0
#define WAIT_UNTIL_READY 30
#define DEFAULT_MAX_CAPACITY 10000

struct TableUnitConf {
  int mWaitTime;
  int mBatchSize;
  int mNumReaders;

  TableUnitConf() {
    mWaitTime = 0;
    mBatchSize = 0;
    mNumReaders = 0;
  }

  TableUnitConf(int wait_time, int batch_size, int num_readers) {
    mWaitTime = wait_time;
    mBatchSize = batch_size;
    mNumReaders = num_readers;
  }

  IVec getVector() {
    IVec d;
    d.push_back(mWaitTime);
    d.push_back(mBatchSize);
    d.push_back(mNumReaders);
    return d;
  }

  void update(std::vector<int> v) {
    if (v.size() == 3) {
      mWaitTime = v[0];
      mBatchSize = v[1];
      mNumReaders = v[2];
    }
  }

  std::string getString() {
    std::stringstream str;
    str << mWaitTime << " " << mBatchSize << " " << mNumReaders;
    return str.str();
  }

  bool isEnabled() const {
    return mWaitTime > 0 && mBatchSize > 0 && mNumReaders > 0;
  }
};

#endif /* COMMON_H */

