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
 * File:   ConcurrentUnorderedSet.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef CONCURRENTUNORDEREDSET_H
#define CONCURRENTUNORDEREDSET_H
#include "common.h"
#include <boost/optional.hpp>

template<typename Data, typename DataHash, typename DataEqual>
class ConcurrentUnorderedSet {
public:
  ConcurrentUnorderedSet();
  void add(const Data &data);
  void unsynchronized_add(const Data &data);
  boost::optional<Data> remove();
  boost::optional<Data> unsynchronized_remove();
  int size();
  int unsynchronized_size();
  virtual ~ConcurrentUnorderedSet();
private:
  mutable boost::mutex mLock;
  boost::unordered_set<Data, DataHash, DataEqual> mSet;

};

template<typename Data, typename DataHash, typename DataEqual>
ConcurrentUnorderedSet<Data, DataHash, DataEqual>::ConcurrentUnorderedSet() {

}

template<typename Data, typename DataHash, typename DataEqual>
void ConcurrentUnorderedSet<Data, DataHash, DataEqual>::add(const Data &data) {
  boost::mutex::scoped_lock lock(mLock);
  unsynchronized_add(data);
}

template<typename Data, typename DataHash, typename DataEqual>
void ConcurrentUnorderedSet<Data, DataHash, DataEqual>::unsynchronized_add(const Data &data) {
  mSet.insert(data);
}

template<typename Data, typename DataHash, typename DataEqual>
boost::optional<Data> ConcurrentUnorderedSet<Data, DataHash, DataEqual>::remove() {
  boost::mutex::scoped_lock lock(mLock);
  return unsynchronized_remove();
}

template<typename Data, typename DataHash, typename DataEqual>
boost::optional<Data> ConcurrentUnorderedSet<Data, DataHash, DataEqual>::unsynchronized_remove() {
  if (!mSet.empty()) {
    Data data = *mSet.begin();
    mSet.erase(mSet.begin());
    return data;
  }
  return boost::none;
}

template<typename Data, typename DataHash, typename DataEqual>
int ConcurrentUnorderedSet<Data, DataHash, DataEqual>::size() {
  boost::mutex::scoped_lock lock(mLock);
  return unsynchronized_size();
}

template<typename Data, typename DataHash, typename DataEqual>
int ConcurrentUnorderedSet<Data, DataHash, DataEqual>::unsynchronized_size() {
  return mSet.size();
}

template<typename Data, typename DataHash, typename DataEqual>
ConcurrentUnorderedSet<Data, DataHash, DataEqual>::~ConcurrentUnorderedSet() {

}

#endif /* CONCURRENTUNORDEREDSET_H */

