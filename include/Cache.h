/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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

#ifndef CACHE_H
#define CACHE_H
#include "Utils.h"
#include "boost/bimap.hpp"
#include "boost/bimap/list_of.hpp"
#include "boost/bimap/unordered_set_of.hpp"
#include "boost/optional.hpp"

template<typename T>
class CacheSingleton {
public:

  static T& getInstance(const int lru_cap = DEFAULT_MAX_CAPACITY, const char* prefix = "") {
    static T instance(lru_cap, prefix);
    return instance;
  }
private:

  CacheSingleton() {
  }
  CacheSingleton(CacheSingleton const&);
  void operator=(CacheSingleton const&);
};

/*
 * LRU Cache based on the design described in http://timday.bitbucket.org/lru.html
 */
template<typename Key, typename Value>
class Cache {
public:

  typedef boost::bimaps::bimap<boost::bimaps::unordered_set_of<Key>,
  boost::bimaps::list_of<Value> > CacheContainer;
  typedef typename CacheContainer::size_type cache_size_type;

  Cache();
  Cache(const int max_capacity);
  Cache(const int max_capacity, const char* trace_prefix);
  void put(Key key, Value value);
  boost::optional<Value> get(Key key);
  void remove(Key key);
  bool contains(Key key);
  void stats();
  virtual ~Cache();

private:
  const cache_size_type mCapacity;
  const char* mTracePrefix;
  CacheContainer mCache;

  int mHits;
  int mMisses;

  int mEvictions;
  int mInserts;

  mutable boost::mutex mLock;

};

template<typename Key, typename Value>
Cache<Key, Value>::Cache() : mCapacity(DEFAULT_MAX_CAPACITY), mTracePrefix(""),
mHits(0), mMisses(0), mEvictions(0), mInserts(0) {
  LOG_INFO(mTracePrefix << " Cache created with Capacity of " << mCapacity);
}

template<typename Key, typename Value>
Cache<Key, Value>::Cache(const int max_capacity) : mCapacity(max_capacity),
mTracePrefix(""), mHits(0), mMisses(0), mEvictions(0), mInserts(0) {
  LOG_INFO(mTracePrefix << " Cache created with Capacity of " << mCapacity);
}

template<typename Key, typename Value>
Cache<Key, Value>::Cache(const int max_capacity, const char* trace_prefix)
: mCapacity(max_capacity), mTracePrefix(trace_prefix), mHits(0), mMisses(0),
mEvictions(0), mInserts(0) {
  LOG_INFO(mTracePrefix << " Cache created with Capacity of " << mCapacity);
}

template<typename Key, typename Value>
Cache<Key, Value>::~Cache() {

}

template<typename Key, typename Value>
void Cache<Key, Value>::put(Key key, Value value) {
  LOG_TRACE("PUT " << mTracePrefix << " [" << key << "]");
  boost::mutex::scoped_lock lock(mLock);
  const typename CacheContainer::left_iterator it = mCache.left.find(key);
  if (it == mCache.left.end()) {
    //new key
    if (mCache.size() == mCapacity) {
      LOG_TRACE("EVICT " << mTracePrefix << " [" << mCache.right.begin()->second << "]");
      mCache.right.erase(mCache.right.begin());
      mEvictions++;
    }
    mCache.insert(typename CacheContainer::value_type(key, value));
    mInserts++;
  } else {
    //update to most recent
    mCache.right.relocate(mCache.right.end(), mCache.project_right(it));
  }
}

template<typename Key, typename Value>
boost::optional<Value> Cache<Key, Value>::get(Key key) {
  LOG_TRACE("GET " << mTracePrefix << " [" << key << "]");
  boost::mutex::scoped_lock lock(mLock);
  const typename CacheContainer::left_iterator it = mCache.left.find(key);
  if (it != mCache.left.end()) {
    //update to most recent
    mCache.right.relocate(mCache.right.end(), mCache.project_right(it));
    mHits++;
    return it->second;
  }
  mMisses++;
  return boost::none;
}

template<typename Key, typename Value>
void Cache<Key, Value>::remove(Key key) {
  LOG_TRACE("REMOVE " << mTracePrefix << " [" << key << "] " << (mCache.size() - 1) << "/" << mCapacity);
  boost::mutex::scoped_lock lock(mLock);
  const typename CacheContainer::left_iterator it = mCache.left.find(key);
  if (it != mCache.left.end()) {
    mCache.left.erase(it);
  }
}

template<typename Key, typename Value>
bool Cache<Key, Value>::contains(Key key) {
  LOG_TRACE("CONTAINS " << mTracePrefix << " [" << key << "]");
  boost::mutex::scoped_lock lock(mLock);
  const typename CacheContainer::left_iterator it = mCache.left.find(key);
  if (it != mCache.left.end()) {
    //update to most recent
    mCache.right.relocate(mCache.right.end(), mCache.project_right(it));
    mHits++;
    return true;
  }
  mMisses++;
  return false;
}

template<typename Key, typename Value>
void Cache<Key, Value>::stats() {
  float hitsRate = (mHits * 100.0) / (mHits + mMisses);
  float missesRate = (mMisses * 100.0) / (mHits + mMisses);
  float evictionsRate = (mEvictions * 100.0) / mInserts;

  LOG_INFO(mTracePrefix << " Cache Stats: Hits=" << hitsRate << ", Misses="
          << missesRate << ", EvictionsRate=" << evictionsRate << ", Size=" << mCache.size() << "/" << mCapacity);
}
#endif /* CACHE_H */

