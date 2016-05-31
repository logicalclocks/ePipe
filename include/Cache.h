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
 * File:   Cache.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef CACHE_H
#define CACHE_H
#include "common.h"
#include "boost/bimap.hpp"
#include "boost/bimap/list_of.hpp"
#include "boost/bimap/unordered_set_of.hpp"
#include "boost/optional.hpp"


/*
 * LRU Cache based on the design described in http://timday.bitbucket.org/lru.html
 */
template<typename Key, typename Value>
class Cache {
public:
    
    typedef boost::bimaps::bimap<boost::bimaps::unordered_set_of<Key>,
            boost::bimaps::list_of<Value> > CacheContainer;
    
    Cache();
    Cache(const int max_capacity);
    Cache(const int max_capacity, const char* trace_prefix);
    void put(Key key, Value value);
    boost::optional<Value> get(Key key);
    boost::optional<Value> remove(Key key);
    bool contains(Key key);
    virtual ~Cache();
        
private:
    const int mCapacity;
    const char* mTracePrefix;
    CacheContainer mCache;
    
    mutable boost::mutex mLock;
    
};

template<typename Key, typename Value>
Cache<Key,Value>::Cache() : mCapacity(DEFAULT_MAX_CAPACITY), mTracePrefix(""){
    
}

template<typename Key, typename Value>
Cache<Key,Value>::Cache(const int max_capacity) : mCapacity(max_capacity), mTracePrefix(""){
    
}

template<typename Key, typename Value>
Cache<Key,Value>::Cache(const int max_capacity, const char* trace_prefix) 
    : mCapacity(max_capacity), mTracePrefix(trace_prefix){
    
}

template<typename Key, typename Value>
Cache<Key,Value>::~Cache(){
    
}

template<typename Key, typename Value>
void Cache<Key,Value>::put(Key key, Value value){
    LOG_TRACE("PUT " << mTracePrefix << " [" << key << "]");
    boost::mutex::scoped_lock lock(mLock);
    const typename CacheContainer::left_iterator it = mCache.left.find(key);
    if(it == mCache.left.end()){
        //new key
        if(mCache.size() == mCapacity){
            LOG_TRACE( "EVICT " << mTracePrefix << " [" << mCache.right.begin()->second << "]");
            mCache.right.erase(mCache.right.begin());
        }
        mCache.insert(typename CacheContainer::value_type(key, value));
    }else{
        //update to most recent
        mCache.right.relocate(mCache.right.end(), mCache.project_right(it));
    }
}

template<typename Key, typename Value>
boost::optional<Value> Cache<Key,Value>::get(Key key){
    LOG_TRACE("GET " << mTracePrefix << " [" << key << "]");
    boost::mutex::scoped_lock lock(mLock);
    const typename CacheContainer::left_iterator it = mCache.left.find(key);
    if(it != mCache.left.end()){
        //update to most recent
        mCache.right.relocate(mCache.right.end(), mCache.project_right(it));
        return it->second;
    }
    return boost::none;
}

template<typename Key, typename Value>
boost::optional<Value> Cache<Key,Value>::remove(Key key){
    LOG_TRACE("REMOVE " << mTracePrefix << " [" << key << "] " << (mCache.size() - 1) << "/" << mCapacity);
    boost::mutex::scoped_lock lock(mLock);
    const typename CacheContainer::left_iterator it = mCache.left.find(key);
    if(it != mCache.left.end()){
        mCache.left.erase(it);
        return it->second;
    }
    return boost::none;
}

template<typename Key, typename Value>
bool Cache<Key,Value>::contains(Key key){
    LOG_TRACE("CONTAINS " << mTracePrefix << " [" << key << "]");
    boost::mutex::scoped_lock lock(mLock);
    const typename CacheContainer::left_iterator it = mCache.left.find(key);
    if(it != mCache.left.end()){
        //update to most recent
        mCache.right.relocate(mCache.right.end(), mCache.project_right(it));
        return true;
    }
    return false;
}

#endif /* CACHE_H */

