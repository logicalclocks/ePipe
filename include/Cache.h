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

//TODO: implement LRU, how to handle eviction?! since i'm using the cache to pass elements between functions

template<typename Key, typename Value>
class Cache {
public:
    Cache();
    void put(Key key, Value value);
    Value get(Key key);
    Value remove(Key key);
    bool contains(Key key);
    virtual ~Cache();
private:
    mutable boost::mutex mLock;
    boost::unordered_map<Key, Value> cache;
};

template<typename Key, typename Value>
Cache<Key,Value>::Cache(){
    
}

template<typename Key, typename Value>
Cache<Key,Value>::~Cache(){
    
}

template<typename Key, typename Value>
void Cache<Key,Value>::put(Key key, Value value){
    boost::mutex::scoped_lock lock(mLock);
    cache[key] = value;
}

template<typename Key, typename Value>
Value Cache<Key,Value>::get(Key key){
    boost::mutex::scoped_lock lock(mLock);
    return cache[key];
}

template<typename Key, typename Value>
Value Cache<Key,Value>::remove(Key key){
    boost::mutex::scoped_lock lock(mLock);
    Value val = cache[key];
    cache.erase(key);
    return val;
}

template<typename Key, typename Value>
bool Cache<Key,Value>::contains(Key key){
    boost::mutex::scoped_lock lock(mLock);
    return cache.find(key) != cache.end();
}
#endif /* CACHE_H */

