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
 * File:   PairCache.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef PAIRCACHE_H
#define PAIRCACHE_H
#include "Cache.h"
#include "Utils.h"
#include "tables/DBTableBase.h"

class PairCache {
public:

  PairCache(int lru_cap, const char* prefix) : mKeyValue(lru_cap, prefix),
  mValueKeys(lru_cap, prefix) {

  }

  void addPair(Int64 key, int value) {
    mKeyValue.put(key, value);

    if (!mValueKeys.contains(value)) {
      mValueKeys.put(value, new ULSet());
    }
    mValueKeys.get(value).get()->insert(key);
    LOG_TRACE("ADD Key[" << key << "] to Value[" << value << "]");
  }

  int getValue(Int64 key) {
    int value = DONT_EXIST_INT();
    boost::optional<int> res = mKeyValue.get(key);
    if (!res) {
      LOG_TRACE("Value not in the cache for Key[" << key << "]");
      return value;
    }
    value = *res;
    LOG_TRACE("GOT Value[" << value << "] for Key[" << key << "]");
    return value;
  }

  ULSet getKeys(int value) {
    ULSet keys;
    ULSet* keysInCache = getKeysInternal(value);
    if (keysInCache != NULL) {
      keys.insert(keysInCache->begin(), keysInCache->end());
    }
    return keys;
  }

  void removeKey(Int64 key) {
    int value = getValue(key);
    mKeyValue.remove(key);
    if (value != DONT_EXIST_INT()) {
      ULSet* keysInCache = getKeysInternal(value);
      if (keysInCache != NULL) {
        keysInCache->erase(key);
      }
    }
    LOG_TRACE("REMOVE Key[" << key << "]");
  }

  ULSet removeValue(int value) {
    ULSet keys = getKeys(value);
    for (ULSet::iterator it = keys.begin(); it != keys.end(); ++it) {
      Int64 key = *it;
      removeKey(key);
    }
    return keys;
  }

  bool containsKey(int key) {
    return mKeyValue.contains(key);
  }

private:
  Cache<Int64, int> mKeyValue;
  Cache<int, ULSet*> mValueKeys;

  ULSet* getKeysInternal(int value) {
    boost::optional<ULSet*> res = mValueKeys.get(value);
    if (!res) {
      LOG_TRACE("Keys not in the cache for Value[" << value << "]");
      return NULL;
    }
    return *res;
  }
};

#endif /* PAIRCACHE_H */

