/*
 * Copyright (C) 2018 Hops.io
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
 * File:   UserTable.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef USERTABLE_H
#define USERTABLE_H

#include "DBTable.h"
#include "Cache.h"

struct UserRow {

  UserRow() {

  }

  UserRow(int id, string name) {
    mId = id;
    mName = name;
  }
  int mId;
  string mName;
};

typedef CacheSingleton<Cache<int, UserRow> > UsersCache;
typedef boost::unordered_map<int, UserRow> UserMap;

class UserTable : public DBTable<UserRow> {
public:

  UserTable(int lru_cap) : DBTable("hdfs_users") {
    addColumn("id");
    addColumn("name");
    UsersCache::getInstance(lru_cap, "User");
  }

  UserRow getRow(NdbRecAttr* values[]) {
    UserRow row;
    row.mId = values[0]->int32_value();
    row.mName = get_string(values[1]);
    return row;
  }

  void updateUsersCache(Ndb* connection, UISet& ids) {
    UISet user_ids;
    for (UISet::iterator it = ids.begin(); it != ids.end(); ++it) {
      int id = *it;
      if (!UsersCache::getInstance().contains(id)) {
        user_ids.insert(id);
      }
    }

    if (user_ids.empty()) {
      LOG_DEBUG("All required users are already in the cache");
      return;
    }

    UserMap users = doRead(connection, user_ids);

    for (UserMap::iterator it = users.begin(); it != users.end(); ++it) {
      UserRow user = it->second;
      if (it->first != user.mId) {
        LOG_ERROR("User " << it->first << " doesn't exist, got userId "
                << user.mId << " was expecting " << it->first);
        continue;
      }
      LOG_DEBUG("ADD User [" << it->first << ", " << user.mName << "] to the Cache");
      UsersCache::getInstance().put(it->first, user);
    }
  }

  UserRow get(Ndb* connection, int id) {
    boost::optional<UserRow> user_ptr = UsersCache::getInstance().get(id);
    if (user_ptr) {
      UserRow user = user_ptr.get();
      LOG_DEBUG("got user (" << id << " -> " << user.mName << ") from the cache");
      return user;
    }
    LOG_DEBUG("get user from the database " << id);
    UserRow row = doRead(connection, id);
    UsersCache::getInstance().put(row.mId, row);
    return row;
  }

  string getFromCache(int id) {
    boost::optional<UserRow> user_ptr = UsersCache::getInstance().get(id);
    if (user_ptr) {
      return user_ptr.get().mName;
    }
    return DONT_EXIST_STR();
  }

};


#endif /* USERTABLE_H */

