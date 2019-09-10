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

#ifndef GROUPTABLE_H
#define GROUPTABLE_H

#include "DBTable.h"
#include "Cache.h"

struct GroupRow {

  GroupRow() {

  }

  GroupRow(int id, std::string name) {
    mId = id;
    mName = name;
  }
  int mId;
  std::string mName;
};

typedef CacheSingleton<Cache<int, GroupRow> > GroupsCache;
typedef boost::unordered_map<int, GroupRow> GroupMap;

class GroupTable : public DBTable<GroupRow> {
public:

  GroupTable(int lru_cap) : DBTable("hdfs_groups") {
    addColumn("id");
    addColumn("name");
    GroupsCache::getInstance(lru_cap, "Group");
  }

  GroupRow getRow(NdbRecAttr* values[]) {
    GroupRow row;
    row.mId = values[0]->int32_value();
    row.mName = get_string(values[1]);
    return row;
  }

  void updateGroupsCache(Ndb* connection, UISet& ids) {
    UISet group_ids;
    for (UISet::iterator it = ids.begin(); it != ids.end(); ++it) {
      int id = *it;
      if (!GroupsCache::getInstance().contains(id)) {
        group_ids.insert(id);
      }
    }

    if (group_ids.empty()) {
      LOG_DEBUG("All required groups are already in the cache");
      return;
    }

    GroupMap groups = doRead(connection, group_ids);

    for (GroupMap::iterator it = groups.begin(); it != groups.end(); ++it) {
      GroupRow group = it->second;
      if (it->first != group.mId) {
        LOG_ERROR("Group " << it->first << " doesn't exist, got groupId "
                << group.mId << " was expecting " << it->first);
        continue;
      }
      LOG_DEBUG("ADD Group [" << it->first << ", " << group.mName << "] to the Cache");
      GroupsCache::getInstance().put(it->first, group);
    }
  }

  GroupRow get(Ndb* connection, int id) {
    boost::optional<GroupRow> group_ptr = GroupsCache::getInstance().get(id);
    if (group_ptr) {
      GroupRow group = group_ptr.get();
      LOG_DEBUG("got user (" << id << " -> " << group.mName << ") from the cache");
      return group;
    }
    LOG_DEBUG("get group from the database " << id);
    GroupRow row = doRead(connection, id);
    GroupsCache::getInstance().put(row.mId, row);
    return row;
  }

  std::string getFromCache(int id) {
    boost::optional<GroupRow> group_ptr = GroupsCache::getInstance().get(id);
    if (group_ptr) {
      return group_ptr.get().mName;
    }
    return DONT_EXIST_STR();
  }

};
#endif /* GROUPTABLE_H */

