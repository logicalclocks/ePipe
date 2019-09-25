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

#ifndef METATABLETABLE_H
#define METATABLETABLE_H
#include <boost/optional/optional.hpp>

#include "DBTable.h"
#include "MetaTemplateTable.h"

struct TableRow {
  int mId;
  std::string mName;
  TemplateRow mTemplate;
};

typedef CacheSingleton<Cache<int, TableRow> > TablesCache;
typedef boost::unordered_map<int, TableRow> TableMap;

class MetaTableTable : public DBTable<TableRow> {
public:

  MetaTableTable(int lru_cap) : DBTable("meta_tables"),
  mTemplatesTable(lru_cap) {
    addColumn("tableid");
    addColumn("name");
    addColumn("templateid");
    TablesCache::getInstance(lru_cap, "Table");
  }

  TableRow getRow(NdbRecAttr* values[]) {
    TableRow row;
    row.mId = values[0]->int32_value();
    row.mName = get_string(values[1]);
    row.mTemplate.mId = values[2]->int32_value();
    return row;
  }

  bool contains(int tableId) {
    return TablesCache::getInstance().contains(tableId);
  }

  boost::optional<TableRow> getFromCache(int tableId) {
    return TablesCache::getInstance().get(tableId);
  }

  void updateCache(Ndb* connection, UISet& tableIds) {
    UISet tables_to_read;
    for (UISet::iterator it = tableIds.begin(); it != tableIds.end(); ++it) {
      int tableId = *it;
      if (!contains(tableId)) {
        tables_to_read.insert(tableId);
      }
    }

    if (tables_to_read.empty()) {
      LOG_DEBUG("All required tables are already in the cache");
      return;
    }

    TableMap tables = doRead(connection, tables_to_read);

    UISet templates_ids;
    for (TableMap::iterator it = tables.begin(); it != tables.end(); ++it) {
      TableRow table = it->second;
      if (it->first != table.mId) {
        // TODO: update elastic?!            
        LOG_ERROR("Table " << it->first << " doesn't exist, got tableId "
                << table.mId << " was expecting " << it->first);
        continue;
      }

      templates_ids.insert(table.mTemplate.mId);
    }

    mTemplatesTable.updateCache(connection, templates_ids);

    for (TableMap::iterator it = tables.begin(); it != tables.end(); ++it) {
      TableRow table = it->second;
      if (it->first != table.mId) {
        continue;
      }

      boost::optional<TemplateRow> temp = mTemplatesTable.getFromCache(table.mTemplate.mId);
      if (temp) {
        table.mTemplate = temp.get();
        TablesCache::getInstance().put(it->first, table);
      }
    }

  }

private:
  MetaTemplateTable mTemplatesTable;

};
#endif /* METATABLETABLE_H */

