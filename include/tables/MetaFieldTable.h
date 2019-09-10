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

#ifndef METAFIELDTABLE_H
#define METAFIELDTABLE_H

#include <boost/optional/optional.hpp>

#include "DBTable.h"
#include "MetaTableTable.h"

enum FieldType {
  BOOL = 0,
  INT = 1,
  DOUBLE = 2,
  TEXT = 3
};

struct FieldRow {
  int mId;
  std::string mName;
  bool mSearchable;
  TableRow mTable;
  FieldType mType;
  
  FieldRow(){
    
  }
  FieldRow(int fieldId){
    mId = fieldId;
    mName = DONT_EXIST_STR();
  }
  
  bool is_empty(){
    return mName == DONT_EXIST_STR();
  }
  
};

typedef CacheSingleton<Cache<int, FieldRow> > FieldsCache;

typedef boost::unordered_map<int, FieldRow> FieldMap;

class MetaFieldTable : public DBTable<FieldRow> {
public:

  MetaFieldTable(int lru_cap) : DBTable("meta_fields"),
  mTablesTable(lru_cap) {
    addColumn("fieldid");
    addColumn("name");
    addColumn("tableid");
    addColumn("searchable");
    addColumn("ftype");
    FieldsCache::getInstance(lru_cap, "Field");
  }

  FieldRow getRow(NdbRecAttr* values[]) {
    FieldRow row;
    row.mId = values[0]->int32_value();
    row.mName = get_string(values[1]);
    row.mTable.mId = values[2]->int32_value();
    row.mSearchable = values[3]->int8_value() == 1;
    row.mType = (FieldType) values[4]->short_value();
    return row;
  }

  bool contains(int fieldId) {
    return FieldsCache::getInstance().contains(fieldId);
  }

  FieldRow get(Ndb* connection, int fieldId){
    UISet fields;
    fields.insert(fieldId);
    updateCache(connection, fields);
    boost::optional<FieldRow> field_ptr = getFromCache(fieldId);
    FieldRow field = field_ptr ? field_ptr.get() : FieldRow(fieldId);
    return field;
  }
  
  boost::optional<FieldRow> getFromCache(int fieldId) {
    return FieldsCache::getInstance().get(fieldId);
  }

  void updateCache(Ndb* connection, UISet& fieldIds) {
    UISet fields_to_read;
    for (UISet::iterator it = fieldIds.begin(); it != fieldIds.end(); ++it) {
      int fieldId = *it;
      if (!contains(fieldId)) {
        fields_to_read.insert(fieldId);
      }
    }

    if (fields_to_read.empty()) {
      LOG_DEBUG("All required fields are already in the cache");
      return;
    }

    FieldMap fields = doRead(connection, fields_to_read);

    UISet tables_ids;
    for (FieldMap::iterator it = fields.begin(); it != fields.end(); ++it) {
      FieldRow field = it->second;
      if (it->first != field.mId) {
        // TODO: update elastic?!            
        LOG_ERROR("Field " << it->first << " doesn't exist, got fieldId "
                << field.mId << " was expecting " << it->first);
        continue;
      }

      if (field.mSearchable) {
        tables_ids.insert(field.mTable.mId);
      }
    }

    mTablesTable.updateCache(connection, tables_ids);

    for (FieldMap::iterator it = fields.begin(); it != fields.end(); ++it) {
      FieldRow field = it->second;
      if (it->first != field.mId) {
        continue;
      }

      int table_id = field.mTable.mId;
      boost::optional<TableRow> table_ptr = mTablesTable.getFromCache(table_id);
      if (table_ptr && field.mSearchable) {
        field.mTable = table_ptr.get();
        FieldsCache::getInstance().put(it->first, field);
      }
    }
  }

private:
  MetaTableTable mTablesTable;
};


#endif /* METAFIELDTABLE_H */

