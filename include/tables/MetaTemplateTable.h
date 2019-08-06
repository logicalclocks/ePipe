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
 * File:   MetaTemplateTable.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef METATEMPLATETABLE_H
#define METATEMPLATETABLE_H

#include "DBTable.h"
#include "MetadataLogTable.h"

struct TemplateRow {
  int mId;
  std::string mName;

  std::string to_delete_json() {
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    docWriter.StartObject();

    docWriter.String("script");

    std::stringstream scriptStream;
    scriptStream << "ctx._source[\"" << XATTR_FIELD_NAME << "\"].remove(\"" << mName << "\")";
    docWriter.String(scriptStream.str().c_str());

    docWriter.EndObject();

    return std::string(sbDoc.GetString());
  }
};

typedef CacheSingleton<Cache<int, TemplateRow> > TemplatesCache;

typedef boost::unordered_map<int, TemplateRow> TemplateMap;

class MetaTemplateTable : public DBTable<TemplateRow> {
public:

  MetaTemplateTable(int lru_cap) : DBTable("meta_templates") {
    addColumn("templateid");
    addColumn("name");
    TemplatesCache::getInstance(lru_cap, "Template");
  }

  TemplateRow getRow(NdbRecAttr* values[]) {
    TemplateRow row;
    row.mId = values[0]->int32_value();
    row.mName = get_string(values[1]);
    return row;
  }

  boost::optional<TemplateRow> get(Ndb* connection, int templateId) {
    boost::optional<TemplateRow> trow = getFromCache(templateId);
    if (trow) {
      return trow;
    }
    TemplateRow row = doRead(connection, templateId);
    if (row.mId == templateId) {
      TemplatesCache::getInstance().put(row.mId, row);
      return row;
    }
    return boost::none;
  }

  bool contains(int templateId) {
    return TemplatesCache::getInstance().contains(templateId);
  }

  boost::optional<TemplateRow> getFromCache(int templateId) {
    return TemplatesCache::getInstance().get(templateId);
  }

  void updateCache(Ndb* connection, UISet& templateIds) {
    UISet templates_to_read;
    for (UISet::iterator it = templateIds.begin(); it != templateIds.end(); ++it) {
      int templateId = *it;
      if (!contains(templateId)) {
        templates_to_read.insert(templateId);
      }
    }

    if (templates_to_read.empty()) {
      LOG_DEBUG("All required templates are already in the cache");
      return;
    }

    TemplateMap templates = doRead(connection, templates_to_read);
    for (TemplateMap::iterator it = templates.begin(); it != templates.end(); ++it) {
      TemplateRow temp = it->second;
      if (it->first != temp.mId) {
        // TODO: update elastic?!            
        LOG_ERROR("Template " << it->first << " doesn't exist, got templateId "
                << temp.mId << " was expecting " << it->first);
        continue;
      }

      TemplatesCache::getInstance().put(it->first, temp);
    }
  }

};
#endif /* METATEMPLATETABLE_H */

