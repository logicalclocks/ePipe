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
 * File:   ElasticSearchBase.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef EPIPE_ELASTICSEARCHBASE_H
#define EPIPE_ELASTICSEARCHBASE_H

#include "TimedRestBatcher.h"

template<typename Keys>
class ElasticSearchBase : public TimedRestBatcher<Keys> {
public:
  ElasticSearchBase(string elastic_addr, int time_to_wait_before_inserting, int bulk_size);
  
  virtual ~ElasticSearchBase();

protected:

  string getElasticSearchUrlonIndex(string index);
  string getElasticSearchUrlOnDoc(string index, Int64 doc);
  string getElasticSearchUpdateDocUrl(string index, Int64 doc);
  string getElasticSearchBulkUrl(string index);
  string getElasticSearchDeleteByQuery(string index);
  virtual bool parseResponse(string response);

private:
  const string DEFAULT_TYPE;
  
};

template<typename Keys>
ElasticSearchBase<Keys>::ElasticSearchBase(string elastic_addr, int time_to_wait_before_inserting, int bulk_size)
: TimedRestBatcher<Keys>(elastic_addr, time_to_wait_before_inserting, bulk_size), DEFAULT_TYPE("_doc") {
}

template<typename Keys>
string ElasticSearchBase<Keys>::getElasticSearchUrlonIndex(string index) {
  string str = this->mEndpointAddr + "/" + index;
  return str;
}

template<typename Keys>
string ElasticSearchBase<Keys>::getElasticSearchUrlOnDoc(string index, Int64 doc) {
  stringstream out;
  out << getElasticSearchUrlonIndex(index) << "/" << DEFAULT_TYPE << "/" << doc;
  return out.str();
}

template<typename Keys>
string ElasticSearchBase<Keys>::getElasticSearchUpdateDocUrl(string index, Int64 doc) {
  string str = getElasticSearchUrlOnDoc(index, doc) + "/_update";
  return str;
}

template<typename Keys>
string ElasticSearchBase<Keys>::getElasticSearchBulkUrl(string index) {
  string str = this->mEndpointAddr + "/" + index + "/" + DEFAULT_TYPE + "/_bulk";
  return str;
}

template<typename Keys>
string ElasticSearchBase<Keys>::getElasticSearchDeleteByQuery(string index) {
  string str = this->mEndpointAddr + "/" + index + "/" + DEFAULT_TYPE + "/_delete_by_query";
  return str;
}

template<typename Keys>
bool ElasticSearchBase<Keys>::parseResponse(string response) {
  try {
    rapidjson::Document d;
    if (!d.Parse<0>(response.c_str()).HasParseError()) {
      if (d.HasMember("errors")) {
        const rapidjson::Value &bulkErrors = d["errors"];
        if (bulkErrors.IsBool() && bulkErrors.GetBool()) {
          const rapidjson::Value &items = d["items"];
          stringstream errors;
          for (rapidjson::SizeType i = 0; i < items.Size(); ++i) {
            const rapidjson::Value &obj = items[i];
            for (rapidjson::Value::ConstMemberIterator itr = obj.MemberBegin(); itr != obj.MemberEnd(); ++itr) {
              const rapidjson::Value & opObj = itr->value;
              if (opObj.HasMember("error")) {
                const rapidjson::Value & error = opObj["error"];
                if (error.IsObject()) {
                  const rapidjson::Value & errorType = error["type"];
                  const rapidjson::Value & errorReason = error["reason"];
                  errors << errorType.GetString() << ":" << errorReason.GetString();
                } else if (error.IsString()) {
                  errors << error.GetString();
                }
                errors << ", ";
              }
            }
          }
          string errorsStr = errors.str();
          LOG_ERROR(" ES got errors: " << errorsStr);
          return false;
        }
      } else if (d.HasMember("error")) {
        const rapidjson::Value &error = d["error"];
        if (error.IsObject()) {
          const rapidjson::Value & errorType = error["type"];
          const rapidjson::Value & errorReason = error["reason"];
          LOG_ERROR(" ES got error: " << errorType.GetString() << ":" << errorReason.GetString());
        } else if (error.IsString()) {
          LOG_ERROR(" ES got error: " << error.GetString());
        }
        return false;
      }
    } else {
      LOG_ERROR(" ES got json error (" << d.GetParseError() << ") while parsing (" << response << ")");
      return false;
    }

  } catch (std::exception &e) {
    LOG_ERROR(e.what());
    return false;
  }
  return true;
}

template<typename Keys>
ElasticSearchBase<Keys>::~ElasticSearchBase() {

}
#endif //EPIPE_ELASTICSEARCHBASE_H
