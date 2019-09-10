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

#ifndef EPIPE_ELASTICSEARCHBASE_H
#define EPIPE_ELASTICSEARCHBASE_H

#include "TimedRestBatcher.h"
#include "http/server/MetricsProvider.h"

template<typename Keys>
class ElasticSearchBase : public TimedRestBatcher<Keys>, public
    MetricsProvider {
public:
  ElasticSearchBase(std::string elastic_addr, int time_to_wait_before_inserting, int bulk_size);
  
  virtual ~ElasticSearchBase();

  std::string getMetrics() const override;

protected:

  std::string getElasticSearchUrlonIndex(std::string index);
  std::string getElasticSearchUrlOnDoc(std::string index, Int64 doc);
  std::string getElasticSearchUpdateDocUrl(std::string index, Int64 doc);
  std::string getElasticSearchBulkUrl(std::string index);
  virtual bool parseResponse(std::string response);

private:
  const std::string DEFAULT_TYPE;
  
};

template<typename Keys>
ElasticSearchBase<Keys>::ElasticSearchBase(std::string elastic_addr, int time_to_wait_before_inserting, int bulk_size)
: TimedRestBatcher<Keys>(elastic_addr, time_to_wait_before_inserting, bulk_size), DEFAULT_TYPE("_doc") {
}

template<typename Keys>
std::string ElasticSearchBase<Keys>::getElasticSearchUrlonIndex(std::string index) {
  std::string str = "/" + index;
  return str;
}

template<typename Keys>
std::string ElasticSearchBase<Keys>::getElasticSearchUrlOnDoc(std::string index, Int64 doc) {
  std::stringstream out;
  out << getElasticSearchUrlonIndex(index) << "/" << DEFAULT_TYPE << "/" << doc;
  return out.str();
}

template<typename Keys>
std::string ElasticSearchBase<Keys>::getElasticSearchUpdateDocUrl(std::string index, Int64 doc) {
  std::string str = getElasticSearchUrlOnDoc(index, doc) + "/_update";
  return str;
}

template<typename Keys>
std::string ElasticSearchBase<Keys>::getElasticSearchBulkUrl(std::string index) {
  std::string str = "/" + index + "/" + DEFAULT_TYPE + "/_bulk";
  return str;
}

template<typename Keys>
bool ElasticSearchBase<Keys>::parseResponse(std::string response) {
  try {
    rapidjson::Document d;
    if (!d.Parse<0>(response.c_str()).HasParseError()) {
      if (d.HasMember("errors")) {
        const rapidjson::Value &bulkErrors = d["errors"];
        if (bulkErrors.IsBool() && bulkErrors.GetBool()) {
          const rapidjson::Value &items = d["items"];
          std::stringstream errors;
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
          std::string errorsStr = errors.str();
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

template<typename Keys>
std::string ElasticSearchBase<Keys>::getMetrics() const {
  return std::string();
}

#endif //EPIPE_ELASTICSEARCHBASE_H
