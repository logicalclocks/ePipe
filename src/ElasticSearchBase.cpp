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
#include "ElasticSearchBase.h"

ElasticSearchBase::ElasticSearchBase(std::string elastic_addr, int time_to_wait_before_inserting, int bulk_size)
    : TimedRestBatcher(elastic_addr, time_to_wait_before_inserting, bulk_size), DEFAULT_TYPE("_doc") {
}

std::string ElasticSearchBase::getElasticSearchUrlonIndex(std::string index) {
  std::string str = "/" + index;
  return str;
}

std::string ElasticSearchBase::getElasticSearchUrlOnDoc(std::string index, Int64 doc) {
  std::stringstream out;
  out << getElasticSearchUrlonIndex(index) << "/" << DEFAULT_TYPE << "/" << doc;
  return out.str();
}

std::string ElasticSearchBase::getElasticSearchUpdateDocUrl(std::string index, Int64 doc) {
  std::string str = getElasticSearchUrlOnDoc(index, doc) + "/_update";
  return str;
}

std::string ElasticSearchBase::getElasticSearchBulkUrl(std::string index) {
  std::string str = "/" + index + "/" + DEFAULT_TYPE + "/_bulk";
  return str;
}

bool ElasticSearchBase::parseResponse(std::string response) {
  try {
    LOG_DEBUG("ES Response: \n" << response);
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

ElasticSearchBase::~ElasticSearchBase() {

}

std::string ElasticSearchBase::getMetrics() const {
  return std::string();
}