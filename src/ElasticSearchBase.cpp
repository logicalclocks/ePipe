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

#include "ElasticSearchBase.h"

ElasticSearchBase::ElasticSearchBase(const HttpClientConfig
elastic_client_config, int time_to_wait_before_inserting, int bulk_size,
const bool statsEnabled, MovingCountersSet* const
metricsCounters) : TimedRestBatcher(elastic_client_config,
    time_to_wait_before_inserting, bulk_size),  mStats
    (statsEnabled), mCounters(metricsCounters), DEFAULT_TYPE("_doc") {
}

std::string ElasticSearchBase::getElasticSearchBulkUrl(std::string index) {
  std::string str = "/" + index + "/" + DEFAULT_TYPE + "/_bulk";
  return str;
}

std::string ElasticSearchBase::getElasticSearchBulkUrl() {
  std::string str = "/_bulk";
  return str;
}

ParsingResponse ElasticSearchBase::parseResponse(std::string response) {
  ParsingResponse pr = {false, false, ""};
  try {
    LOG_DEBUG("ES Response: \n" << response);
    if(response == "Open Distro Security not initialized."){
      pr.mRetryable = true;
      LOG_DEBUG("Retry the request until Open Distro Security is initialized");
      return pr;
    }
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
          LOG_ERROR("ES got errors: " << errorsStr);
          pr.errorMsg = errorsStr;
          return pr;
        }
      } else if (d.HasMember("error")) {
        const rapidjson::Value &error = d["error"];
        if (error.IsObject()) {
          const rapidjson::Value & errorType = error["type"];
          const rapidjson::Value & errorReason = error["reason"];
          LOG_ERROR(" ES got error: " << errorType.GetString() << ":" << errorReason.GetString());
          std::stringstream errorS;
          errorS << errorType.GetString() << ":" << errorReason.GetString();
          pr.errorMsg = errorS.str();
        } else if (error.IsString()) {
          LOG_ERROR(" ES got error: " << error.GetString());
          pr.errorMsg = error.GetString();
        }
        return pr;
      }
    } else {
      LOG_ERROR(" ES got json error (" << d.GetParseError() << ") while parsing (" << response << ")");
      return pr;
    }

  } catch (std::exception &e) {
    LOG_ERROR(e.what());
    return pr;
  }
  pr.mSuccess = true;
  return pr;
}

ElasticSearchBase::~ElasticSearchBase() {

}

std::string ElasticSearchBase::getMetrics(){
  return mCounters->getMetrics(mCurrentQueueSize,
      mElasticConnetionFailed, mTimeElasticConnectionFailed);
}