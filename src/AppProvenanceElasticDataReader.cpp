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

#include "AppProvenanceElasticDataReader.h"

AppProvenanceElasticDataReader::AppProvenanceElasticDataReader(SConn connection, const bool hopsworks)
: NdbDataReader(connection, hopsworks) {
}

class Helper {
public:
  static std::list<std::string> process_row(AppProvenanceRow row) {
    LOG_DEBUG("app prov - processing:" << row.getPK().to_string());
    std::list<std::string> bulkOps;

    if(row.mFinishTime != 0) {
      AppProvenancePK finishId(row.mId, row.mState, row.mFinishTime);
      std::string finishLog = bulk_add_json(finishId, row.mName, row.mUser);
      bulkOps.push_back(finishLog);
    }
    AppProvenancePK runningId(row.mId, FileProvenanceConstants::APP_RUNNING_STATE, row.mStartTime);
    std::string runningLog = bulk_add_json(runningId, row.mName, row.mUser);
    bulkOps.push_back(runningLog);

    AppProvenancePK submitId(row.mId, FileProvenanceConstants::APP_SUBMITTED_STATE, row.mSubmitTime);
    std::string submitLog = bulk_add_json(submitId, row.mName, row.mUser);
    bulkOps.push_back(submitLog);

    return bulkOps;
  }

  static std::string bulk_add_json(AppProvenancePK key, std::string appName, std::string userName) {
    rapidjson::Document op;
    op.SetObject();
    rapidjson::Document::AllocatorType& opAlloc = op.GetAllocator();

    rapidjson::Value opVal(rapidjson::kObjectType);
    opVal.AddMember("_id", rapidjson::Value().SetString(key.to_string().c_str(), opAlloc), opAlloc);

    op.AddMember("update", opVal, opAlloc);

    rapidjson::Document data;
    data.SetObject();
    rapidjson::Document::AllocatorType& dataAlloc = data.GetAllocator();

    rapidjson::Value dataVal(rapidjson::kObjectType);

    std::string readable_t = readable_timestamp(key.mTimestamp);
    dataVal.AddMember("app_id",     rapidjson::Value().SetString(key.mId.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("app_state",  rapidjson::Value().SetString(key.mState.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("timestamp",  rapidjson::Value().SetInt64(key.mTimestamp), dataAlloc);
    dataVal.AddMember("readable_timestamp",  rapidjson::Value().SetString(readable_t.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("app_name",   rapidjson::Value().SetString(appName.c_str(), dataAlloc), dataAlloc);
    dataVal.AddMember("app_user",   rapidjson::Value().SetString(userName.c_str(), dataAlloc), dataAlloc);

    data.AddMember("doc", dataVal, dataAlloc);
    data.AddMember("doc_as_upsert", rapidjson::Value().SetBool(true), dataAlloc);

    rapidjson::StringBuffer opBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(opBuffer);
    op.Accept(opWriter);

    rapidjson::StringBuffer dataBuffer;
    rapidjson::Writer<rapidjson::StringBuffer> dataWriter(dataBuffer);
    data.Accept(dataWriter);
    
    std::stringstream out;
    out << opBuffer.GetString() << std::endl << dataBuffer.GetString();
    LOG_TRACE("app prov - elastic - add:" << out.str());
    return out.str();
  }

  static std::string readable_timestamp(Int64 timestamp) {
    using namespace boost::posix_time;
    using namespace boost::gregorian;
    time_t raw_t = (time_t)timestamp/1000; //time_t is time in seconds?
    ptime p_timestamp = from_time_t(raw_t);
    std::stringstream t_date;
    t_date << p_timestamp.date().year() << "." << p_timestamp.date().month() << "." << p_timestamp.date().day();
    std::stringstream t_time;
    t_time << p_timestamp.time_of_day().hours() << ":" << p_timestamp.time_of_day().minutes() << ":" << p_timestamp.time_of_day().seconds();
    std::stringstream readable_timestamp;
    readable_timestamp << t_date.str().c_str() << " " << t_time.str().c_str();
    return readable_timestamp.str();
  }
};

void AppProvenanceElasticDataReader::processAddedandDeleted(AppPq* data_batch, eBulk& bulk) {
  std::stringstream out;
  for (AppPq::iterator it = data_batch->begin(); it != data_batch->end(); ++it) {
    AppProvenanceRow row = *it;
    std::list<std::string> result = Helper::process_row(row);
    for(std::string op : result) {
      out << op << std::endl;
    }
    bulk.push(mAppLogTable.getLogRemovalHandler(row), row.mEventCreationTime, out.str());
  }
}

AppProvenanceElasticDataReader::~AppProvenanceElasticDataReader() {
  
}