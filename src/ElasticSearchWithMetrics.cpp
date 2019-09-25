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
#include "ElasticSearchWithMetrics.h"

ElasticSearchWithMetrics::ElasticSearchWithMetrics(std::string elastic_addr, int time_to_wait_before_inserting, int bulk_size, const bool stats)
    : ElasticSearchBase(elastic_addr, time_to_wait_before_inserting, bulk_size), mStats(stats), mStartTime(Utils::getCurrentTime()) {
}

ElasticSearchWithMetrics::~ElasticSearchWithMetrics() {
}

std::string ElasticSearchWithMetrics::getMetrics() const {
  std::stringstream out;
  out << "up_seconds " << Utils::getTimeDiffInSeconds(mStartTime, Utils::getCurrentTime()) << std::endl;
  out << "epipe_elastic_queue_length " << mCurrentQueueSize << std::endl;

  if(mElasticConnetionFailed) {
    out << "epipe_elastic_connection_failed " << mElasticConnetionFailed << std::endl;
    out << "epipe_elastic_connection_failed_since_seconds " <<
        Utils::getTimeDiffInSeconds(mTimeElasticConnectionFailed, Utils::getCurrentTime()) << std::endl;
  }
  out << mCounters.getMetrics(mStartTime);
  return out.str();
}