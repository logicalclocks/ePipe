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

class ElasticSearchBase : public TimedRestBatcher, public
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
#endif //EPIPE_ELASTICSEARCHBASE_H
