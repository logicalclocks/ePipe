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

#ifndef APPPROVENANCEELASTIC_H
#define APPPROVENANCEELASTIC_H

#include "ElasticSearchWithMetrics.h"
#include "AppProvenanceTableTailer.h"

class AppProvenanceElastic : public ElasticSearchWithMetrics {
public:
  AppProvenanceElastic(std::string elastic_addr, std::string index,
          int time_to_wait_before_inserting, int bulk_size,
          const bool stats, SConn conn);

  virtual ~AppProvenanceElastic();
private:
  const std::string mIndex;
  std::string mElasticBulkAddr;
  SConn mConn;

  virtual void process(std::vector<eBulk>* bulks);
  bool bulkRequest(eEvent& event);
};

#endif /* APPPROVENANCEELASTIC_H */