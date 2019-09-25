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

#ifndef FILEPROVENANCEELASTIC_H
#define FILEPROVENANCEELASTIC_H

#include "ElasticSearchWithMetrics.h"
#include "FileProvenanceTableTailer.h"
#include "tables/FileProvenanceLogTable.h"
#include "tables/FileProvenanceXAttrBufferTable.h"

class FileProvenanceElastic : public ElasticSearchWithMetrics {
public:
  FileProvenanceElastic(std::string elastic_addr,int time_to_wait_before_inserting, int bulk_size,
      const bool stats, SConn conn);

  virtual ~FileProvenanceElastic();
private:
  SConn mConn;

  void intProcessOneByOne(eBulk bulk);
  bool intProcessBatch(std::string val, std::vector<eBulk>* bulks, std::vector<const LogHandler*> cleanupHandlers, ptime start_time);
  virtual void process(std::vector<eBulk>* bulks);
};

template <typename Iter>
Iter next(Iter iter)
{
  return ++iter;
};

template <typename Iter, typename Cont>
bool is_last(Iter iter, const Cont& cont)
{
  return (iter != cont.end()) && (next(iter) == cont.end());
};
#endif /* FILEPROVENANCEELASTIC_H */

