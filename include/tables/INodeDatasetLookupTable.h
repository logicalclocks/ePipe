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

#ifndef INODEDATASETLOOKUPTABLE_H
#define INODEDATASETLOOKUPTABLE_H

#include "DBTable.h"

struct INodeDatasetLookupRow {
  Int64 mInodeId;
  Int64 mDatasetINodeId;
};

typedef boost::unordered_map<Int64, INodeDatasetLookupRow> INodeDatasetLookupMap;

class INodeDatasetLookupTable : public DBTable<INodeDatasetLookupRow> {
public:

  INodeDatasetLookupTable() : DBTable("hdfs_inode_dataset_lookup") {
    addColumn("inode_id");
    addColumn("dataset_id");
  }

  INodeDatasetLookupRow getRow(NdbRecAttr* values[]) {
    INodeDatasetLookupRow row;
    row.mInodeId = values[0]->int64_value();
    row.mDatasetINodeId = values[1]->int64_value();
    return row;
  }

  INodeDatasetLookupMap get(Ndb* connection, ULSet inodes_ids) {
    return doRead(connection, inodes_ids);
  }

};
#endif /* INODEDATASETLOOKUPTABLE_H */

