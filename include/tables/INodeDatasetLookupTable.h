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
 * File:   INodeDatasetLookupTable.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef INODEDATASETLOOKUPTABLE_H
#define INODEDATASETLOOKUPTABLE_H

#include "DBTable.h"

struct INodeDatasetLookupRow {
  int mInodeId;
  int mDatasetId;
};

typedef boost::unordered_map<int, INodeDatasetLookupRow> INodeDatasetLookupMap;

class INodeDatasetLookupTable : public DBTable<INodeDatasetLookupRow> {
public:

  INodeDatasetLookupTable() : DBTable("hdfs_inode_dataset_lookup") {
    addColumn("inode_id");
    addColumn("dataset_id");
  }

  INodeDatasetLookupRow getRow(NdbRecAttr* values[]) {
    INodeDatasetLookupRow row;
    row.mInodeId = values[0]->int32_value();
    row.mDatasetId = values[1]->int32_value();
    return row;
  }

  INodeDatasetLookupMap get(Ndb* connection, UISet inodes_ids) {
    return doRead(connection, inodes_ids);
  }

};
#endif /* INODEDATASETLOOKUPTABLE_H */

