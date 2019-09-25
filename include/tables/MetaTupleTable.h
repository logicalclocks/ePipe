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

#ifndef METATUPLETABLE_H
#define METATUPLETABLE_H
#include "DBTable.h"

struct TupleRow {
  int mId;
  Int64 mInodeId;
};

typedef boost::unordered_map<int, TupleRow> TupleMap;

class MetaTupleTable : public DBTable<TupleRow> {
public:

  MetaTupleTable() : DBTable("meta_tuple_to_file") {
    addColumn("tupleid");
    addColumn("inodeid");
  }

  TupleRow getRow(NdbRecAttr* values[]) {
    TupleRow row;
    row.mId = values[0]->int32_value();
    row.mInodeId = values[1]->int64_value();
    return row;
  }

  TupleRow get(Ndb* connection, int tupleId){
    return doRead(connection, tupleId);
  }
  
  TupleMap get(Ndb* connection, UISet& tupleIds) {
    return doRead(connection, tupleIds);
  }
};
#endif /* METATUPLETABLE_H */

