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

