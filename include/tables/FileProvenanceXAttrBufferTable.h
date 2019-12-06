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

#ifndef FILEPROVENANCEXATTRBUFFERTABLE_H
#define FILEPROVENANCEXATTRBUFFERTABLE_H

#include "XAttrTable.h"

struct FPXAttrBufferPK {
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;
  int mInodeLogicalTime;

  FPXAttrBufferPK(Int64 inodeId, Int8 ns, std::string name, int inodeLogicalTime) {
    mInodeId = inodeId;
    mNamespace = ns;
    mName = name;
    mInodeLogicalTime = inodeLogicalTime;
  }

  std::string to_string() {
    std::stringstream out;
    out << mInodeId << "-" << std::to_string(mNamespace) << "-" << mName << "-" << mInodeLogicalTime;
    return out.str();
  }
};

struct FPXAttrVersionsK {
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;

  FPXAttrVersionsK(Int64 inodeId, Int8 ns, std::string name) {
    mInodeId = inodeId;
    mNamespace = ns;
    mName = name;
  }

  std::string to_string() {
    std::stringstream out;
    out << mInodeId << "-" << std::to_string(mNamespace) << "-" << mName;
    return out.str();
  }
};

struct FPXAttrBufferRow {
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;
  int mInodeLogicalTime;
  std::string mValue;

  std::string to_string(){
    std::stringstream stream;
    stream << "-------------------------" << std::endl;
    stream << "InodeId = " << mInodeId << std::endl;
    stream << "Namespace = " << (int)mNamespace << std::endl;
    stream << "Name = " << mName << std::endl;
    stream << "InodeLogicalTime = " << mInodeLogicalTime << std::endl;
    stream << "Value = " << mValue << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }

  FPXAttrBufferPK getPK() {
    return FPXAttrBufferPK(mInodeId, mNamespace, mName, mInodeLogicalTime);
  }
};

typedef std::vector <boost::optional<FPXAttrBufferPK> > FPXAttrBKeys;

class FileProvenanceXAttrBufferTable : public DBTable<FPXAttrBufferRow> {

public:
  public:

  FileProvenanceXAttrBufferTable() : DBTable("hdfs_file_provenance_xattrs_buffer") {
    addColumn("inode_id");
    addColumn("namespace");
    addColumn("name");
    addColumn("inode_logical_time");
    addColumn("value");
  }

  FPXAttrBufferRow getRow(NdbRecAttr* values[]) {
    FPXAttrBufferRow row;
    row.mInodeId = values[0]->int64_value();
    row.mNamespace = values[1]->int8_value();
    row.mName = get_string(values[2]);
    row.mInodeLogicalTime = values[3]->int32_value();
    row.mValue = get_string(values[4]);
    return row;
  }

  FPXAttrBufferRow get(Ndb* connection, Int64 inodeId, Int8 ns, std::string name, int inodeLogicalTime) {
    AnyMap a;
    a[0] = inodeId;
    a[1] = ns;
    a[2] = name;
    a[3] = inodeLogicalTime;
    return DBTable<FPXAttrBufferRow>::doRead(connection, a);
  }

  boost::optional<FPXAttrBufferRow> get(Ndb* connection, FPXAttrBufferPK key) {
    FPXAttrBufferRow row = get(connection, key.mInodeId, key.mNamespace, key.mName, key.mInodeLogicalTime);
    if (readCheckExists(key, row)) {
      return row;
    } else {
      return boost::none;
    }
  }

  std::vector<FPXAttrBufferRow> get(Ndb* connection, FPXAttrVersionsK key) {
    AnyMap a;
    a[0] = key.mInodeId;
    a[1] = key.mNamespace;
    a[2] = key.mName;
    return DBTable<FPXAttrBufferRow>::doRead(connection, "xattr_versions", a);
  }

  private:
  inline static bool readCheckExists(FPXAttrBufferPK key, FPXAttrBufferRow row) {
    return key.mInodeId == row.mInodeId && key.mNamespace == row.mNamespace 
    && key.mName == row.mName && key.mInodeLogicalTime == row.mInodeLogicalTime;
  }
};
#endif /* FILEPROVENANCEXATTRBUFFERTABLE_H */