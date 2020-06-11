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
  Int16 mNumParts;

  FPXAttrBufferPK(Int64 inodeId, Int8 ns, std::string name, int inodeLogicalTime, Int16 numParts) {
    mInodeId = inodeId;
    mNamespace = ns;
    mName = name;
    mInodeLogicalTime = inodeLogicalTime;
    mNumParts = numParts;
  }

  std::string to_string() {
    std::stringstream out;
    out << mInodeId << "-" << std::to_string(mNamespace) << "-" << mName << "-" << mInodeLogicalTime << "-" << mNumParts;
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

struct FPXAttrBufferRowPart {
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;
  int mInodeLogicalTime;
  std::string mValue;
  Int16 mIndex;
  Int16 mNumParts;

  std::string to_string(){
    std::stringstream stream;
    stream << "-------------------------" << std::endl;
    stream << "InodeId = " << mInodeId << std::endl;
    stream << "Namespace = " << (int)mNamespace << std::endl;
    stream << "Name = " << mName << std::endl;
    stream << "InodeLogicalTime = " << mInodeLogicalTime << std::endl;
    stream << "Index = " << mIndex << std::endl;
    stream << "Value = " << mValue << std::endl;
    stream << "NumParts = " << mNumParts << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }

  std::string getUniqueXAttrId(){
    std::stringstream out;
    out << mInodeId << "-" << std::to_string(mNamespace) << "-" << mName << "-" << mInodeLogicalTime;
    return out.str();
  }
};

typedef std::vector<FPXAttrBufferRowPart> FPXAttrBufferRowPartVec;

struct FPXAttrBufferRow {
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;
  int mInodeLogicalTime;
  std::string mValue;
  Int16 mNumParts;

  FPXAttrBufferRow(FPXAttrBufferRowPartVec& vec){
    if(!vec.empty()){
      mInodeId = vec[0].mInodeId;
      mNamespace = vec[0].mNamespace;
      mName = vec[0].mName;
      mInodeLogicalTime = vec[0].mInodeLogicalTime;
      mNumParts = vec[0].mNumParts;
      mValue = combineValues(vec);
    }
  }

  FPXAttrBufferRow(Int64 inodeId, Int8 ns, std::string name, int logicalTime, Int16 numParts, std::string value){
    mInodeId = inodeId;
    mNamespace = ns;
    mName = name;
    mInodeLogicalTime = logicalTime;
    mNumParts = numParts;
    mValue = value;
  }

  std::string to_string(){
    std::stringstream stream;
    stream << "-------------------------" << std::endl;
    stream << "InodeId = " << mInodeId << std::endl;
    stream << "Namespace = " << (int)mNamespace << std::endl;
    stream << "Name = " << mName << std::endl;
    stream << "InodeLogicalTime = " << mInodeLogicalTime << std::endl;
    stream << "NumParts = " << mNumParts << std::endl;
    stream << "Value = " << mValue << std::endl;
    stream << "ValueSize = " << mValue.length() << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }

  FPXAttrBufferPK getPK() {
    return FPXAttrBufferPK(mInodeId, mNamespace, mName, mInodeLogicalTime, mNumParts);
  }

private:
  std::string combineValues(FPXAttrBufferRowPartVec& partVec){
    std::string value;
    for(auto & part : partVec){
       value = value + part.mValue;
    }
    return value;
  }
};

typedef std::vector <boost::optional<FPXAttrBufferPK> > FPXAttrBKeys;

class FileProvenanceXAttrBufferTable : public DBTable<FPXAttrBufferRowPart> {

public:
  public:

  FileProvenanceXAttrBufferTable() : DBTable("hdfs_file_provenance_xattrs_buffer") {
    addColumn("inode_id");
    addColumn("namespace");
    addColumn("name");
    addColumn("inode_logical_time");
    addColumn("index");
    addColumn("value");
    addColumn("num_parts");
  }

  FPXAttrBufferRowPart getRow(NdbRecAttr* values[]) {
    FPXAttrBufferRowPart row;
    row.mInodeId = values[0]->int64_value();
    row.mNamespace = values[1]->int8_value();
    row.mName = get_string(values[2]);
    row.mInodeLogicalTime = values[3]->int32_value();
    row.mIndex = values[4]->short_value();
    row.mValue = get_string(values[5]);
    row.mNumParts = values[6]->short_value();
    return row;
  }

  boost::optional<FPXAttrBufferRow> get(Ndb* connection, FPXAttrBufferPK key) {
    FPXAttrBufferRow row = get(connection, key.mInodeId, key.mNamespace, key.mName, key.mInodeLogicalTime, key.mNumParts);
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
    FPXAttrBufferRowPartVec partsVec = doRead(connection, "xattr_versions", a, key.mInodeId);
    return combine(partsVec);
  }

  private:
  inline static bool readCheckExists(FPXAttrBufferPK key, FPXAttrBufferRow row) {
    return key.mInodeId == row.mInodeId && key.mNamespace == row.mNamespace 
    && key.mName == row.mName && key.mInodeLogicalTime == row.mInodeLogicalTime;
  }

  FPXAttrBufferRow get(Ndb* connection, Int64 inodeId, Int8 ns, std::string name, int inodeLogicalTime, int numParts) {
    LOG_DEBUG("FileProvXAttr get by parts " << numParts);
    AnyVec anyVec;
    for(Int16 index=0; index < numParts; index++){
       AnyMap a;
       a[0] = inodeId;
       a[1] = ns;
       a[2] = name;
       a[3] = inodeLogicalTime;
       a[4] = index;
       anyVec.push_back(a);
    }
    FPXAttrBufferRowPartVec parts = doRead(connection,anyVec);
    LOG_DEBUG("FileProvXAttr batch read parts " << parts.size());
    return FPXAttrBufferRow(parts);
  }

  typedef boost::unordered_map<std::string, FPXAttrBufferRowPartVec> FPXAttrBufferRowPartMap;

  std::vector<FPXAttrBufferRow> combine(FPXAttrBufferRowPartVec& partsVec){ 
    LOG_DEBUG("FileProvXAttr combine parts " << partsVec.size());   
    FPXAttrBufferRowPartMap xattrsMap;
    for(auto& part: partsVec){
      std::string id = part.getUniqueXAttrId();
      if(xattrsMap.find(id) == xattrsMap.end()){
        xattrsMap[id] = FPXAttrBufferRowPartVec();
      }
       xattrsMap[id].push_back(part);
    }

    std::vector<FPXAttrBufferRow> results;
    for(auto& e: xattrsMap){
      auto& vec = e.second;
      std::sort(vec.begin(), vec.end(), [](FPXAttrBufferRowPart a, FPXAttrBufferRowPart b){
        return a.mIndex < b.mIndex;
      });
      results.push_back(FPXAttrBufferRow(vec));
    }
    return results;
  }

};
#endif /* FILEPROVENANCEXATTRBUFFERTABLE_H */