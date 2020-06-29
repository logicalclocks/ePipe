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

#include "Cache.h"
#include "Utils.h"
#include "XAttrTable.h"
#include "FileProvenanceConstantsRaw.h"

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

  FPXAttrBufferPK withNumParts(Int16 numParts) {
    return FPXAttrBufferPK(mInodeId, mNamespace, mName, mInodeLogicalTime, numParts);
  }

  AnyMap getKey0Map() {
    AnyMap a;
    a[0] = mInodeId;
    a[1] = mNamespace;
    a[2] = mName;
    a[3] = mInodeLogicalTime;
    Int16 key0 = 0;
    a[4] = key0;
    return a;
  }

  AnyVec getKeysVec() {
    if(mNumParts == 0) {
      LOG_ERROR("file prov - prov core - logic error - using a place holder key as an actual key");
      LOG_FATAL("file prov - prov core - cannot recover");
    }
    AnyVec keys;
    for(Int16 i=0; i < mNumParts; i++) {
      AnyMap key;
      key[0] = mInodeId;
      key[1] = mNamespace;
      key[2] = mName;
      key[3] = mInodeLogicalTime;
      key[4] = i;
      keys.push_back(key);
    }
    return keys;
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

  inline static bool readCheckExists(FPXAttrBufferPK key, FPXAttrBufferRowPart row) {
    return key.mInodeId == row.mInodeId && key.mNamespace == row.mNamespace
           && key.mName == row.mName && key.mInodeLogicalTime == row.mInodeLogicalTime;
  }
};

struct FPXAttrBufferRow {
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;
  int mInodeLogicalTime;
  std::string mValue;
  Int16 mNumParts;

  FPXAttrBufferRow(Int64 inodeId, Int8 ns, std::string name, int logicalTime, Int16 numParts, std::string value){
    mInodeId = inodeId;
    mNamespace = ns;
    mName = name;
    mInodeLogicalTime = logicalTime;
    mNumParts = numParts;
    mValue = value;
  }

  FPXAttrBufferRow(FPXAttrBufferRowPart part) :
  FPXAttrBufferRow(part.mInodeId, part.mNamespace, part.mName, part.mInodeLogicalTime, part.mNumParts, part.mValue){
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

  inline static boost::optional<FPXAttrBufferRow> combineParts(FPXAttrBufferPK key, std::vector<FPXAttrBufferRowPart> parts){
    LOG_INFO("key:" << key.to_string() <<" parts:" << parts.size());
    if((long unsigned int)key.mNumParts != parts.size()) {
      return boost::none;
    }
    std::string value;
    for(auto& part : parts){
      if(!FPXAttrBufferRowPart::readCheckExists(key, part)) {
        LOG_WARN("key:" << key.to_string() << " missing part");
        return boost::none;
      }
      value = value + part.mValue;
    }
    return FPXAttrBufferRow(parts[0].mInodeId, parts[0].mNamespace, parts[0].mName, parts[0].mInodeLogicalTime, parts[0].mNumParts, value);
  }
};

struct ProvCoreEntry {
  FPXAttrBufferPK key;
  FPXAttrBufferRow value;
  int upToLogicalTime;

  ProvCoreEntry(FPXAttrBufferPK mKey, FPXAttrBufferRow mValue, int mLogicalTime) :
          key(mKey), value(mValue), upToLogicalTime(mLogicalTime) {}
};

struct ProvCore {
  ProvCoreEntry* core1;
  ProvCoreEntry* core2;

  ProvCore(ProvCoreEntry* provCore) : core1(provCore) {}
};

class ProvCoreCache {
public:
  ProvCoreCache(int lru_cap, const char* prefix) : mProvCores(lru_cap, prefix) {}
  /* for each inode we keep to cached values core1 and core2 and they are ordered core1 < core2
  * we do this, in the hope we get a nicer transition we the core changes but we might still get some out of order operations (using old core1)
  * each core is used for an interval of logical times...
  * we do not know the upper bound until we get the next core,
  * so the upToLogicalTime increments as we find from db that we should use same core
  */
  void add(FPXAttrBufferRow value, int opLogicalTime) {
    FPXAttrBufferPK key = value.getPK();
    if(mProvCores.contains(key.mInodeId)) {
      ProvCore provCore = mProvCores.get(key.mInodeId).get();
      //case {new} - <> -> <new>
      if (provCore.core1 == nullptr) {
        //no core defined
        provCore.core1 = new ProvCoreEntry(key, value, opLogicalTime);
        return;
      }
      //update core usage for upTo
      if (provCore.core1->key.mInodeLogicalTime == key.mInodeLogicalTime
          && provCore.core1->upToLogicalTime < opLogicalTime) {
        provCore.core1->upToLogicalTime = opLogicalTime;
        return;
      }
      //case {new, 1, 2?} - <1> -> <new, 1> or <1,2> -> <new, 1>
      if (provCore.core1->key.mInodeLogicalTime > key.mInodeLogicalTime) {
        //evict 2 if necessary(not null)
        if (provCore.core2 != nullptr) {
          delete provCore.core2;
        }
        provCore.core2 = provCore.core1;
        provCore.core1 = new ProvCoreEntry(key, value, opLogicalTime);
        return;
      }
      //holds: core1->key.mInodeLogicalTime < key.mInodeLogicalTime
      //case {1,new} - <1> -> <1,new>
      if (provCore.core2 == nullptr) {
        provCore.core2 = new ProvCoreEntry(key, value, opLogicalTime);
        return;
      }
      //update core usage for upTo
      if (provCore.core2->key.mInodeLogicalTime == key.mInodeLogicalTime
          && provCore.core2->upToLogicalTime < opLogicalTime) {
        provCore.core2->upToLogicalTime = opLogicalTime;
        return;
      }
      //case {1,2,new} - <1,2> -> <2,new>
      if (provCore.core2->key.mInodeLogicalTime < key.mInodeLogicalTime) {
        delete provCore.core1;
        provCore.core1 = provCore.core2;
        provCore.core2 = new ProvCoreEntry(key, value, opLogicalTime);
      } //case {1,new,2} - <1,2> -> <new,2>
      else {
        delete provCore.core1;
        provCore.core1 = new ProvCoreEntry(key, value, opLogicalTime);
      }
    } else {
      ProvCore core1(new ProvCoreEntry(key, value, opLogicalTime));
      mProvCores.put(key.mInodeId, core1);
    }
  }

  boost::optional<FPXAttrBufferRow> get(Int64 inodeId, int logicalTime) {
    boost::optional<ProvCore> provCore = mProvCores.get(inodeId);
    if(provCore) {
      if(provCore.get().core1 != nullptr
         && provCore.get().core1->key.mInodeLogicalTime <= logicalTime && logicalTime <= provCore.get().core1->upToLogicalTime) {
        return provCore.get().core1->value;
      }
      if(provCore.get().core2 != nullptr
         && provCore.get().core2->key.mInodeLogicalTime <= logicalTime && logicalTime <= provCore.get().core1->upToLogicalTime) {
        return provCore.get().core2->value;
      }
    }
    return boost::none;
  }

  /*
   * get the closest prov core logical time we can guess - for scanning the xattr buffer table for the actual prov core
   */
  int getProvCoreLogicalTime(Int64 inodeId, int opLogicalTime) {
    boost::optional<ProvCore> provCore = mProvCores.get(inodeId);
    if(provCore) {
      if(provCore.get().core1 != nullptr) {
        if(provCore.get().core1->upToLogicalTime <= opLogicalTime) {
          return provCore.get().core1->key.mInodeLogicalTime;
        }
      } else if(provCore.get().core2 != nullptr) {
        if(opLogicalTime < provCore.get().core2->key.mInodeLogicalTime) {
          return provCore.get().core1->key.mInodeLogicalTime;
        } else {
          return provCore.get().core2->key.mInodeLogicalTime;
        }
      }
    }
    return 0;
  }
private:
  Cache<Int64, ProvCore> mProvCores;
};

typedef CacheSingleton<ProvCoreCache> FProvCoreCache;

class FileProvenanceXAttrBufferTable : public DBTable<FPXAttrBufferRowPart> {

public:
  public:

  FileProvenanceXAttrBufferTable(int lru_cap) : DBTable("hdfs_file_provenance_xattrs_buffer") {
    addColumn("inode_id");
    addColumn("namespace");
    addColumn("name");
    addColumn("inode_logical_time");
    addColumn("index");
    addColumn("value");
    addColumn("num_parts");
    FProvCoreCache::getInstance(lru_cap, "FileProvCore");
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
    LOG_INFO("key:" << key.to_string());
    AnyVec keys = key.getKeysVec();
    std::vector<FPXAttrBufferRowPart> rows = DBTable<FPXAttrBufferRowPart>::doRead(connection, keys);
    return FPXAttrBufferRow::combineParts(key, rows);
  }

  /**
   * Get a range of prov cores between fromLogicalTime and toKey.mInodeLogicalTime.
   * This range is sparse (doesn't contain an entry for each key), but we also don't know the exact keys that have a value
   * The values themselves are also made of multiple parts and we only know the size of the parts after reading part0 of it
   * @param connection
   * @param toKey
   * @param fromLogicalTime
   * @return
   */
  std::map<int, boost::optional<FPXAttrBufferRow>> getProvCore(Ndb* connection, Int64 inodeId, int fromLogicalTime, int toLogicalTime) {
    std::map<int, boost::optional<FPXAttrBufferRow>> result;

    //generate part0 keys for interval fromLogicalTime to toLogicalTime
    std::vector<FPXAttrBufferPK> keys;
    AnyVec keysVec;
    for(int logicalTime=fromLogicalTime; logicalTime <= toLogicalTime; logicalTime++){
      FPXAttrBufferPK key(inodeId, FileProvenanceConstantsRaw::XATTRS_USER_NAMESPACE, FileProvenanceConstantsRaw::XATTR_PROV_CORE, logicalTime, 0);
      keys.push_back(key);
      keysVec.push_back(key.getKey0Map());
    }
    std::vector<FPXAttrBufferRowPart> rows = DBTable<FPXAttrBufferRowPart>::doRead(connection, keysVec);

    //save prov cores with only 1 part directly to result and generate the keys for multi part values
    AnyVec multiPartKeyVec;
    //holds key with numParts
    std::vector<FPXAttrBufferPK> multiPartKeys;
    for(unsigned int i=0; i<keys.size(); i++){
      FPXAttrBufferPK key0 = keys[i];
      FPXAttrBufferRowPart row = rows[i];
      if(FPXAttrBufferRowPart::readCheckExists(key0, row)) {
        if(row.mNumParts == 1) {
          boost::optional<FPXAttrBufferRow> provCore = FPXAttrBufferRow(row);
          result.insert(std::make_pair(key0.mInodeLogicalTime, provCore));
        } else {
          FPXAttrBufferPK multiPartKey = key0.withNumParts(row.mNumParts);
          LOG_INFO("key:" << multiPartKey.to_string());
          AnyVec partKeys = multiPartKey.getKeysVec();
          multiPartKeyVec.insert(multiPartKeyVec.end(), partKeys.begin(), partKeys.end());
          multiPartKeys.push_back(multiPartKey);
        }
      }
    }
    if(multiPartKeys.empty()) {
      return result;
    }
    //read multi part values for all multi part prov cores
    std::vector<FPXAttrBufferRowPart> multiPartRows = DBTable<FPXAttrBufferRowPart>::doRead(connection, multiPartKeyVec);
    unsigned index = 0;
    for(auto& key : multiPartKeys) {
      std::vector<FPXAttrBufferRowPart> provCoreParts(multiPartRows.begin() + index, multiPartRows.begin() + index + key.mNumParts - 1);
      boost::optional<FPXAttrBufferRow> provCore = FPXAttrBufferRow::combineParts(key, provCoreParts);
      result.insert(std::make_pair(key.mInodeLogicalTime, provCore));
      index += key.mNumParts;
    }
    return result;
  }
};
#endif /* FILEPROVENANCEXATTRBUFFERTABLE_H */