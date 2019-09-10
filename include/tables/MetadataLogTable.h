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

#ifndef METADATALOGTABLE_H
#define METADATALOGTABLE_H
#include "DBWatchTable.h"
#include "ConcurrentPriorityQueue.h"
#include "HopsworksOpsLogTable.h"

#define XATTR_FIELD_NAME "xattr"

struct MetadataKey {
  Int32 mId;
  Int32 mFieldId;
  Int32 mTupleId;

  MetadataKey() {
  }

  MetadataKey(Int32 id, Int32 fieldId, Int32 tupleId) {
    mId = id;
    mFieldId = fieldId;
    mTupleId = tupleId;
  }

  bool operator==(const MetadataKey &other) const {
    return (mId == other.mId) && (mFieldId == other.mFieldId) && (mTupleId == other
    .mTupleId);
  }

  std::string getPKStr(){
    std::stringstream stream;
    stream << mId << "-" << mTupleId << "-" << mFieldId;
    return stream.str();
  }
};

struct MetadataKeyHasher {

  std::size_t operator()(const MetadataKey &key) const {
    std::size_t seed = 0;
    boost::hash_combine(seed, boost::hash_value(key.mId));
    boost::hash_combine(seed, boost::hash_value(key.mFieldId));
    boost::hash_combine(seed, boost::hash_value(key.mTupleId));
    return seed;
  }
};

struct MetadataLogEntry {
  Int32 mId;
  MetadataKey mMetaPK;
  HopsworksOpType mMetaOpType;
  ptime mEventCreationTime;

  std::string to_string() {
    std::stringstream stream;
    stream << "-------------------------" << std::endl;
    stream << "Id = " << mId << std::endl;
    stream << "MetaPK = " << mMetaPK.getPKStr() << std::endl;
    stream << "MetaOpType = " << HopsworksOpTypeToStr(mMetaOpType) << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }
};

struct MetadataLogEntryComparator {

  bool operator()(const MetadataLogEntry &r1, const MetadataLogEntry &r2) const {
    return r1.mId > r2.mId;
  }
};

typedef ConcurrentPriorityQueue<MetadataLogEntry, MetadataLogEntryComparator> CMetaQ;
typedef std::vector<MetadataLogEntry> MetaQ;

class MetadataLogTable : public DBWatchTable<MetadataLogEntry> {
public:
  struct MetaLogHandler : public LogHandler{
    int mPK;

    MetaLogHandler(int pk) : mPK(pk) {}
    void removeLog(Ndb* connection) const override {
      MetadataLogTable().removeLog(connection, mPK);
    }
    LogType getType() const override {
      return LogType::METALOG;
    }
    std::string getDescription() const override {
      std::stringstream out;
      out << "MetaLog (meta_log) Key (" << mPK << ")";
      return out.str();
    }
  };

  MetadataLogTable() : DBWatchTable("meta_log") {
    addColumn("id");
    addColumn("meta_id");
    addColumn("meta_field_id");
    addColumn("meta_tuple_id");
    addColumn("meta_op_type");
    addRecoveryIndex(PRIMARY_INDEX);
    addWatchEvent(NdbDictionary::Event::TE_INSERT);
  }

  MetadataLogEntry getRow(NdbRecAttr* value[]) {
    MetadataLogEntry row;
    row.mEventCreationTime = Utils::getCurrentTime();
    row.mId = value[0]->int32_value();
    Int32 metaId = value[1]->int32_value();
    Int32 metaFieldId = value[2]->int32_value();
    Int32 metaTupleId = value[3]->int32_value();
    row.mMetaPK = MetadataKey(metaId, metaFieldId, metaTupleId);
    row.mMetaOpType = static_cast<HopsworksOpType> (value[4]->int8_value());
    return row;
  }

  void removeLogs(Ndb* conn, std::vector<const LogHandler*>& logrh) {
    start(conn);
    for (auto log : logrh) {
      if(log == nullptr){
        continue;
      }
      if(log->getType() != LogType::METALOG){
        continue;
      }
      const MetaLogHandler* logh = static_cast<const
          MetaLogHandler*>(log);
      doDelete(logh->mPK);
      LOG_TRACE("Delete log row: " << logh->mPK);
    }
    end();
  }

  void removeLog(Ndb* conn,int pk) {
    start(conn);
    doDelete(pk);
    LOG_TRACE("Delete log row: " << pk);
    end();
  }

  std::string getPKStr(MetadataLogEntry row) override {
    return row.mMetaPK.getPKStr();
  }

  LogHandler* getLogRemovalHandler(MetadataLogEntry row) override {
    return new MetaLogHandler(row.mId);
  }
};


#endif /* METADATALOGTABLE_H */

