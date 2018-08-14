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
 * File:   MetadataLogTable.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef METADATALOGTABLE_H
#define METADATALOGTABLE_H
#include "DBWatchTable.h"
#include "ConcurrentPriorityQueue.h"
#include "HopsworksOpsLogTable.h"

#define XATTR_FIELD_NAME "xattr"

enum MetadataType {
  Schemabased = 0,
  Schemaless = 1
};

inline static const char* MetadataTypeToStr(MetadataType metaType) {
  switch (metaType) {
    case Schemabased:
      return "SchemaBased";
    case Schemaless:
      return "SchemaLess";
    default:
      return "Unkown";
  }
}

struct MetadataKey {
  int mPK1;
  int mPK2;
  int mPK3;

  MetadataKey() {
  }

  MetadataKey(int pk1, int pk2, int pk3) {
    mPK1 = pk1;
    mPK2 = pk2;
    mPK3 = pk3;
  }

  bool operator==(const MetadataKey &other) const {
    return (mPK1 == other.mPK1) && (mPK2 == other.mPK2) && (mPK3 == other.mPK3);
  }

  string to_string() {
    stringstream stream;
    stream << "[" << mPK1 << "," << mPK2 << "," << mPK3 << "]";
    return stream.str();
  }
};

struct MetadataKeyHasher {

  std::size_t operator()(const MetadataKey &key) const {
    std::size_t seed = 0;
    boost::hash_combine(seed, boost::hash_value(key.mPK1));
    boost::hash_combine(seed, boost::hash_value(key.mPK2));
    boost::hash_combine(seed, boost::hash_value(key.mPK3));
    return seed;
  }
};

struct MetadataLogEntry {
  int mId;
  MetadataKey mMetaPK;
  HopsworksOpType mMetaOpType;
  MetadataType mMetaType;

  ptime mEventCreationTime;

  string to_string() {
    stringstream stream;
    stream << "-------------------------" << endl;
    stream << "Id = " << mId << endl;
    stream << "MetaType = " << MetadataTypeToStr(mMetaType) << endl;
    stream << "MetaPK = " << mMetaPK.to_string() << endl;
    stream << "MetaOpType = " << HopsworksOpTypeToStr(mMetaOpType) << endl;
    stream << "-------------------------" << endl;
    return stream.str();
  }
};

struct MetadataLogEntryComparator {

  bool operator()(const MetadataLogEntry &r1, const MetadataLogEntry &r2) const {
    return r1.mId > r2.mId;
  }
};

typedef ConcurrentPriorityQueue<MetadataLogEntry, MetadataLogEntryComparator> CMetaQ;
typedef vector<MetadataLogEntry> MetaQ;

class MetadataLogTable : public DBWatchTable<MetadataLogEntry> {
public:

  MetadataLogTable() : DBWatchTable("meta_log") {
    addColumn("id");
    addColumn("meta_pk1");
    addColumn("meta_pk2");
    addColumn("meta_pk3");
    addColumn("meta_type");
    addColumn("meta_op_type");
    addRecoveryIndex("PRIMARY");
    addWatchEvent(NdbDictionary::Event::TE_INSERT);
  }

  MetadataLogEntry getRow(NdbRecAttr* value[]) {
    MetadataLogEntry row;
    row.mEventCreationTime = Utils::getCurrentTime();
    row.mId = value[0]->int32_value();
    int PK1 = value[1]->int32_value();
    int PK2 = value[2]->int32_value();
    int PK3 = value[3]->int32_value();
    row.mMetaPK = MetadataKey(PK1, PK2, PK3);
    row.mMetaType = static_cast<MetadataType> (value[4]->int8_value());
    row.mMetaOpType = static_cast<HopsworksOpType> (value[5]->int8_value());
    return row;
  }

  void removeLogs(Ndb* conn, UISet& pks) {
    start(conn);
    for (UISet::iterator it = pks.begin(); it != pks.end(); ++it) {
      int id = *it;
      doDelete(id);
      LOG_TRACE("Delete log row: " << id);
    }
    end();
  }
};


#endif /* METADATALOGTABLE_H */

