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
 * File:   ProvenanceLogTable.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef PROVENANCELOGTABLE_H
#define PROVENANCELOGTABLE_H
#include "DBWatchTable.h"

#include "ConcurrentPriorityQueue.h"
#include "ConcurrentQueue.h"

struct ProvenancePK {
  int mInodeId;
  int mUserId;
  string mAppId;
  int mLogicalTime;

  ProvenancePK(int inodeId, int userId, string appId, int logicalTime) {
    mInodeId = inodeId;
    mUserId = userId;
    mAppId = appId;
    mLogicalTime = logicalTime;
  }

  string to_string() {
    stringstream out;
    out << mInodeId << "-" << mUserId << "-" << mLogicalTime << "-" << mAppId;
    return out.str();
  }
};

struct ProvenanceRow {
  int mInodeId;
  int mUserId;
  string mAppId;
  int mLogicalTime;
  int mPartitionId;
  int mParentId;
  string mProjectName;
  string mDatasetName;
  string mInodeName;
  string mUserName;
  int mLogicalTimeBatch;
  long mTimestamp;
  long mTimestampBatch;
  short mOperation;

  ptime mEventCreationTime;

  ProvenancePK getPK() {
    return ProvenancePK(mInodeId, mUserId, mAppId, mLogicalTime);
  }

  string to_string() {
    stringstream stream;
    stream << "-------------------------" << endl;
    stream << "InodeId = " << mInodeId << endl;
    stream << "UserId = " << mUserId << endl;
    stream << "AppId = " << mAppId << endl;
    stream << "LogicalTime = " << mLogicalTime << endl;
    stream << "PartitionId = " << mPartitionId << endl;
    stream << "ParentId = " << mParentId << endl;
    stream << "ProjectName = " << mProjectName << endl;
    stream << "DatasetName = " << mDatasetName << endl;
    stream << "InodeName = " << mInodeName << endl;
    stream << "UserName = " << mUserName << endl;
    stream << "LogicalTimeBatch = " << mLogicalTimeBatch << endl;
    stream << "Timestamp = " << mTimestamp << endl;
    stream << "TimestampBatch = " << mTimestampBatch << endl;
    stream << "Operation = " << mOperation << endl;
    stream << "-------------------------" << endl;
    return stream.str();
  }

  string to_create_json() {
    stringstream out;
    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("update");
    opWriter.StartObject();

    opWriter.String("_id");
    opWriter.String(getPK().to_string().c_str());

    opWriter.EndObject();

    opWriter.EndObject();

    out << sbOp.GetString() << endl;

    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);

    docWriter.StartObject();
    docWriter.String("doc");
    docWriter.StartObject();

    docWriter.String("inode_id");
    docWriter.Int(mInodeId);

    docWriter.String("user_id");
    docWriter.Int(mUserId);

    docWriter.String("app_id");
    docWriter.String(mAppId.c_str());

    docWriter.String("logical_time");
    docWriter.Int(mLogicalTime);

    docWriter.String("partition_id");
    docWriter.Int(mPartitionId);

    docWriter.String("parent_id");
    docWriter.Int(mParentId);

    docWriter.String("project_name");
    docWriter.String(mProjectName.c_str());

    docWriter.String("dataset_name");
    docWriter.String(mDatasetName.c_str());

    docWriter.String("inode_name");
    docWriter.String(mInodeName.c_str());

    docWriter.String("user_name");
    docWriter.String(mUserName.c_str());

    docWriter.String("logical_time_batch");
    docWriter.Int(mLogicalTimeBatch);

    docWriter.String("timestamp");
    docWriter.Int64(mTimestamp);

    docWriter.String("timestamp_batch");
    docWriter.Int64(mTimestampBatch);

    docWriter.String("operation");
    docWriter.Int(mOperation);

    docWriter.EndObject();

    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);

    docWriter.EndObject();

    out << sbDoc.GetString() << endl;
    return out.str();
  }
};

struct ProvenanceRowEqual {

  bool operator()(const ProvenanceRow &lhs, const ProvenanceRow &rhs) const {
    return lhs.mInodeId == rhs.mInodeId && lhs.mUserId == rhs.mUserId
            && lhs.mAppId == rhs.mAppId && lhs.mLogicalTime == rhs.mLogicalTime;
  }
};

struct ProvenanceRowHash {

  std::size_t operator()(const ProvenanceRow &a) const {
    std::size_t seed = 0;
    boost::hash_combine(seed, a.mInodeId);
    boost::hash_combine(seed, a.mUserId);
    boost::hash_combine(seed, a.mAppId);
    boost::hash_combine(seed, a.mLogicalTime);
    return seed;
  }
};

struct ProvenanceRowComparator {

  bool operator()(const ProvenanceRow &r1, const ProvenanceRow &r2) const {
    if (r1.mInodeId == r2.mInodeId) {
      return r1.mLogicalTime > r2.mLogicalTime;
    } else {
      return r1.mInodeId > r2.mInodeId;
    }
  }
};

typedef ConcurrentQueue<ProvenanceRow> CPRq;
typedef boost::heap::priority_queue<ProvenanceRow, boost::heap::compare<ProvenanceRowComparator> > PRpq;
typedef vector<ProvenancePK> PKeys;
typedef vector<ProvenanceRow> Pq;

typedef vector<ProvenanceRow> Pv;
typedef boost::unordered_map<Uint64, Pv* > ProvenanceRowsByGCI;
typedef boost::tuple<vector<Uint64>*, ProvenanceRowsByGCI* > ProvenanceRowsGCITuple;

class ProvenanceLogTable : public DBWatchTable<ProvenanceRow> {
public:

  ProvenanceLogTable() : DBWatchTable("hdfs_provenance_log") {
    addColumn("inode_id");
    addColumn("user_id");
    addColumn("app_id");
    addColumn("logical_time");
    addColumn("partition_id");
    addColumn("parent_id");
    addColumn("project_name");
    addColumn("dataset_name");
    addColumn("inode_name");
    addColumn("user_name");
    addColumn("logical_time_batch");
    addColumn("timestamp");
    addColumn("timestamp_batch");
    addColumn("operation");
    addRecoveryIndex("logical_time");
    addWatchEvent(NdbDictionary::Event::TE_INSERT);
  }

  ProvenanceRow getRow(NdbRecAttr* value[]) {
    ProvenanceRow row;
    row.mEventCreationTime = Utils::getCurrentTime();
    row.mInodeId = value[0]->int32_value();
    row.mUserId = value[1]->int32_value();
    row.mAppId = get_string(value[2]);
    row.mLogicalTime = value[3]->int32_value();
    row.mPartitionId = value[4]->int32_value();
    row.mParentId = value[5]->int32_value();
    row.mProjectName = get_string(value[6]);
    row.mDatasetName = get_string(value[7]);
    row.mInodeName = get_string(value[8]);
    row.mUserName = get_string(value[9]);
    row.mLogicalTimeBatch = value[10]->int32_value();
    row.mTimestamp = value[11]->int64_value();
    row.mTimestampBatch = value[12]->int64_value();
    row.mOperation = value[13]->int8_value();
    return row;
  }

  void removeLogs(Ndb* connection, PKeys& pks) {
    start(connection);
    for (PKeys::iterator it = pks.begin(); it != pks.end(); ++it) {
      ProvenancePK pk = *it;
      AnyMap a;
      a[0] = pk.mInodeId;
      a[1] = pk.mUserId;
      a[2] = pk.mAppId;
      a[3] = pk.mLogicalTime;
      doDelete(a);
      LOG_DEBUG("Delete log row: App[" << pk.mAppId << "], INode["
              << pk.mInodeId << "], User[" << pk.mUserId << "], Timestamp["
              << pk.mLogicalTime << "]");
    }
    end();
  }

};


#endif /* PROVENANCELOGTABLE_H */

