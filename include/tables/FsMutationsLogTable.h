/*
 * Copyright (C) 2018 Logical Clocks AB
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
 * File:   FsMutationsLogTable.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef FSMUTATIONSLOGTABLE_H
#define FSMUTATIONSLOGTABLE_H

#include "DBWatchTable.h"
#include "ConcurrentPriorityQueue.h"
#include "ConcurrentQueue.h"

enum FsOpType {
  FsAdd = 0,
  FsDelete = 1,
  FsUpdate = 2,
  FsRename = 3,
  FsChangeDataset = 4,

  XAttrAdd = 10,
  XAttrAddAll = 11,
  XAttrUpdate = 12,
  XAttrDelete = 13
};

inline static const char *FsOpTypeToStr(FsOpType optype) {
  switch (optype) {
    case FsAdd:
      return "Add";
    case FsUpdate:
      return "Update";
    case FsDelete:
      return "Delete";
    case FsRename:
      return "Rename";
    case FsChangeDataset:
      return "ChangeDataset";
    case XAttrAdd:
      return "XAttrAdd";
    case XAttrAddAll:
      return "XAttrAddAll";
    case XAttrUpdate:
      return "XAttrUpdate";
    case XAttrDelete:
      return "XAttrDelete";
    default:
      return "Unkown";
  }
}

struct FsMutationPK {
  Int64 mDatasetINodeId;
  Int64 mInodeId;
  int mLogicalTime;

  FsMutationPK(Int64 datasetId, Int64 inodeId, int logicalTime) {
    mDatasetINodeId = datasetId;
    mInodeId = inodeId;
    mLogicalTime = logicalTime;
  }
};

struct FsMutationRow {
  Int64 mDatasetINodeId;
  Int64 mInodeId;
  int mLogicalTime;

  Int64 mPk1;
  Int64 mPk2;
  std::string mPk3;
  FsOpType mOperation;

  ptime mEventCreationTime;

  FsMutationPK getPK() {
    return FsMutationPK(mDatasetINodeId, mInodeId, mLogicalTime);
  }

  std::string getPKStr(){
    std::stringstream stream;
    stream << mDatasetINodeId << "-" << mInodeId << "-" << mLogicalTime;
    return stream.str();
  }

  std::string to_string() {
    std::stringstream stream;
    stream << "-------------------------" << std::endl;
    stream << "DatasetId = " << mDatasetINodeId << std::endl;
    stream << "InodeId = " << mInodeId << std::endl;
    stream << "Pk1 = " << mPk1 << std::endl;
    stream << "Pk2 = " << mPk2 << std::endl;
    stream << "Pk3 = " << mPk3 << std::endl;
    stream << "LogicalTime = " << mLogicalTime << std::endl;
    stream << "Operation = " << FsOpTypeToStr(mOperation) << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }

  bool isINodeOperation(){
    return mOperation == FsAdd || mOperation == FsDelete || mOperation ==
    FsUpdate || mOperation == FsRename || mOperation == FsChangeDataset;
  }

  bool requiresReadingINode(){
    return mOperation == FsAdd || mOperation == FsUpdate || mOperation == FsRename;;
  }

  Int64 getPartitionId(){
    return mPk1;
  }

  Int64 getParentId(){
    return mPk2;
  }

  std::string getINodeName(){
    return mPk3;
  }

  bool isXAttrOperation(){
    return mOperation == XAttrAdd || mOperation == XAttrAddAll || mOperation
    == XAttrUpdate || mOperation == XAttrDelete;
  }

  bool requiresReadingXAttr(){
    return mOperation == XAttrAdd || mOperation == XAttrUpdate || mOperation
    == XAttrAddAll ;
  }

  Int8 getNamespace(){
    return static_cast<Int8>(mPk2);
  }

  std::string getXAttrName(){
    return mPk3;
  }
};

struct FsMutationRowEqual {

  bool operator()(const FsMutationRow &lhs, const FsMutationRow &rhs) const {
    return lhs.mDatasetINodeId == rhs.mDatasetINodeId && lhs.mInodeId == rhs
    .mInodeId && lhs.mLogicalTime == rhs.mLogicalTime;
  }
};

struct FsMutationRowHash {

  std::size_t operator()(const FsMutationRow &a) const {
    std::size_t seed = 0;
    boost::hash_combine(seed, a.mDatasetINodeId);
    boost::hash_combine(seed, a.mInodeId);
    boost::hash_combine(seed, a.mLogicalTime);
    boost::hash_combine(seed, a.mPk1);
    boost::hash_combine(seed, a.mPk2);
    boost::hash_combine(seed, a.mPk3);

    return seed;
  }
};

struct FsMutationRowComparator {

  bool operator()(const FsMutationRow &r1, const FsMutationRow &r2) const {
    if (r1.mInodeId == r2.mInodeId) {
      return r1.mLogicalTime > r2.mLogicalTime;
    } else {
      return r1.mInodeId > r2.mInodeId;
    }
  }
};

//typedef ConcurrentPriorityQueue<FsMutationRow, FsMutationRowComparator> CFSpq;
typedef std::vector<FsMutationRow> Fmq;
typedef ConcurrentQueue<FsMutationRow> CFSq;
typedef boost::heap::priority_queue<FsMutationRow, boost::heap::compare<FsMutationRowComparator> > FSpq;
typedef std::vector<FsMutationPK> FPK;

typedef std::vector<FsMutationRow> FSv;
typedef boost::unordered_map<Uint64, FSv* > FsMutationRowsByGCI;
typedef boost::tuple<std::vector<Uint64>*, FsMutationRowsByGCI* > FsMutationRowsGCITuple;

class FsMutationsLogTable : public DBWatchTable<FsMutationRow> {
public:

  FsMutationsLogTable() : DBWatchTable("hdfs_metadata_log") {
    addColumn("dataset_id");
    addColumn("inode_id");
    addColumn("logical_time");
    addColumn("pk1");
    addColumn("pk2");
    addColumn("pk3");
    addColumn("operation");
    addRecoveryIndex("logical_time");
    addWatchEvent(NdbDictionary::Event::TE_INSERT);
  }

  FsMutationRow getRow(NdbRecAttr* value[]) {
    FsMutationRow row;
    row.mEventCreationTime = Utils::getCurrentTime();
    row.mDatasetINodeId = value[0]->int64_value();
    row.mInodeId = value[1]->int64_value();
    row.mLogicalTime = value[2]->int32_value();
    row.mPk1 = value[3]->int64_value();
    row.mPk2 = value[4]->int64_value();
    row.mPk3 = get_string(value[5]);
    row.mOperation = static_cast<FsOpType> (value[6]->int8_value());
    return row;
  }

  void removeLogs(Ndb* connection, FPK& pks) {
    start(connection);
    for (FPK::iterator it = pks.begin(); it != pks.end(); ++it) {
      FsMutationPK pk = *it;
      AnyMap a;
      a[0] = pk.mDatasetINodeId;
      a[1] = pk.mInodeId;
      a[2] = pk.mLogicalTime;
      doDelete(a);
      LOG_DEBUG("Delete log row: Dataset[" << pk.mDatasetINodeId << "], INode["
              << pk.mInodeId << "], Timestamp[" << pk.mLogicalTime << "]");
    }
    end();
  }
};


#endif /* FSMUTATIONSLOGTABLE_H */

