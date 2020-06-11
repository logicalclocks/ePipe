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

#ifndef FILEPROVENANCELOGTABLE_H
#define FILEPROVENANCELOGTABLE_H
#include "DBWatchTable.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/document.h"

#include "ConcurrentPriorityQueue.h"
#include "ConcurrentQueue.h"
#include "XAttrTable.h"
#include "FileProvenanceXAttrBufferTable.h"

struct FileProvenancePK {
  Int64 mInodeId;
  std::string mOperation;
  int mLogicalTime;
  Int64 mTimestamp;
  std::string mAppId;
  int mUserId;
  std::string mTieBreaker;

  FileProvenancePK() {
  }

  FileProvenancePK(Int64 inodeId, std::string operation, int logicalTime, Int64 timestamp, std::string appId,
      int userId, std::string tieBreaker) {
    mInodeId = inodeId;
    mOperation = operation;
    mLogicalTime = logicalTime;
    mTimestamp = timestamp;
    mAppId = appId;
    mUserId = userId;
    mTieBreaker = tieBreaker;
  }

  std::string to_string() const {
    std::stringstream out;
    out << mInodeId << "-" << mOperation << "-" << mLogicalTime << "-" << mTimestamp << "-" << mAppId << "-" << mUserId
    <<"-"<<mTieBreaker;
    return out.str();
  }
};

struct FileProvenanceRow {
  Int64 mInodeId;
  std::string mOperation;
  int mLogicalTime;
  Int64 mTimestamp;
  std::string mAppId;
  int mUserId;
  std::string mTieBreaker;
  
  Int64 mPartitionId;
  Int64 mProjectId;
  Int64 mDatasetId;
  Int64 mParentId;
  std::string mInodeName;
  std::string mProjectName;
  std::string mDatasetName;
  std::string mP1Name;
  std::string mP2Name;
  std::string mParentName;
  std::string mUserName;
  std::string mXAttrName;
  int mLogicalTimeBatch;
  Int64 mTimestampBatch;
  int mDatasetLogicalTime;
  Int16 mXAttrNumParts;

  ptime mEventCreationTime;

  FileProvenancePK getPK() {
    return FileProvenancePK(mInodeId, mOperation, mLogicalTime, mTimestamp, mAppId, mUserId, mTieBreaker);
  }

  std::string to_string() {
    std::stringstream stream;
    stream << "-------------------------" << std::endl;
    stream << "InodeId = " << mInodeId << std::endl;
    stream << "Operation = " << mOperation << std::endl;
    stream << "LogicalTime = " << mLogicalTime << std::endl;
    stream << "Timestamp = " << mTimestamp << std::endl;
    stream << "AppId = " << mAppId << std::endl;
    stream << "UserId = " << mUserId << std::endl;
    stream << "TieBreaker = " << mTieBreaker << std::endl;
    
    stream << "PartitionId = " << mPartitionId << std::endl;
    stream << "ProjectId = " << mProjectId << std::endl;
    stream << "DatasetId = " << mDatasetId << std::endl;
    stream << "ParentId = " << mParentId << std::endl;
    stream << "InodeName = " << mInodeName << std::endl;
    stream << "ProjectName = " << mProjectName << std::endl;
    stream << "DatasetName = " << mDatasetName << std::endl;
    stream << "Parent1Name = " << mP1Name << std::endl;
    stream << "Parent2Name = " << mP2Name << std::endl;
    stream << "ParentName = " << mParentName << std::endl;
    stream << "UserName = " << mUserName << std::endl;
    stream << "XAttrName = " << mXAttrName << std::endl;
    stream << "LogicalTimeBatch = " << mLogicalTimeBatch << std::endl;
    stream << "TimestampBatch = " << mTimestampBatch << std::endl;
    stream << "DatasetLogicalTime = " << mDatasetLogicalTime << std::endl;
    stream << "XAttrNumParts = " << mXAttrNumParts << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }
};
  
struct FileProvenanceRowEqual {

  bool operator()(const FileProvenanceRow &lhs, const FileProvenanceRow &rhs) const {
    return lhs.mInodeId == rhs.mInodeId && lhs.mUserId == rhs.mUserId
            && lhs.mAppId == rhs.mAppId && lhs.mLogicalTime == rhs.mLogicalTime
            && lhs.mTieBreaker == rhs.mTieBreaker;
  }
};

struct FileProvenanceRowHash {

  std::size_t operator()(const FileProvenanceRow &a) const {
    std::size_t seed = 0;
    boost::hash_combine(seed, a.mInodeId);
    boost::hash_combine(seed, a.mOperation);
    boost::hash_combine(seed, a.mLogicalTime);
    boost::hash_combine(seed, a.mTimestamp);
    boost::hash_combine(seed, a.mAppId);
    boost::hash_combine(seed, a.mUserId);
    boost::hash_combine(seed, a.mTieBreaker);
    
    return seed;
  }
};

struct FileProvenanceRowComparator {

  bool operator()(const FileProvenanceRow &r1, const FileProvenanceRow &r2) const {
    if (r1.mInodeId == r2.mInodeId) {
      return r1.mLogicalTime > r2.mLogicalTime;
    } else {
      return r1.mInodeId > r2.mInodeId;
    }
  }
};

typedef ConcurrentQueue<FileProvenanceRow> CPRq;
typedef boost::heap::priority_queue<FileProvenanceRow, boost::heap::compare<FileProvenanceRowComparator> > PRpq;
typedef std::vector <boost::optional<FileProvenancePK> > PKeys;
typedef std::vector <FileProvenanceRow> Pq;

class FileProvenanceLogTable : public DBWatchTable<FileProvenanceRow> {
public:
  struct FileProvLogHandler : public LogHandler{
    FileProvenancePK mPK;
    boost::optional<FPXAttrBufferPK> mBufferPK;

    FileProvLogHandler(FileProvenancePK pk, boost::optional<FPXAttrBufferPK> bufferPK) :
    mPK(pk), mBufferPK(bufferPK) {}

    void removeLog(Ndb* connection) const override {
      LOG_ERROR("do not use - logic error");
      std::stringstream cause;
      cause << "do not use - logic error";
      throw std::logic_error(cause.str());
    }
    LogType getType() const override {
      return LogType::PROVFILELOG;
    }

    std::string getDescription() const override {
      std::stringstream out;
      out << "FileProvLog (hdfs_file_provenance_log) Key (" << mPK.to_string() << ")";
      return out.str();
    }
  };

  FileProvenanceLogTable() : DBWatchTable("hdfs_file_provenance_log", new FileProvenanceXAttrBufferTable()) {
    addColumn("inode_id");
    addColumn("inode_operation");
    addColumn("io_logical_time");
    addColumn("io_timestamp");
    addColumn("io_app_id");
    addColumn("io_user_id");
    addColumn("tb");
    addColumn("i_partition_id");
    addColumn("project_i_id");
    addColumn("dataset_i_id");
    addColumn("parent_i_id");
    addColumn("i_name");
    addColumn("project_name");
    addColumn("dataset_name");
    addColumn("i_p1_name");
    addColumn("i_p2_name");
    addColumn("i_parent_name");
    addColumn("io_user_name");
    addColumn("i_xattr_name");
    addColumn("io_logical_time_batch");
    addColumn("io_timestamp_batch");
    addColumn("ds_logical_time");
    addColumn("i_xattr_num_parts");
    addWatchEvent(NdbDictionary::Event::TE_INSERT);
  }

  FileProvenanceRow getRow(NdbRecAttr* value[]) {
    FileProvenanceRow row;
    row.mEventCreationTime = Utils::getCurrentTime();
    row.mInodeId = value[0]->int64_value();
    row.mOperation = get_string(value[1]);
    row.mLogicalTime = value[2]->int32_value();
    row.mTimestamp = value[3]->int64_value();
    row.mAppId = get_string(value[4]);
    row.mUserId = value[5]->int32_value();
    row.mTieBreaker = get_string(value[6]);
    row.mPartitionId = value[7]->int64_value();
    row.mProjectId = value[8]->int64_value();
    row.mDatasetId = value[9]->int64_value();
    row.mParentId = value[10]->int64_value();
    row.mInodeName = get_string(value[11]);
    row.mProjectName = get_string(value[12]);
    row.mDatasetName = get_string(value[13]);
    row.mP1Name = get_string(value[14]);
    row.mP2Name = get_string(value[15]);
    row.mParentName = get_string(value[16]);
    row.mUserName = get_string(value[17]);
    row.mXAttrName = get_string(value[18]);
    row.mLogicalTimeBatch = value[19]->int32_value();
    row.mTimestampBatch = value[20]->int64_value();
    row.mDatasetLogicalTime = value[21]->int32_value();
    row.mXAttrNumParts = value[22]->short_value();
    return row;
  }

  void cleanLogs(Ndb* connection, std::vector<const LogHandler*>& logrh) {
    try{
      cleanLogsOneTransaction(connection, logrh);
    } catch (NdbTupleDidNotExist &e){
      cleanLogsMultiTransaction(connection, logrh);
    }
  }

  void cleanLog(Ndb* connection, const LogHandler* log) {
    if (log != nullptr && log->getType() == LogType::PROVFILELOG) {
      const FileProvLogHandler *fplog = static_cast<const FileProvLogHandler *>(log);
      try{
        start(connection);
        _doDeleteOnCompanion(fplog);
        _doDelete(fplog);
        end();
      } catch (NdbTupleDidNotExist &e){
        doDeleteOnCompanionTransaction(connection, fplog);
        doDeleteTransaction(connection, fplog);
      }
    }
  }

  std::string getPKStr(FileProvenanceRow row) {
    return row.getPK().to_string();
  }

  LogHandler* getLogHandler(FileProvenancePK pk, boost::optional<FPXAttrBufferPK> bufferPK) {
    return new FileProvLogHandler(pk, bufferPK);
  }

private:
  void cleanLogsOneTransaction(Ndb* connection, std::vector<const LogHandler*>&logrh) {
    start(connection);
    for (auto log : logrh) {
      if (log != nullptr && log->getType() == LogType::PROVFILELOG) {
        const FileProvLogHandler *fplog = static_cast<const FileProvLogHandler *>(log);
        _doDeleteOnCompanion(fplog);
        _doDelete(fplog);
      }
    }
    end();
  }

  void cleanLogsMultiTransaction(Ndb* connection, std::vector<const LogHandler*>&logrh) {
    for (auto log : logrh) {
      if (log != nullptr && log->getType() == LogType::PROVFILELOG) {
        const FileProvLogHandler *fplog = static_cast<const FileProvLogHandler *>(log);
        doDeleteOnCompanionTransaction(connection, fplog);
        doDeleteTransaction(connection, fplog);
      }
    }
  }

  void doDeleteOnCompanionTransaction(Ndb* connection, const FileProvLogHandler *fplog){
    try{
      start(connection);
      _doDeleteOnCompanion(fplog);
      end();
    } catch (NdbTupleDidNotExist &e){
      LOG_DEBUG("Companion Log row was already deleted");
    }
  }

  void doDeleteTransaction(Ndb* connection, const FileProvLogHandler *fplog){
    try{
      start(connection);
      _doDelete(fplog);
      end();
    } catch (NdbTupleDidNotExist &e){
      LOG_DEBUG("Log row was already deleted");
    }
  }

  void _doDeleteOnCompanion(const FileProvLogHandler *fplog){
    if (fplog->mBufferPK){
      FPXAttrBufferPK cPK = fplog->mBufferPK.get();
      AnyVec companionPKs = getCompanionPKS(cPK);
      LOG_DEBUG("Delete xattr buffer row: " << cPK.to_string());
      for(auto& pk : companionPKs){
        doDeleteOnCompanion(pk);
      }
    }
  }

  void _doDelete(const FileProvLogHandler *fplog){
    FileProvenancePK fPK = fplog->mPK;
    AnyMap fileProvPK = getFileProvPK(fPK);
    LOG_DEBUG("Delete file provenance row: " << fPK.to_string());
    doDelete(fileProvPK);
  }

  AnyMap getFileProvPK(FileProvenancePK pk) {
    AnyMap a;
    a[0] = pk.mInodeId;
    a[1] = pk.mOperation;
    a[2] = pk.mLogicalTime;
    a[3] = pk.mTimestamp;
    a[4] = pk.mAppId;
    a[5] = pk.mUserId;
    a[6] = pk.mTieBreaker;
    return a;
  }

  AnyVec getCompanionPKS(FPXAttrBufferPK pk) {
    AnyVec vec;
    for(Int16 index=0; index < pk.mNumParts; index++){
      AnyMap a;
      a[0] = pk.mInodeId;
      a[1] = pk.mNamespace;
      a[2] = pk.mName;
      a[3] = pk.mInodeLogicalTime;
      a[4] = index;
      vec.push_back(a);
    }
    return vec;
  }
};
#endif /* FILEPROVENANCELOGTABLE_H */

