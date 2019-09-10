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

#ifndef HOPSWORKSOPSLOGTABLE_H
#define HOPSWORKSOPSLOGTABLE_H

#include "DBWatchTable.h"

enum HopsworksOpType {
  HopsworksAdd = 0,
  HopsworksDelete = 1,
  HopsworksUpdate = 2,
};

enum OpsLogOn {
  Dataset = 0,
  Project = 1,
  Schema = 2
};

inline static const char* OpsLogOnToStr(OpsLogOn op) {
  switch (op) {
    case Dataset:
      return "Dataset";
    case Project:
      return "Project";
    case Schema:
      return "Schema";
    default:
      return "Unkown";
  }
}

inline static const char* HopsworksOpTypeToStr(HopsworksOpType optype) {
  switch (optype) {
    case HopsworksAdd:
      return "Add";
    case HopsworksUpdate:
      return "Update";
    case HopsworksDelete:
      return "Delete";
    default:
      return "Unkown";
  }
}

struct HopsworksOpRow {
  int mId;
  int mOpId;
  OpsLogOn mOpOn;
  HopsworksOpType mOpType;
  int mProjectId;
  Int64 mDatasetINodeId;
  Int64 mInodeId;

  std::string to_string() {
    std::stringstream out;
    out << "HopsworksOpRow::" << std::endl
            << "ID = " << mId << std::endl
            << "OpID = " << mOpId << std::endl
            << "OpOn = " << OpsLogOnToStr(mOpOn) << std::endl
            << "OpType = " << HopsworksOpTypeToStr(mOpType) << std::endl
            << "ProjectID = " << mProjectId << std::endl
            << "DatasetID = " << mDatasetINodeId << std::endl
            << "INodeID = " << mInodeId;
    return out.str();
  }
};

class HopsworksOpsLogTable : public DBWatchTable<HopsworksOpRow> {
public:

  HopsworksOpsLogTable() : DBWatchTable("ops_log") {
    addColumn("id");
    addColumn("op_id");
    addColumn("op_on");
    addColumn("op_type");
    addColumn("project_id");
    addColumn("dataset_id");
    addColumn("inode_id");
    addRecoveryIndex(PRIMARY_INDEX);
    addWatchEvent(NdbDictionary::Event::TE_INSERT);
  }

  HopsworksOpRow getRow(NdbRecAttr* value[]) {
    HopsworksOpRow row;
    row.mId = value[0]->int32_value();
    //op_id is the dataset_id or project_id or schema_id depending on the operation type
    row.mOpId = value[1]->int32_value();
    row.mOpOn = static_cast<OpsLogOn> (value[2]->int8_value());
    row.mOpType = static_cast<HopsworksOpType> (value[3]->int8_value());
    row.mProjectId = value[4]->int32_value();
    row.mDatasetINodeId = value[5]->int64_value();
    row.mInodeId = value[6]->int64_value();
    return row;
  }

  void removeLog(Ndb* conn, int pk) {
    start(conn);
    doDelete(pk);
    LOG_DEBUG("Remove log " << pk);
    end();
  }

  std::string getPKStr(HopsworksOpRow row) override {
    return std::to_string(row.mId);
  }

};

#endif /* HOPSWORKSOPSLOGTABLE_H */

