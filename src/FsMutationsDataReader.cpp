/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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

#include "FsMutationsDataReader.h"
#include "HopsworksOpsLogTailer.h"

FsMutationsDataReader::FsMutationsDataReader(MConn connection, const bool hopsworks, const int lru_cap, const std::string search_index, const std::string featurestore_index)
: NdbDataReader<FsMutationRow, MConn>(connection, hopsworks), mInodesTable(lru_cap), mDatasetTable(lru_cap), mProjectTable(lru_cap), mSearchIndex(search_index), mFeaturestoreIndex(featurestore_index) {
}

void FsMutationsDataReader::processAddedandDeleted(Fmq* data_batch, eBulk&
bulk) {

  INodeMap inodes = mInodesTable.get(mNdbConnection.inodeConnection, data_batch);
  XAttrMap xattrs = mXAttrTable.get(mNdbConnection.inodeConnection, data_batch);
  if (mHopsworksEnabled) {
    ULSet dataset_inode_ids;
    for (Fmq::iterator it = data_batch->begin(); it != data_batch->end(); ++it) {
      FsMutationRow row = *it;
      dataset_inode_ids.insert(row.mDatasetINodeId);
    }
    mDatasetTable.loadProjectIds(mNdbConnection.metadataConnection, dataset_inode_ids, mProjectTable);
  }
  createJSON(data_batch, inodes, xattrs, bulk);
}

void FsMutationsDataReader::createJSON(Fmq* pending, INodeMap& inodes,
    XAttrMap& xattrs, eBulk& bulk) {

  for (Fmq::iterator it = pending->begin(); it != pending->end(); ++it) {
    FsMutationRow row = *it;

    if (row.isINodeOperation()) {
      if (!row.requiresReadingINode()) {
        bulk.push(nullptr, row.mEventCreationTime, INodeRow::to_delete_json(mFeaturestoreIndex, row.mInodeId));
        //Handle the delete and change dataset
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row.mEventCreationTime,
                INodeRow::to_delete_change_dataset_json(mSearchIndex, row));
        continue;
      }

      if (inodes.find(row.mInodeId) == inodes.end()) {
        LOG_DEBUG(
            " Data for inode: " << row.getParentId() << ", " << row
            .getINodeName() << ", " << row.mInodeId << " was not found");
        bulk.push(nullptr, row.mEventCreationTime, INodeRow::to_delete_json(mFeaturestoreIndex, row.mInodeId));
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row.mEventCreationTime,
                  INodeRow::to_delete_json(mSearchIndex, row.mInodeId));
        continue;
      }

      INodeRow inode = inodes[row.mInodeId];

      Int64 datasetINodeId = DONT_EXIST_INT();
      int projectId = DONT_EXIST_INT();
      if (mHopsworksEnabled) {
        datasetINodeId = row.mDatasetINodeId;
        projectId = mDatasetTable.getProjectIdFromCache(row.mDatasetINodeId);

        std::string datasetName = mDatasetTable.getDatasetNameFromCache(datasetINodeId);
        std::string projectName = mProjectTable.getProjectNameFromCache(projectId);
        std::string docType = FileProvenanceConstants::isPartOfFeaturestore(inode.mParentId, datasetINodeId, projectName, datasetName);
        if(docType != DONT_EXIST_STR()) {
          boost::optional<std::pair<std::string, int>> nameParts = FileProvenanceConstants::splitNameVersion(inode.mName);
          if(nameParts) {
            LOG_DEBUG("featurestore type:" << docType << "name:" << nameParts.get().first << " version:" << std::to_string(nameParts.get().second));
            bulk.push(nullptr, row.mEventCreationTime,
                    featurestoreDoc(docType, inode.mId, nameParts.get().first, nameParts.get().second, projectId, projectName, datasetINodeId));
          }
        }
      }

      //FsAdd, FsUpdate, FsRename are handled the same way
      bulk.push(mFSLogTable.getLogRemovalHandler(row), row.mEventCreationTime,
                inode.to_create_json(mSearchIndex, datasetINodeId, projectId));
    } else if (row.isXAttrOperation()) {
      inodes.find(row.mInodeId);
      INodeRow inode = inodes[row.mInodeId];
      Int64 datasetINodeId = DONT_EXIST_INT();
      int projectId = DONT_EXIST_INT();
      std::string datasetName = DONT_EXIST_STR();
      std::string projectName = DONT_EXIST_STR();
      if (mHopsworksEnabled) {
        datasetINodeId = row.mDatasetINodeId;
        projectId = mDatasetTable.getProjectIdFromCache(row.mDatasetINodeId);
        datasetName = mDatasetTable.getDatasetNameFromCache(datasetINodeId);
        projectName = mProjectTable.getProjectNameFromCache(projectId);
      }

      if (!row.requiresReadingXAttr()) {
        //handle delete xattr
        if(FileProvenanceConstants::isPartOfFeaturestore(inode.mParentId, datasetINodeId, projectName, datasetName) != DONT_EXIST_STR()) {
          bulk.push(nullptr, row.mEventCreationTime, XAttrRow::to_delete_json(mFeaturestoreIndex, row));
        }
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row.mEventCreationTime, XAttrRow::to_delete_json(mSearchIndex, row));
        continue;
      }

      std::string mutationpk = row.getPKStr();

      if(xattrs.find(mutationpk) == xattrs.end()){
        LOG_DEBUG(" Data for xattr: " << row.getXAttrName() << ", "
        << row.getNamespace() <<  " for inode " << row.mInodeId
        << " was not found");
        if(FileProvenanceConstants::isPartOfFeaturestore(inode.mParentId, datasetINodeId, projectName, datasetName) != DONT_EXIST_STR()) {
          bulk.push(nullptr, row.mEventCreationTime, XAttrRow::to_delete_json(mFeaturestoreIndex, row));
        }
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row.mEventCreationTime, XAttrRow::to_delete_json(mSearchIndex, row));
        continue;
      }

      XAttrVec xattr = xattrs[mutationpk];
      if(xattr.empty()){
        LOG_DEBUG(" Data for all xattrs of inode " << row.mInodeId
                                      << " was not found");
        if(FileProvenanceConstants::isPartOfFeaturestore(inode.mParentId, datasetINodeId, projectName, datasetName) != DONT_EXIST_STR()) {
          bulk.push(nullptr, row.mEventCreationTime, XAttrRow::to_delete_json(mFeaturestoreIndex, row));
        }
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row.mEventCreationTime, XAttrRow::to_delete_json(mSearchIndex, row));
        continue;
      }

      for(XAttrVec::iterator it = xattr.begin(); it != xattr.end() ; ++it){
        //only add the clean removal handler to the last event
        const LogHandler *const logh = std::next(it) != xattr.end() ? nullptr : mFSLogTable.getLogRemovalHandler(row);
        XAttrRow xAttrRow = *it;
        if (xAttrRow.mInodeId ==  row.mInodeId) {
          if(FileProvenanceConstants::isPartOfFeaturestore(inode.mParentId, datasetINodeId, projectName, datasetName) != DONT_EXIST_STR()) {
            boost::optional<std::pair<std::string, int>> nameParts = FileProvenanceConstants::splitNameVersion(inode.mName);
            if(nameParts) {
//              LOG_INFO("featurestore name:" << nameParts.get().first << " version:" << nameParts.get().second << " xattr:" << row.getXAttrName());
              bulk.push(nullptr, row.mEventCreationTime, xAttrRow.to_upsert_json(mFeaturestoreIndex, row.mOperation));
            }
          }
          bulk.push(logh, row.mEventCreationTime, xAttrRow.to_upsert_json(mSearchIndex, row.mOperation));
        } else {
          LOG_DEBUG(" Data for xattr: " << row.getXAttrName() << ", "
          << row.getNamespace() <<  " for inode " << row.mInodeId
          << " was not ""found");
          if(FileProvenanceConstants::isPartOfFeaturestore(inode.mParentId, datasetINodeId, projectName, datasetName) != DONT_EXIST_STR()) {
            bulk.push(nullptr, row.mEventCreationTime, XAttrRow::to_delete_json(mFeaturestoreIndex, row));
          }
          bulk.push(logh, row.mEventCreationTime, XAttrRow::to_delete_json(mSearchIndex, row));
        }
      }
    }else{
      LOG_ERROR("Unknown fs operation " << row.to_string());
    }
  }
}

std::string FsMutationsDataReader::docHeader(std::string index, Int64 id) {
  std::stringstream out;
  rapidjson::StringBuffer sbOp;
  rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

  opWriter.StartObject();
  opWriter.String("update");
  opWriter.StartObject();

  opWriter.String("_id");
  opWriter.Int64(id);
  opWriter.String("_index");
  opWriter.String(index.c_str());

  opWriter.EndObject();
  opWriter.EndObject();

  return sbOp.GetString();
}

std::string FsMutationsDataReader::featurestoreDoc(std::string docType, Int64 inodeId, std::string name, int version,
        int projectId, std::string projectName, Int64 datasetIId) {
  std::stringstream out;
  out << docHeader(mFeaturestoreIndex, inodeId) << std::endl;

  rapidjson::StringBuffer sbDoc;
  rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);

  docWriter.StartObject();
  docWriter.String("doc");
  docWriter.StartObject();

  docWriter.String("doc_type");
  docWriter.String(docType.c_str());
  docWriter.String("name");
  docWriter.String(name.c_str());
  docWriter.String("version");
  docWriter.Int(version);

  docWriter.String("project_id");
  docWriter.Int(projectId);
  docWriter.String("project_name");
  docWriter.String(projectName.c_str());
  docWriter.String("dataset_iid");
  docWriter.Int(datasetIId);

  docWriter.EndObject();

  docWriter.String("doc_as_upsert");
  docWriter.Bool(true);

  docWriter.EndObject();

  out << sbDoc.GetString() << std::endl;
  return out.str();
}

FsMutationsDataReader::~FsMutationsDataReader() {

}
