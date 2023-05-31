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

FsMutationsDataReader::FsMutationsDataReader(MConn connection, const bool hopsworks, const int lru_cap, const std::string search_index)
: NdbDataReader<FsMutationRow, MConn>(connection, hopsworks), mInodesTable(lru_cap), mDatasetTable(lru_cap), mProjectTable(lru_cap), mSearchIndex(search_index) {
  DatasetProjectSCache::getInstance(lru_cap, "DatasetProjectCache");
}

void FsMutationsDataReader::processAddedandDeleted(Fmq* data_batch, eBulk&
bulk) {
  INodeMap inodes = mInodesTable.get(mNdbConnection.hopsConnection, data_batch);
  XAttrMap xattrs = mXAttrTable.get(mNdbConnection.hopsConnection, data_batch);
  INodeRow projectsInode = mInodesTable.getProjectsInode(mNdbConnection.hopsConnection);
  if (mHopsworksEnabled) {
    for (Fmq::iterator it = data_batch->begin(); it != data_batch->end(); ++it) {
      FsMutationRow row = *it;
      DatasetProjectSCache::getInstance().loadDatasetFromInode(row.mDatasetINodeId,
                                                               mNdbConnection.hopsConnection, mInodesTable,
                                                               mNdbConnection.hopsworksConnection, mProjectTable, mDatasetTable,
                                                               projectsInode);
    }
  }
  createJSON(data_batch, inodes, xattrs, bulk);
}

void FsMutationsDataReader::createJSON(Fmq* pending, INodeMap& inodes,
    XAttrMap& xattrs, eBulk& bulk) {
  for (Fmq::iterator it = pending->begin(); it != pending->end(); ++it) {
    FsMutationRow row = *it;
    if (row.isINodeOperation()) {
      if (!row.requiresReadingINode()) {
        //Handle the delete and change dataset
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row.mEventCreationTime,
                INodeRow::to_delete_change_dataset_json(mSearchIndex, row));
        continue;
      }

      if (inodes.find(row.mInodeId) == inodes.end()) {
        LOG_DEBUG(
            " Data for inode: " << row.getParentId() << ", " << row
            .getINodeName() << ", " << row.mInodeId << " was not found");
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row.mEventCreationTime,
                  INodeRow::to_delete_json(mSearchIndex, row.mInodeId));
        continue;
      }

      INodeRow inode = inodes[row.mInodeId];

      Int64 datasetINodeId = DONT_EXIST_INT();
      int projectId = DONT_EXIST_INT();
      if (mHopsworksEnabled) {
        datasetINodeId = row.mDatasetINodeId;
        projectId = DatasetProjectSCache::getInstance().getProjectId(row.mDatasetINodeId);
      }
      //FsAdd, FsUpdate, FsRename are handled the same way
      bulk.push(mFSLogTable.getLogRemovalHandler(row), row.mEventCreationTime,
                inode.to_create_json(mSearchIndex, datasetINodeId, projectId));
      LOG_DEBUG("handling mutation row json " << row.getPKStr() << " - end");
    } else if (row.isXAttrOperation()) {
      Int64 datasetINodeId = DONT_EXIST_INT();
      int projectId = DONT_EXIST_INT();
      std::string datasetName = DONT_EXIST_STR();
      std::string projectName = DONT_EXIST_STR();
      if (mHopsworksEnabled) {
        datasetINodeId = row.mDatasetINodeId;
        projectId = DatasetProjectSCache::getInstance().getProjectId(row.mDatasetINodeId);
        datasetName = DatasetProjectSCache::getInstance().getDatasetName(row.mDatasetINodeId);
        projectName = DatasetProjectSCache::getInstance().getProjectName(row.mDatasetINodeId);
      }

      if (!row.requiresReadingXAttr()) {
        //handle delete xattr
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row.mEventCreationTime, XAttrRow::to_delete_json(mSearchIndex, row));
        continue;
      }

      std::string mutationpk = row.getPKStr();

      if(xattrs.find(mutationpk) == xattrs.end()){
        LOG_DEBUG(" Data for xattr: " << row.getXAttrName() << ", "
        << row.getNamespace() <<  " for inode " << row.mInodeId << " was not found");
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row.mEventCreationTime, XAttrRow::to_delete_json(mSearchIndex, row));
        continue;
      }

      XAttrVec xattr = xattrs[mutationpk];
      if(xattr.empty()){
        LOG_DEBUG(" Data for all xattrs of inode " << row.mInodeId << " was not found");
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row.mEventCreationTime, XAttrRow::to_delete_json(mSearchIndex, row));
        continue;
      }

      for(XAttrVec::iterator it = xattr.begin(); it != xattr.end() ; ++it){
        //only add the clean removal handler to the last event
        const LogHandler *const logh = std::next(it) != xattr.end() ? nullptr : mFSLogTable.getLogRemovalHandler(row);
        XAttrRow xAttrRow = *it;
        if (xAttrRow.mInodeId ==  row.mInodeId) {
          bulk.push(logh, row.mEventCreationTime, xAttrRow.to_upsert_json(mSearchIndex, row.mOperation));
        } else {
          LOG_DEBUG(" Data for xattr: " << row.getXAttrName() << ", "
          << row.getNamespace() <<  " for inode " << row.mInodeId << " was not ""found");
          bulk.push(logh, row.mEventCreationTime, XAttrRow::to_delete_json(mSearchIndex, row));
        }
      }
    }else{
      LOG_ERROR("Unknown fs operation " << row.to_string());
    }
  }
}

FsMutationsDataReader::~FsMutationsDataReader() {

}
