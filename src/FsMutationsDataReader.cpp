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

FsMutationsDataReader::FsMutationsDataReader(MConn connection, const bool hopsworks,
        const int lru_cap)
: NdbDataReader<FsMutationRow, MConn>(connection, hopsworks),
        mInodesTable(lru_cap), mDatasetTable(lru_cap) {
}

void FsMutationsDataReader::processAddedandDeleted(Fmq* data_batch, eBulk&
bulk) {

  INodeMap inodes = mInodesTable.get(mNdbConnection.inodeConnection, data_batch);
  XAttrMap xattrs = mXAttrTable.get(mNdbConnection.inodeConnection, data_batch);
  if (mHopsworksEnalbed) {
    ULSet dataset_inode_ids;
    for (Fmq::iterator it = data_batch->begin(); it != data_batch->end(); ++it) {
      FsMutationRow row = *it;
      dataset_inode_ids.insert(row.mDatasetINodeId);
    }
    mDatasetTable.loadProjectIds(mNdbConnection.metadataConnection, dataset_inode_ids);
  }

  createJSON(data_batch, inodes, xattrs, bulk);
}

void FsMutationsDataReader::createJSON(Fmq* pending, INodeMap& inodes,
    XAttrMap& xattrs, eBulk& bulk) {

  for (Fmq::iterator it = pending->begin(); it != pending->end(); ++it) {
    FsMutationRow row = *it;

    if(row.isINodeOperation()) {
      if (!row.requiresReadingINode()) {
        //Handle the delete and change dataset
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row
        .mEventCreationTime, INodeRow::to_delete_change_dataset_json(row));
        continue;
      }

      if (inodes.find(row.mInodeId) == inodes.end()) {
        LOG_DEBUG(
            " Data for inode: " << row.getParentId() << ", " << row
            .getINodeName() << ", " << row.mInodeId << " was not found");
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row
            .mEventCreationTime, INodeRow::to_delete_json(row.mInodeId));
        continue;
      }

      INodeRow inode = inodes[row.mInodeId];

      Int64 datasetINodeId = DONT_EXIST_INT();
      int projectId = DONT_EXIST_INT();
      if (mHopsworksEnalbed) {
        datasetINodeId = row.mDatasetINodeId;
        projectId = mDatasetTable.getProjectIdFromCache(row.mDatasetINodeId);
      }

      //FsAdd, FsUpdate, FsRename are handled the same way
      bulk.push(mFSLogTable.getLogRemovalHandler(row), row
          .mEventCreationTime, inode.to_create_json(datasetINodeId, projectId));
    } else if(row.isXAttrOperation()){
      if(!row.requiresReadingXAttr()){
        //handle delete xattr
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row
            .mEventCreationTime, XAttrRow::to_delete_json(row));
        continue;
      }

      std::string mutationpk = row.getPKStr();

      if(xattrs.find(mutationpk) == xattrs.end()){
        LOG_DEBUG(" Data for xattr: " << row.getXAttrName() << ", "
        << row.getNamespace() <<  " for inode " << row.mInodeId
        << " was not ""found");
        bulk.push(mFSLogTable.getLogRemovalHandler(row), row
            .mEventCreationTime, XAttrRow::to_delete_json(row));
        continue;
      }

      XAttrVec xattr = xattrs[mutationpk];
      int i = 0;
      for(XAttrVec::iterator it = xattr.begin(); it != xattr.end() ; ++it, i++){
        //only add the first event with a removal handler
        const LogHandler* const logh = i > 0 ? nullptr : mFSLogTable
            .getLogRemovalHandler(row);
        XAttrRow xAttrRow = *it;
        if(xAttrRow.mInodeId ==  row.mInodeId){
          bulk.push(logh, row.mEventCreationTime, xAttrRow.to_upsert_json
          (row.mOperation));
        }else{
          LOG_DEBUG(" Data for xattr: " << row.getXAttrName() << ", "
          << row.getNamespace() <<  " for inode " << row.mInodeId
          << " was not ""found");
          bulk.push(logh, row.mEventCreationTime, XAttrRow::to_delete_json
          (row));
        }
      }
    }else{
      LOG_ERROR("Unknown fs operation " << row.to_string());
    }
  }
}

FsMutationsDataReader::~FsMutationsDataReader() {

}
