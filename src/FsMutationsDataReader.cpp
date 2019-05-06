/*
 * Copyright (C) 2016 Hops.io
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
 * File:   FsMutationsDataReader.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "FsMutationsDataReader.h"
#include "HopsworksOpsLogTailer.h"

FsMutationsDataReader::FsMutationsDataReader(MConn connection, const bool hopsworks,
        const int lru_cap)
: NdbDataReader<FsMutationRow, MConn, FSKeys>(connection, hopsworks),
        mInodesTable(lru_cap), mDatasetTable(lru_cap) {
}

void FsMutationsDataReader::processAddedandDeleted(Fmq* data_batch, FSBulk& bulk) {

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
    XAttrMap& xattrs, FSBulk& bulk) {

  vector<ptime> arrivalTimes(pending->size());
  stringstream out;
  int i = 0;
  for (Fmq::iterator it = pending->begin(); it != pending->end(); ++it, i++) {
    FsMutationRow row = *it;
    arrivalTimes[i] = row.mEventCreationTime;
    bulk.mPKs.mFSPKs.push_back(row.getPK());

    if(row.isINodeOperation()) {
      if (!row.requiresReadingINode()) {
        //Handle the delete, rename, and change dataset
        out << INodeRow::to_json(row);
        out << endl;
        continue;
      }

      if (inodes.find(row.mInodeId) == inodes.end()) {
        LOG_DEBUG(
            " Data for inode: " << row.getParentId() << ", " << row
            .getINodeName() << ", " << row.mInodeId << " was not found");
        out << INodeRow::to_delete_json(row.mInodeId);
        out << endl;
        continue;
      }

      INodeRow inode = inodes[row.mInodeId];

      Int64 datasetINodeId = DONT_EXIST_INT();
      int projectId = DONT_EXIST_INT();
      if (mHopsworksEnalbed) {
        datasetINodeId = row.mDatasetINodeId;
        projectId = mDatasetTable.getProjectIdFromCache(row.mDatasetINodeId);
      }

      string inodeJSON = inode.to_create_json(datasetINodeId, projectId);

      out << inodeJSON << endl;
    } else if(row.isXAttrOperation()){
      if(!row.requiresReadingXAttr()){
        //handle delete xattr
        out << FsMutationsHelper::to_delete_json(row.mInodeId, row.getXAttrName());
        out << endl;
        continue;
      }

      string mutationpk = row.getPKStr();

      if(xattrs.find(mutationpk) == xattrs.end()){
        LOG_DEBUG(" Data for xattr: " << row.getXAttrName() << ", "
        << row.getNamespace() <<  " for inode " << row.mInodeId
        << " was not ""found");
        out << FsMutationsHelper::to_delete_json(row.mInodeId, row.getXAttrName());
        out << endl;
        continue;
      }

      XAttrVec xattr = xattrs[mutationpk];
      for(XAttrVec::iterator it = xattr.begin(); it != xattr.end() ; ++it){
        XAttrRow xAttrRow = *it;
        if(xAttrRow.mInodeId ==  row.mInodeId){
          out << FsMutationsHelper::to_upsert_json(xAttrRow);
          out << endl;
        }else{
          LOG_DEBUG(" Data for xattr: " << row.getXAttrName() << ", "
          << row.getNamespace() <<  " for inode " << row.mInodeId
          << " was not ""found");
          out << FsMutationsHelper::to_delete_json(row.mInodeId, row.getXAttrName());
          out << endl;
        }
      }
    }else{
      LOG_ERROR("Unknown fs operation " << row.to_string());
    }
  }

  bulk.mArrivalTimes = arrivalTimes;
  bulk.mJSON = out.str();
}

FsMutationsDataReader::~FsMutationsDataReader() {

}
