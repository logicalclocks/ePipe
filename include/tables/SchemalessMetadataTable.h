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
 * File:   SchemalessMetadataTable.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef SCHEMALESSMETADATATABLE_H
#define SCHEMALESSMETADATATABLE_H

#include "DBTable.h"
#include "MetadataLogTable.h"

#define EMPTY_DOC "{\"doc\" : {\"" XATTR_FIELD_NAME "\" : {} }, \"doc_as_upsert\" : true}"
#define REMOVE_DOC_SCRIPT "{\"script\" :\"ctx._source.remove(\\\"" XATTR_FIELD_NAME "\\\")\"}"

struct SchemalessMetadataEntry {
  int mId;
  Int64 mINodeId;
  Int64 mParentId;
  string mJSONData;
  HopsworksOpType mOperation;
  ptime mEventCreationTime;

  SchemalessMetadataEntry() {

  }

  SchemalessMetadataEntry(MetadataLogEntry ml) {
    mId = ml.mMetaPK.mPK1;
    mINodeId = ml.mMetaPK.mPK2;
    mParentId = ml.mMetaPK.mPK3;
    mOperation = ml.mMetaOpType;
    mEventCreationTime = ml.mEventCreationTime;
  }

  bool is_equal(MetadataKey metaKey) {
    return mId == metaKey.mPK1 && mINodeId == metaKey.mPK2
            && mParentId == metaKey.mPK3;
  }
  
  bool is_equal(SchemalessMetadataEntry ml) {
    return mId == ml.mId && mINodeId == ml.mINodeId
            && mParentId == ml.mParentId;
  }

  string to_create_json() {
    stringstream out;
    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("update");
    opWriter.StartObject();

    opWriter.String("_id");
    opWriter.Int64(mINodeId);

    opWriter.EndObject();
    opWriter.EndObject();

    out << sbOp.GetString() << endl;

    switch (mOperation) {
      case HopsworksAdd:
        out << REMOVE_DOC_SCRIPT << endl;
        out << sbOp.GetString() << endl;
        out << upsertMetadata(mJSONData) << endl;
        break;
      case HopsworksUpdate:
        out << upsertMetadata(mJSONData) << endl;
        break;
      case HopsworksDelete:
        out << REMOVE_DOC_SCRIPT << endl;
        break;
    }
    return out.str();
  }

  string upsertMetadata(string jsonData) {
    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
    rapidjson::Document doc;
    doc.Parse(EMPTY_DOC);
    rapidjson::Document xattr(&doc.GetAllocator());
    if (!xattr.Parse(jsonData.c_str()).HasParseError()) {
      mergeDoc(doc, xattr);
    } else {
      LOG_ERROR("JSON Parsing error: " << jsonData);
    }
    doc.Accept(docWriter);
    return string(sbDoc.GetString());
  }

  void mergeDoc(rapidjson::Document& target, rapidjson::Document& source) {
    for (rapidjson::Document::MemberIterator itr = source.MemberBegin(); itr != source.MemberEnd(); ++itr) {
      target["doc"][XATTR_FIELD_NAME].AddMember(itr->name, itr->value, target.GetAllocator());
    }
  }

  string to_string() {
    stringstream stream;
    stream << "-------------------------" << endl;
    stream << "Id = " << mId << endl;
    stream << "INodeId = " << mParentId << endl;
    stream << "ParentId = " << mParentId << endl;
    stream << "Operation = " << HopsworksOpTypeToStr(mOperation) << endl;
    stream << "Data = " << mJSONData << endl;
    stream << "-------------------------" << endl;
    return stream.str();
  }
};

typedef vector<SchemalessMetadataEntry> SchemalessMq;

class SchemalessMetadataTable : public DBTable<SchemalessMetadataEntry> {
public:

  SchemalessMetadataTable() : DBTable("meta_data_schemaless") {
    addColumn("id");
    addColumn("inode_id");
    addColumn("inode_parent_id");
    addColumn("data");
  }

  SchemalessMetadataEntry getRow(NdbRecAttr* values[]) {
    SchemalessMetadataEntry row;
    row.mId = values[0]->int32_value();
    row.mINodeId = values[1]->int64_value();
    row.mParentId = values[2]->int64_value();
    row.mJSONData = get_string(values[3]);
    return row;
  }

  SchemalessMq* get(Ndb* connection, MetaQ* batch, UISet& primaryKeys) {
    
    SchemalessMq schemalessQ;
    AnyVec args;
    
    for (MetaQ::iterator it = batch->begin(); it != batch->end(); ++it) {
      MetadataLogEntry le = *it;
      primaryKeys.insert(le.mId);
      
      SchemalessMetadataEntry ml = SchemalessMetadataEntry(le);
      schemalessQ.push_back(ml);
      
      if (ml.mOperation == HopsworksDelete) {
        continue;
      }

      AnyMap a;
      a[0] = ml.mId;
      a[1] = ml.mINodeId;
      a[2] = ml.mParentId;
     
      args.push_back(a);
      LOG_TRACE("Read SchemalessMetadata row for [" << ml.mId << ","
              << ml.mINodeId << "," << ml.mParentId << "]");
    }

    SchemalessMq readFromDB = doRead(connection, args);
    
    SchemalessMq* res = new SchemalessMq();
    
    int i =0;
    for (SchemalessMq::iterator it = schemalessQ.begin(); it != schemalessQ.end(); ++it, i++) {
      SchemalessMetadataEntry row = *it;
      if(row.mOperation != HopsworksDelete){
        SchemalessMetadataEntry read = readFromDB[i];
        if(!read.is_equal(row)){
          LOG_WARN("Ignore " << row.to_string() << " since it seems to be deleted");
          continue;
        }
        row = read;
      }
      res->push_back(row);
    }

    return res;
  }

};


#endif /* SCHEMALESSMETADATATABLE_H */

