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

#ifndef EPIPE_XATTRTABLE_H
#define EPIPE_XATTRTABLE_H
#include "DBTable.h"
#include "FsMutationsLogTable.h"
#include "MetadataLogTable.h"


struct XAttrRow {
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;
  std::string mValue;


  std::string to_upsert_json(FsOpType operation){
    std::stringstream out;
    if(operation == XAttrUpdate){
      out << getDocUpdatePrefix(mInodeId) << std::endl;
      out << removeXAttrScript(mName) << std::endl;
    }
    out << getDocUpdatePrefix(mInodeId) << std::endl;
    out << upsert() << std::endl;
    return out.str();
  }

  std::string to_upsert_json(){
    std::stringstream out;
    out << getDocUpdatePrefix(mInodeId) << std::endl;
    out << upsert() << std::endl;
    return out.str();
  }

  static std::string to_delete_json(FsMutationRow row){
    std::stringstream out;
    out << getDocUpdatePrefix(row.mInodeId) << std::endl;
    out << removeXAttrScript(row.getXAttrName()) << std::endl;
    return out.str();
  }

  std::string to_string(){
    std::stringstream stream;
    stream << "-------------------------" << std::endl;
    stream << "InodeId = " << mInodeId << std::endl;
    stream << "Namespace = " << (int)mNamespace << std::endl;
    stream << "Name = " << mName << std::endl;
    stream << "Value = " << mValue << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }
private:

  std::string upsert() {
    rapidjson::Document doc;
    doc.Parse(getXAttrDoc(true).c_str());
    rapidjson::Document xattr(&doc.GetAllocator());
    if (!xattr.Parse(mValue.c_str()).HasParseError()) {
      doc["doc"][XATTR_FIELD_NAME][mName.c_str()] = xattr.Move();
      rapidjson::StringBuffer sbDoc;
      rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
      doc.Accept(docWriter);
      return std::string(sbDoc.GetString());
    }

    LOG_DEBUG("XAttr is non json " << mName << "=" << mValue);
    return getXAttrDoc(false);
  }

  static std::string getDocUpdatePrefix(Int64 inodeId){
    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("update");
    opWriter.StartObject();

    opWriter.String("_id");
    opWriter.Int64(inodeId);

    opWriter.EndObject();
    opWriter.EndObject();

    return std::string(sbOp.GetString());
  }

  static std::string removeXAttrScript(std::string xattrname){

    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("script");

    std::stringstream rmout;
    rmout << "if(ctx._source.containsKey(\"" << XATTR_FIELD_NAME << "\")){ ";
    rmout << "if(ctx._source." << XATTR_FIELD_NAME << ".containsKey(\"" << xattrname << "\")){ ";
    rmout << "ctx._source." << XATTR_FIELD_NAME << ".remove(\"" << xattrname<<  "\");";
    rmout << "} else{ ctx.op=\"noop\";}";
    rmout << "} else{ ctx.op=\"noop\";}";
    opWriter.String(rmout.str().c_str());

    opWriter.String("upsert");
    opWriter.StartObject();
    opWriter.EndObject();

    opWriter.EndObject();
    return std::string(sbOp.GetString());

  }

  std::string getXAttrDoc(bool isJSONVal){

    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("doc");
    opWriter.StartObject();

    opWriter.String(XATTR_FIELD_NAME);
    opWriter.StartObject();

    opWriter.String(mName.c_str());
    if(isJSONVal) {
      opWriter.StartObject();
      opWriter.EndObject();
    }else{
      opWriter.String(mValue.c_str());
    }
    opWriter.EndObject();


    opWriter.EndObject();
    opWriter.String("doc_as_upsert");
    opWriter.Bool(true);

    opWriter.EndObject();

    return std::string(sbOp.GetString());
  }

};

struct XAttrPK {
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;

  XAttrPK(Int64 inodeId, Int8 ns, std::string name) {
    mInodeId = inodeId;
    mNamespace = ns;
    mName = name;
  }

  std::string to_string() {
    std::stringstream out;
    out << mInodeId << "-" << mNamespace << "-" << mName;
    return out.str();
  }
};

typedef std::vector<XAttrRow> XAttrVec;
typedef boost::unordered_map<std::string, XAttrVec> XAttrMap;

class XAttrTable : public DBTable<XAttrRow> {

public:

  XAttrTable() : DBTable("hdfs_xattrs"){
    addColumn("inode_id");
    addColumn("namespace");
    addColumn("name");
    addColumn("value");
  }

  XAttrRow getRow(NdbRecAttr* values[]) {
    XAttrRow row;
    row.mInodeId = values[0]->int64_value();
    row.mNamespace = values[1]->int8_value();
    row.mName = get_string(values[2]);
    row.mValue = get_string(values[3]);
    return row;
  }

  XAttrRow get(Ndb* connection, Int64 inodeId, Int8 ns, std::string name) {
    AnyMap a;
    a[0] = inodeId;
    a[1] = ns;
    a[2] = name;
    return DBTable<XAttrRow>::doRead(connection, a);
  }

  XAttrMap get(Ndb* connection, Fmq* data_batch) {
    AnyVec anyVec;
    Fmq batchedMutations;

    Fmq addAllXattrs;

    for (Fmq::iterator it = data_batch->begin();
         it != data_batch->end(); ++it) {
      FsMutationRow row = *it;
      if (!row.requiresReadingXAttr() || !row.isXAttrOperation()) {
        continue;
      }

      if(row.mOperation == XAttrAddAll){
        addAllXattrs.push_back(row);
        continue;
      }

      AnyMap pk;
      pk[0] = row.mInodeId;
      pk[1] = row.getNamespace();
      pk[2] = row.getXAttrName();
      anyVec.push_back(pk);
      batchedMutations.push_back(row);
    }

    XAttrVec xattrs = doRead(connection, anyVec);

    XAttrMap results;

    int i=0;
    for(XAttrVec::iterator it = xattrs.begin(); it != xattrs.end(); ++it, i++){
      XAttrRow xattr = *it;
      FsMutationRow mr = batchedMutations[i];

      XAttrVec xvec;
      xvec.push_back(xattr);
      results[mr.getPKStr()] = xvec;
    }

    for(Fmq::iterator it = addAllXattrs.begin(); it != addAllXattrs.end();
    ++it){
      FsMutationRow mr = *it;
      results[mr.getPKStr()] = getByInodeId(connection, mr.mInodeId);
    }

    return results;
  }

  XAttrVec getByInodeId(Ndb* connection, Int64 inodeId){
    AnyMap args;
    args[0] = inodeId;
    return doRead(connection, PRIMARY_INDEX, args);
  }

  boost::optional<XAttrRow> get(Ndb* connection, XAttrPK key) {
    XAttrRow row = get(connection, key.mInodeId, key.mNamespace, key.mName);
    if(readCheckExists(key, row)) {
      return row;
    } else {
      return boost::none;
    }
  }

private:
  inline static bool readCheckExists(XAttrPK key, XAttrRow row) {
    return key.mInodeId == row.mInodeId && key.mNamespace == row.mNamespace && key.mName == row.mName;
  }
};
#endif //EPIPE_XATTRTABLE_H
