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

struct XAttrRowPart{
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;
  Int16 mIndex;
  Int16 mNumParts;
  std::string mValue;

  std::string to_string(){
    std::stringstream stream;
    stream << "-------------------------" << std::endl;
    stream << "InodeId = " << mInodeId << std::endl;
    stream << "Namespace = " << std::to_string(mNamespace) << std::endl;
    stream << "Name = " << mName << std::endl;
    stream << "Index = " << mIndex << std::endl;
    stream << "NumParts = " << mNumParts << std::endl;
    stream << "Value = " << mValue << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }

  std::string getXAttrUniqueId(){
    std::stringstream out;
    out << mInodeId << "-" << std::to_string(mNamespace) << "-" << mName;
    return out.str();
  }

  static std::string getXAttrUniqueId(FsMutationRow& row){
    std::stringstream out;
    out << row.mInodeId << "-" << std::to_string(row.getNamespace()) << "-" << row.getXAttrName();
    return out.str();
  }
};

typedef std::vector<XAttrRowPart> XAttrPartVec;

struct XAttrRow {
  Int64 mInodeId;
  Int8 mNamespace;
  std::string mName;
  std::string mValue;

  XAttrRow(XAttrPartVec& vec){
    if(!vec.empty()){
      mInodeId = vec[0].mInodeId;
      mNamespace = vec[0].mNamespace;
      mName = vec[0].mName;
      mValue = combineValues(vec);
    }
  }

  XAttrRow(XAttrRowPart& firstPart, XAttrPartVec& restOfParts) 
  : XAttrRow(firstPart.mInodeId, firstPart.mNamespace, firstPart.mName, combineValues(firstPart, restOfParts)){
  }

  XAttrRow(XAttrRowPart& row) : XAttrRow(row.mInodeId, row.mNamespace, row.mName, row.mValue){
  }

  XAttrRow(Int64 inodeId, Int8 ns, std::string name, std::string value){
    mInodeId = inodeId;
    mNamespace = ns;
    mName = name;
    mValue = value;
  }

  std::string to_upsert_json(std::string index, FsOpType operation){
    std::stringstream out;
    if(operation == XAttrUpdate){
      out << getDocUpdatePrefix(index, mInodeId) << std::endl;
      out << removeXAttrScript(mName) << std::endl;
    }
    out << getDocUpdatePrefix(index, mInodeId) << std::endl;
    out << upsert() << std::endl;
    return out.str();
  }

  std::string to_upsert_json(std::string index){
    std::stringstream out;
    out << getDocUpdatePrefix(index, mInodeId) << std::endl;
    out << upsert() << std::endl;
    return out.str();
  }

  static std::string to_delete_json(std::string index, FsMutationRow row){
    std::stringstream out;
    out << getDocUpdatePrefix(index, row.mInodeId) << std::endl;
    if(row.mOperation == XAttrAddAll){
      out << removeAllXAttrsScript() << std::endl;
    }else {
      out << removeXAttrScript(row.getXAttrName()) << std::endl;
    }
    return out.str();
  }

  std::string to_string(){
    std::stringstream stream;
    stream << "-------------------------" << std::endl;
    stream << "InodeId = " << mInodeId << std::endl;
    stream << "Namespace = " << (int)mNamespace << std::endl;
    stream << "Name = " << mName << std::endl;
    stream << "Value = " << mValue << std::endl;
    stream << "ValueSize = " << mValue.length() << std::endl;
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

  static std::string getDocUpdatePrefix(std::string index, Int64 inodeId){
    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("update");
    opWriter.StartObject();

    opWriter.String("_id");
    opWriter.Int64(inodeId);
    opWriter.String("_index");
    opWriter.String(index.c_str());

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

  static std::string removeAllXAttrsScript(){

    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("script");

    std::stringstream rmout;
    rmout << "if(ctx._source.containsKey(\"" << XATTR_FIELD_NAME << "\")){ ";
    rmout << "ctx._source.remove(\"" << XATTR_FIELD_NAME<<  "\");";
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

  std::string combineValues(XAttrRowPart& firstPart, XAttrPartVec& restOfParts){
    std::string value = firstPart.mValue + combineValues(restOfParts);
    return value;
  }

  std::string combineValues(XAttrPartVec& partVec){
    std::string value;
    for(auto & part : partVec){
       value = value + part.mValue;
    }
    return value;
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
    out << mInodeId << "-" << std::to_string(mNamespace) << "-" << mName;
    return out.str();
  }
};

typedef std::vector<XAttrRow> XAttrVec;
typedef boost::unordered_map<std::string, XAttrVec> XAttrMap;
typedef boost::unordered_map<std::string, XAttrPartVec> XAttrPartMap;

class XAttrTable : public DBTable<XAttrRowPart> {

public:

  XAttrTable() : DBTable("hdfs_xattrs"){
    addColumn("inode_id");
    addColumn("namespace");
    addColumn("name");
    addColumn("index");
    addColumn("value");
    addColumn("num_parts");
  }

  XAttrRowPart getRow(NdbRecAttr* values[]) {
    XAttrRowPart row;
    row.mInodeId = values[0]->int64_value();
    row.mNamespace = values[1]->int8_value();
    row.mName = get_string(values[2]);
    row.mIndex = values[3]->short_value();
    row.mValue = get_string(values[4]);
    row.mNumParts = values[5]->short_value();
    return row;
  }

  XAttrRow get(Ndb* connection, Int64 inodeId, Int8 ns, std::string name) {
    AnyMap a;
    a[0] = inodeId;
    a[1] = ns;
    a[2] = name;
    a[3] = static_cast<Int16>(0);

    XAttrRowPart firstPart = DBTable<XAttrRowPart>::doRead(connection, a);
    LOG_DEBUG("XAttr get by parts " << firstPart.mNumParts);
    if(firstPart.mNumParts == 1){
      return XAttrRow(firstPart);
    }

    AnyVec anyVec;
    for(Int16 index=1; index < firstPart.mNumParts; index++){
        AnyMap pk;
        pk[0] = inodeId;
        pk[1] = ns;
        pk[2] = name;
        pk[3] = index;
        anyVec.push_back(pk);
    }
    
    XAttrPartVec restOfParts = doRead(connection, anyVec);
    LOG_DEBUG("XAttr batch read the rest of parts " << restOfParts.size());
    return XAttrRow(firstPart, restOfParts);
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

      LOG_DEBUG("doRead batch for XAttr [ " + row.getXAttrName() + " ] to get its " << row.getNumParts() << " parts ");
      for(Int16 index=0; index < row.getNumParts(); index++){
        AnyMap pk;
        pk[0] = row.mInodeId;
        pk[1] = row.getNamespace();
        pk[2] = row.getXAttrName();
        pk[3] = index;
        anyVec.push_back(pk);
      }
      batchedMutations.push_back(row);
    }

    XAttrPartVec xattrsParts = doRead(connection, anyVec);
    XAttrMap results = combine(xattrsParts, batchedMutations);

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
    XAttrPartVec xattrsParts = doRead(connection, PRIMARY_INDEX, args, inodeId);
    return combine(xattrsParts);
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

  XAttrVec combine(XAttrPartVec& partVec){
     XAttrPartMap xAttrsByName;
     convert(partVec, xAttrsByName);
     XAttrVec results;
     for(auto& e : xAttrsByName){
       results.push_back(XAttrRow(e.second));
     }
     return results;
  }

  XAttrMap combine(XAttrPartVec& partVec, Fmq& xAttrMutations){
    XAttrPartMap xAttrsByName;
    convert(partVec, xAttrsByName);
    XAttrMap results;
    for(auto& m : xAttrMutations){
      std::string id = XAttrRowPart::getXAttrUniqueId(m);
      auto& vec = xAttrsByName[id];
      XAttrVec xvec;
      xvec.push_back(XAttrRow(vec));
      results[m.getPKStr()] = xvec;
    }

    return results;
  }
  
  void convert(XAttrPartVec& partVec, XAttrPartMap& xAttrsByName){
    for(auto& part : partVec){
      std::string id = part.getXAttrUniqueId();
      if(xAttrsByName.find(id) == xAttrsByName.end()){
        xAttrsByName[id] = XAttrPartVec();
      }
      xAttrsByName[id].push_back(part);
    }

    for(auto& e :xAttrsByName){
      auto& vec = e.second;
      std::sort(vec.begin(), vec.end(), [](XAttrRowPart a, XAttrRowPart b){
        return a.mIndex < b.mIndex;
      });
    }
  }

};
#endif //EPIPE_XATTRTABLE_H
