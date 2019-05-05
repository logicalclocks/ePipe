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
 * File:   XAttrHelper.h
 * Author: Alexandru Ormenisan <aaor@kth.se>
 *
 */

#ifndef EPIPE_XATTRHELPER_H
#define EPIPE_XATTRHELPER_H

#include "XAttrTable.h"

class XAttrHelper {

public:
  static string to_upsert_json(XAttrRow row){
    stringstream out;
    out << getDocUpdatePrefix(row.mInodeId) << endl;
    out << upsert(row) << endl;
    return out.str();
  }

  static string to_delete_json(Int64 inodeId, string name){
    stringstream out;
    out << getDocUpdatePrefix(inodeId) << endl;
    out << removeXAttrScript(name) << endl;
    return out.str();
  }
private:
  static string upsert(XAttrRow row) {
    rapidjson::Document doc;
    doc.Parse(getXAttrDoc(row, true).c_str());
    rapidjson::Document xattr(&doc.GetAllocator());
    if (!xattr.Parse(row.mValue.c_str()).HasParseError()) {
      mergeDoc(row.mName, doc, xattr);
      rapidjson::StringBuffer sbDoc;
      rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
      doc.Accept(docWriter);
      return string(sbDoc.GetString());
    } else {
      LOG_DEBUG("XAttr is non json " << row.mName << "=" << row.mValue);
      return getXAttrDoc(row, false);
    }
  }

  static void mergeDoc(string attrName, rapidjson::Document& target, rapidjson::Document& source) {
    for (rapidjson::Document::MemberIterator itr = source.MemberBegin(); itr != source.MemberEnd(); ++itr) {
      target["doc"][XATTR_FIELD_NAME][attrName.c_str()].AddMember(itr->name,
          itr->value, target.GetAllocator());
    }
  }

  static string removeXAttrScript(string xattrname){

    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("script");

    stringstream rmout;
    rmout << "ctx._source." << XATTR_FIELD_NAME << ".remove(\"" << xattrname <<  "\")";
    opWriter.String(rmout.str().c_str());

    opWriter.EndObject();

    return string(sbOp.GetString());

  }

  static string getXAttrDoc(XAttrRow row, bool isJSONVal){

    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("doc");
    opWriter.StartObject();

    opWriter.String(XATTR_FIELD_NAME);
    opWriter.StartObject();

    opWriter.String(row.mName.c_str());
    if(isJSONVal) {
      opWriter.StartObject();
      opWriter.EndObject();
    }else{
      opWriter.String(row.mValue.c_str());
    }
    opWriter.EndObject();


    opWriter.EndObject();
    opWriter.String("doc_as_upsert");
    opWriter.Bool(true);

    opWriter.EndObject();

    return string(sbOp.GetString());
  }

  static string getDocUpdatePrefix(Int64 inodeId){
    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("update");
    opWriter.StartObject();

    opWriter.String("_id");
    opWriter.Int64(inodeId);

    opWriter.EndObject();
    opWriter.EndObject();

    return string(sbOp.GetString());
  }
};
#endif //EPIPE_XATTRHELPER_H