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

#ifndef SCHEMABASEDMETADATATABLE_H
#define SCHEMABASEDMETADATATABLE_H

#include "DBTable.h"
#include "MetaFieldTable.h"
#include "MetaTableTable.h"
#include "MetaTemplateTable.h"
#include "MetaTupleTable.h"
#include "MetadataLogTable.h"

struct SchemabasedMetadataEntry {
  int mId;
  FieldRow mField;
  TupleRow mTuple;
  std::string mMetadata;
  HopsworksOpType mOperation;

  ptime mEventCreationTime;

  SchemabasedMetadataEntry() {
  }

  SchemabasedMetadataEntry(MetadataLogEntry ml) {
    mId = ml.mMetaPK.mId;
    mField.mId = ml.mMetaPK.mFieldId;
    mTuple.mId = ml.mMetaPK.mTupleId;
    mOperation = ml.mMetaOpType;
    mEventCreationTime = ml.mEventCreationTime;
  }

  bool is_equal(MetadataKey metaKey) {
    return mId == metaKey.mId && mField.mId == metaKey.mFieldId
            && mTuple.mId == metaKey.mTupleId;
  }
  
  bool is_equal(SchemabasedMetadataEntry ml) {
    return mId == ml.mId && mField.mId == ml.mField.mId
            && mTuple.mId == ml.mTuple.mId;
  }

  std::string to_string() {
    std::stringstream stream;
    stream << "-------------------------" << std::endl;
    stream << "Id = " << mId << std::endl;
    stream << "FieldId = " << mField.mId << std::endl;
    stream << "TupleId = " << mTuple.mId << std::endl;
    stream << "Data = " << mMetadata << std::endl;
    stream << "Operation = " << HopsworksOpTypeToStr(mOperation) << std::endl;
    stream << "-------------------------" << std::endl;
    return stream.str();
  }

  std::string to_create_json() {
    if(mField.is_empty()){
      LOG_ERROR("Empty Schema : " << to_string());
      return "";
    }
    
    std::stringstream out;
    rapidjson::StringBuffer sbOp;
    rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

    opWriter.StartObject();

    opWriter.String("update");
    opWriter.StartObject();

    opWriter.String("_id");
    opWriter.Int(mTuple.mInodeId);

    opWriter.EndObject();
    opWriter.EndObject();

    out << sbOp.GetString() << std::endl;

    rapidjson::StringBuffer sbDoc;
    rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);

    docWriter.StartObject();
    docWriter.String("doc");
    docWriter.StartObject();

    docWriter.String(XATTR_FIELD_NAME);
    docWriter.StartObject();

    docWriter.String(mField.mTable.mTemplate.mName.c_str());
    docWriter.StartObject();

    docWriter.String(mField.mTable.mName.c_str());
    docWriter.StartObject();

    docWriter.String(mField.mName.c_str());

    switch (mField.mType) {
      case BOOL:
      {
        bool boolVal = mMetadata == "true" || mMetadata == "1";
        docWriter.Bool(boolVal);
        break;
      }
      case INT:
      {
        try {
          int intVal = DONT_EXIST_INT();
          if (mOperation != HopsworksDelete) {
            intVal = boost::lexical_cast<int>(mMetadata);
          }
          docWriter.Int(intVal);
        } catch (boost::bad_lexical_cast &e) {
          LOG_ERROR("Error while casting [" << mMetadata << "] to int" << e.what());
        }

        break;
      }
      case DOUBLE:
      {
        try {
          double doubleVal = DONT_EXIST_INT();
          if (mOperation != HopsworksDelete) {
            doubleVal = boost::lexical_cast<double>(mMetadata);
          }
          docWriter.Double(doubleVal);
        } catch (boost::bad_lexical_cast &e) {
          LOG_ERROR("Error while casting [" << mMetadata << "] to double" << e.what());
        }

        break;
      }
      case TEXT:
      {
        std::string stringVal = mMetadata;
        if (mOperation == HopsworksDelete) {
          stringVal = DONT_EXIST_STR();
        }
        docWriter.String(stringVal.c_str());
        break;
      }
    }

    docWriter.EndObject();

    docWriter.EndObject();

    docWriter.EndObject();
    docWriter.EndObject();

    docWriter.String("doc_as_upsert");
    docWriter.Bool(true);

    docWriter.EndObject();

    out << sbDoc.GetString() << std::endl;
    return out.str();
  }
};

typedef std::vector<SchemabasedMetadataEntry> SchemabasedMq;

class SchemabasedMetadataTable : public DBTable<SchemabasedMetadataEntry> {
public:

  SchemabasedMetadataTable(int lru_cap) : DBTable("meta_data"), mFieldsTable(lru_cap) {
    addColumn("id");
    addColumn("fieldid");
    addColumn("tupleid");
    addColumn("data");
  }

  SchemabasedMetadataEntry getRow(NdbRecAttr* values[]) {
    SchemabasedMetadataEntry row;
    row.mId = values[0]->int32_value();
    row.mField.mId = values[1]->int32_value();
    row.mTuple.mId = values[2]->int32_value();
    row.mMetadata = get_string(values[3]);
    return row;
  }
  
  SchemabasedMetadataEntry currRow(Ndb* connection){
    SchemabasedMetadataEntry row = DBTable<SchemabasedMetadataEntry>::currRow();
    row.mField = mFieldsTable.get(connection, row.mField.mId);
    row.mTuple = mTuplesTable.get(connection, row.mTuple.mId);
    return row;
  }
  
  SchemabasedMq* get(Ndb* connection, MetaQ* batch, UISet& primaryKeys) {

    UISet fields_ids;
    UISet tuples_ids;
    AnyVec args;
    SchemabasedMq schemaBasedQ;
    for (MetaQ::iterator it = batch->begin(); it != batch->end(); ++it) {
      MetadataLogEntry le = *it;
      primaryKeys.insert(le.mId);
      
      SchemabasedMetadataEntry ml = SchemabasedMetadataEntry(le);
      
      fields_ids.insert(ml.mField.mId);
      tuples_ids.insert(ml.mTuple.mId);
      
      schemaBasedQ.push_back(ml);
      
      if (ml.mOperation == HopsworksDelete) {
        continue;
      }
      
      AnyMap a;
      a[0] = ml.mId;
      a[1] = ml.mField.mId;
      a[2] = ml.mTuple.mId;
      
      args.push_back(a);
      
      LOG_TRACE("Read SchameBasedMetadata row for [" << ml.mId << ","
              << ml.mField.mId << "," << ml.mTuple.mId << "]");
    }

    SchemabasedMq readFromDb = doRead(connection, args);
    
    //Update Caches
    mFieldsTable.updateCache(connection, fields_ids);

    TupleMap tuples = mTuplesTable.get(connection, tuples_ids);
    
    SchemabasedMq* populated_res = new SchemabasedMq();
    int i=0;
    for(SchemabasedMq::iterator it=schemaBasedQ.begin(); it != schemaBasedQ.end(); ++it, i++){
      SchemabasedMetadataEntry row = *it;
      if(row.mOperation != HopsworksDelete){
        SchemabasedMetadataEntry read = readFromDb[i];
        if(!read.is_equal(row)){
          LOG_WARN("Ignore " << row.to_string() << " since it seems to be deleted");
          continue;
        }
        row = read;
      }
      
      boost::optional<FieldRow> field_ptr = mFieldsTable.getFromCache(row.mField.mId);
      if (field_ptr) {
        row.mField = field_ptr.get();
      }
      row.mTuple = tuples[row.mTuple.mId];
      
      populated_res->push_back(row);
    }
    
    return populated_res;
  }


private:
  MetaFieldTable mFieldsTable;
  MetaTupleTable mTuplesTable;
};


#endif /* SCHEMABASEDMETADATATABLE_H */

