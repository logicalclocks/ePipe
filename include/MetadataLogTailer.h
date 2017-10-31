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
 * File:   MetadataLogTailer.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef METADATALOGTAILER_H
#define METADATALOGTAILER_H

#include "RCTableTailer.h"
#include "ConcurrentPriorityQueue.h"
#include "HopsworksOpsLogTailer.h"

struct MetadataKey
{
  int mPK1;
  int mPK2;
  int mPK3;
  
  MetadataKey(){
  }
  
  MetadataKey(int pk1, int pk2, int pk3){
      mPK1 = pk1;
      mPK2 = pk2;
      mPK3 = pk3;
  }
  bool operator == (const MetadataKey &other) const {
    return (mPK1 == other.mPK1) && (mPK2 == other.mPK2)  && (mPK3 == other.mPK3);
  }
  
  string to_string(){
      stringstream stream;
      stream << "[" << mPK1 << "," << mPK2 << "," << mPK3 << "]";
      return stream.str();
  }
};

struct MetadataKeyHasher
{
  std::size_t operator () (const MetadataKey &key) const 
  {
    std::size_t seed = 0;
    boost::hash_combine(seed, boost::hash_value(key.mPK1));
    boost::hash_combine(seed, boost::hash_value(key.mPK2));
    boost::hash_combine(seed, boost::hash_value(key.mPK3));
    return seed;
  }
};

struct MetadataLogEntry {
     int mId;
     MetadataKey mMetaPK;
     OperationType mMetaOpType;
     MetadataType mMetaType;
     
     ptime mEventCreationTime;
     
     string to_string(){
        stringstream stream;
        stream << "-------------------------" << endl;
        stream << "Id = " << mId << endl;
        stream << "MetaType = " << Utils::MetadataTypeToStr(mMetaType) << endl;
        stream << "MetaPK = " << mMetaPK.to_string() << endl;
        stream << "MetaOpType = " << Utils::OperationTypeToStr(mMetaOpType) << endl;
        stream << "-------------------------" << endl;
        return stream.str();
     }
};

struct MetadataLogEntryComparator
{
    bool operator()(const MetadataLogEntry &r1, const MetadataLogEntry &r2) const
    {
        return r1.mId > r2.mId;
    }
};


struct SchemabasedMetadataEntry {
     int mId;
     int mFieldId;
     int mTupleId;
     string mMetadata;
     OperationType mOperation;
     
     ptime mEventCreationTime;
     
     SchemabasedMetadataEntry(MetadataLogEntry ml){
         mId = ml.mMetaPK.mPK1;
         mFieldId = ml.mMetaPK.mPK2;
         mTupleId =  ml.mMetaPK.mPK3;
         mOperation = ml.mMetaOpType;
         mEventCreationTime = ml.mEventCreationTime;
     }
     
     string to_string(){
        stringstream stream;
        stream << "-------------------------" << endl;
        stream << "Id = " << mId << endl;
        stream << "FieldId = " << mFieldId << endl;
        stream << "TupleId = " << mTupleId << endl;
        stream << "Data = " << mMetadata << endl;
        stream << "Operation = " << Utils::OperationTypeToStr(mOperation) << endl;
        stream << "-------------------------" << endl;
        return stream.str();
     }
};

struct SchemalessMetadataEntry {
    int mId;
    int mINodeId;
    int mParentId;
    string mJSONData;
    OperationType mOperation;
    ptime mEventCreationTime;

    SchemalessMetadataEntry(MetadataLogEntry ml) {
        mId = ml.mMetaPK.mPK1;
        mINodeId = ml.mMetaPK.mPK2;
        mParentId = ml.mMetaPK.mPK3;
        mOperation = ml.mMetaOpType;
        mEventCreationTime = ml.mEventCreationTime;
    }
    
    string to_string() {
        stringstream stream;
        stream << "-------------------------" << endl;
        stream << "Id = " << mId << endl;
        stream << "INodeId = " << mParentId << endl;
        stream << "ParentId = " << mParentId << endl;
        stream << "Operation = " << Utils::OperationTypeToStr(mOperation) << endl;
        stream << "Data = " << mJSONData << endl;
        stream << "-------------------------" << endl;
        return stream.str();
    }
};


typedef ConcurrentPriorityQueue<MetadataLogEntry, MetadataLogEntryComparator> CMetaQ;
typedef vector<MetadataLogEntry> MetaQ;
typedef boost::unordered_map<MetadataKey, Row, MetadataKeyHasher> UMetadataKeyRowMap;

typedef vector<SchemabasedMetadataEntry> SchemabasedMq;
typedef vector<SchemalessMetadataEntry> SchemalessMq;

class MetadataLogTailer : public RCTableTailer<MetadataLogEntry> {
public:
    MetadataLogTailer(Ndb* ndb, const int poll_maxTimeToWait, const Barrier barrier);
    MetadataLogEntry consumeMultiQueue(int queue_id);
    MetadataLogEntry consume();
    
    static void removeLogs(Ndb* conn, UISet& pks);
    
    static SchemabasedMq* readSchemaBasedMetadataRows(const NdbDictionary::Dictionary* database, 
        NdbTransaction* transaction, MetaQ* batch, UISet& primaryKeys);
    
    static SchemalessMq* readSchemalessMetadataRows(const NdbDictionary::Dictionary* database, 
        NdbTransaction* transaction, MetaQ* batch, UISet& primaryKeys);
    
    virtual ~MetadataLogTailer();
private:
    static UMetadataKeyRowMap readMetadataRows(const NdbDictionary::Dictionary* database, 
        NdbTransaction* transaction, const char* table_name, MetaQ* batch, const char** columns_to_read, 
        const int columns_count, const int column_pk1, const int column_pk2, const int column_pk3);
    
    static const WatchTable TABLE;

    virtual void handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]);
    CMetaQ* mSchemaBasedQueue;
    CMetaQ* mSchemalessQueue;
};

#endif /* METADATALOGTAILER_H */

