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
 * File:   MetadataReader.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef METADATAREADER_H
#define METADATAREADER_H

#include "MetadataTableTailer.h"
#include "NdbDataReader.h"
#include <boost/lexical_cast.hpp>

enum FieldType{
    BOOL = 0,
    INT = 1,
    DOUBLE = 2,
    TEXT = 3
};

struct Field{
    int mFieldId;
    string mName;
    bool mSearchable;
    int mTableId;
    FieldType mType;
};

struct Table{
   string mName;
   int mTemplateId;
};
typedef boost::unordered_map<int, vector<Field> > UTupleToFields;
typedef boost::unordered_map<string, UTupleToFields > UTableToTuples;
typedef boost::unordered_map<string, UTableToTuples> UTemplateToTables;
typedef boost::unordered_map<int, UTemplateToTables> UInodesToTemplates;

typedef boost::unordered_map<int, MetadataEntry> UFieldIdToMetadataEntry;
typedef boost::unordered_map<int, UFieldIdToMetadataEntry> UTupleIdToMetadataEntries;

struct MConn{
    Ndb* inodeConnection;
    Ndb* metadataConnection;
};

class MetadataReader : public NdbDataReader<Mq_Mq, MConn>{
public:
    MetadataReader(MConn* connections, const int num_readers, string elastic_ip, 
            const bool hopsworks, const string elastic_index, const string elastic_inode_type, 
            ProjectDatasetINodeCache* cache);
    virtual ~MetadataReader();
private:
    virtual ReadTimes readData(MConn connection, Mq_Mq data_batch);
    
    string processAdded(MConn connection, Mq* added, ReadTimes& rt);
    
    UInodesToTemplates readMetadataColumns(const NdbDictionary::Dictionary* database, 
        NdbTransaction* transaction, Mq* added, UTupleIdToMetadataEntries &tupleToRows);
    UISet readFields(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet fields_ids);
    UISet readTables(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet tables_ids);
    void readTemplates(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet templates_ids);
    
    //Read from the inodes database
    void readINodeToDatasetLookup(const NdbDictionary::Dictionary* inodesDatabase, 
        NdbTransaction* inodesTransaction, UInodesToTemplates inodesToTemplates);
    
    string createJSON(UInodesToTemplates inodesToTemplates, UTupleIdToMetadataEntries tupleToEntries);
    UInodesToTemplates getInodesToTemplates(UIRowMap tuples, UTupleIdToMetadataEntries tupleToEntries);
        
    Cache<int, Field> mFieldsCache;
    Cache<int, Table> mTablesCache;
    Cache<int, string> mTemplatesCache;
};

#endif /* METADATAREADER_H */

