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

#ifndef SCHEMABASEDMETADATAREADER_H
#define SCHEMABASEDMETADATAREADER_H

#include "MetadataLogTailer.h"
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

class SchemabasedMetadataReader : public NdbDataReader<MetadataLogEntry, MConn>{
public:
    SchemabasedMetadataReader(MConn* connections, const int num_readers, const bool hopsworks, 
            ElasticSearch* elastic, ProjectDatasetINodeCache* cache, const int lru_cap);
    virtual ~SchemabasedMetadataReader();
private:    
    virtual void processAddedandDeleted(MConn connection, MetaQ* data_batch, Bulk& bulk);
    
    UIRowMap readMetadataColumns(const NdbDictionary::Dictionary* database, 
        NdbTransaction* transaction, SchemabasedMq* added);
    UISet readFields(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet fields_ids);
    UISet readTables(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet tables_ids);
    void readTemplates(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet templates_ids);
    
    void refreshProjectDatasetINodeCache(SConn inode_connection, UIRowMap tuples,
        const NdbDictionary::Dictionary* metaDatabase, NdbTransaction* metaTransaction);  
    
    void createJSON(UIRowMap tuples, SchemabasedMq* data_batch, Bulk& bulk);
    
    Cache<int, Field> mFieldsCache;
    Cache<int, Table> mTablesCache;
    Cache<int, string> mTemplatesCache;
};

#endif /* SCHEMABASEDMETADATAREADER_H */

