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

struct Field{
    string mName;
    bool mSearchable;
    int mTableId;
};

struct Table{
   string mName;
   int mTemplateId;
};

class MetadataReader : public NdbDataReader<Mq_Mq>{
public:
    MetadataReader(Ndb** connections, const int num_readers, string elastic_ip);
    virtual ~MetadataReader();
private:
    virtual ReadTimes readData(Ndb* connection, Mq_Mq data_batch);
    
    UIRowMap readMetadataColumns(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, Mq* added);
    UISet readFields(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet fields_ids);
    UISet readTables(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet tables_ids);
    void readTemplates(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet templates_ids);
    
    string createJSON(UIRowMap tuples, Mq* added);
    
    Cache<int, Field> mFieldsCache;
    Cache<int, Table> mTablesCache;
    Cache<int, string> mTemplatesCache;
};

#endif /* METADATAREADER_H */

