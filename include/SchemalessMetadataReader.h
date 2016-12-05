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
 * File:   SchemalessMetadataReader.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef SCHEMALESSMETADATAREADER_H
#define SCHEMALESSMETADATAREADER_H

#include "NdbDataReader.h"
#include "SchemalessMetadataTailer.h"
#include "HopsworksOpsLogTailer.h"

class SchemalessMetadataReader : public NdbDataReader<SchemalessMetadataEntry, MConn> {
public:
    SchemalessMetadataReader(MConn* connections, const int num_readers, const bool hopsworks,
            ElasticSearch* elastic, ProjectDatasetINodeCache* cache);
    virtual ~SchemalessMetadataReader();
private:
    virtual void processAddedandDeleted(MConn connection, Smq* data_batch, Bulk& bulk);
    void createJSON(Smq* data_batch, Bulk& bulk);
    void mergeDoc(rapidjson::Document& target, rapidjson::Document& source);
};

#endif /* SCHEMALESSMETADATAREADER_H */

