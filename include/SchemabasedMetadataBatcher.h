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
 * File:   MetadataBatcher.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef SCHEMABASEDMETADATABATCHER_H
#define SCHEMABASEDMETADATABATCHER_H

#include "MetadataLogTailer.h"
#include "SchemabasedMetadataReader.h"

class SchemabasedMetadataBatcher : public RCBatcher<MetadataLogEntry, MConn>{
public:
    SchemabasedMetadataBatcher(MetadataLogTailer* table_tailer, SchemabasedMetadataReader* data_reader, 
            const int time_before_issuing_ndb_reqs, const int batch_size) 
        : RCBatcher<MetadataLogEntry, MConn>(table_tailer, data_reader, 
                time_before_issuing_ndb_reqs, batch_size, Schemabased){
        
    }
};

#endif /* SCHEMABASEDMETADATABATCHER_H */

