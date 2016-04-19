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
 * File:   MetadataTableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "MetadataTableTailer.h"

const char *METADATA_TABLE_NAME= "meta_data";
const int NO_METADATA_TABLE_COLUMNS= 3;
const char *METADATA_TABLE_COLUMNS[NO_METADATA_TABLE_COLUMNS]=
    {"id",
     "fieldid",
     "tupleid"
    };

const int METADATA_NUM_EVENT_TYPES_TO_WATCH = 1; 
const NdbDictionary::Event::TableEvent METADATA_EVENT_TYPES_TO_WATCH[METADATA_NUM_EVENT_TYPES_TO_WATCH] = { NdbDictionary::Event::TE_INSERT } ;

MetadataTableTailer::MetadataTableTailer(Ndb* ndb, const int poll_maxTimeToWait) : TableTailer(ndb, METADATA_TABLE_NAME, METADATA_TABLE_COLUMNS, 
        NO_METADATA_TABLE_COLUMNS, METADATA_EVENT_TYPES_TO_WATCH,METADATA_NUM_EVENT_TYPES_TO_WATCH, poll_maxTimeToWait) {

}

void MetadataTableTailer::handleEvent(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
    LOG_INFO() << " Metadata change";
    LOG_INFO() << "Id= " << value[0]->int32_value();
    LOG_INFO() << "FieldId= " << value[1]->int32_value();
    LOG_INFO() << "TupleId= " << value[2]->int32_value();
}

MetadataTableTailer::~MetadataTableTailer() {
}

