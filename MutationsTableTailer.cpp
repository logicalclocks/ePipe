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
 * File:   MutationsTableTailer.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "MutationsTableTailer.h"
#include "Utils.h"

using namespace Utils;

const char *MUTATION_TABLE_NAME= "hdfs_metadata_log2";
const int NO_MUTATION_TABLE_COLUMNS= 6;
const char *MUTATION_TABLE_COLUMNS[NO_MUTATION_TABLE_COLUMNS]=
    {"dataset_id",
     "inode_id",
     "inode_pid",
     "inode_name",
     "logical_time",
     "operation"
    };

const NdbDictionary::Event::TableEvent WATCH_EVENT_TYPE = NdbDictionary::Event::TE_INSERT;

MutationsTableTailer::MutationsTableTailer(Ndb* ndb) : TableTailer(ndb, MUTATION_TABLE_NAME, MUTATION_TABLE_COLUMNS, 
        NO_MUTATION_TABLE_COLUMNS, WATCH_EVENT_TYPE) {

}

void MutationsTableTailer::handleValue(NdbDictionary::Event::TableEvent eventType, NdbRecAttr* preValue[], NdbRecAttr* value[]){
    printf("-------------------------\n");
    printf("%s = from (%i) to (%i) \n", MUTATION_TABLE_COLUMNS[0], preValue[0]->int32_value(), value[0]->int32_value());
    printf("%s = from (%i) to (%i) \n", MUTATION_TABLE_COLUMNS[1], preValue[1]->int32_value(), value[1]->int32_value());
    printf("%s = from (%i) to (%i) \n", MUTATION_TABLE_COLUMNS[2], preValue[2]->int32_value(), value[2]->int32_value());

    printf("%s = from (%s) to (%s) \n", MUTATION_TABLE_COLUMNS[3], get_string(preValue[3]), get_string(value[3]));
    printf("%s = from (%i) to (%i) \n", MUTATION_TABLE_COLUMNS[4], preValue[4]->int32_value(), value[4]->int32_value());
    printf("%s = from (%i) to (%i) \n", MUTATION_TABLE_COLUMNS[5], preValue[5]->int8_value(), value[5]->int8_value());
    printf("-------------------------\n");
    
}

MutationsTableTailer::~MutationsTableTailer() {
}

