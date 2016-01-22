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
 * File:   NdbUtils.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef NDBUTILS_H
#define NDBUTILS_H

#include "common.h"
#include<cstdlib>
#include<cstring>

namespace Utils {

    static const char* concat(const char* a, const char* b) {
        std::string buf(a);
        buf.append(b);
        return buf.c_str();
    }

    /*
     * Based on The example file ndbapi_array_simple.cpp found in 
     * the MySQL Cluster source distribution's storage/ndb/ndbapi-examples directory
     */

    /* extracts the length and the start byte of the data stored */
    static int get_byte_array(const NdbRecAttr* attr,
            const char*& first_byte,
            size_t& bytes) {
        const NdbDictionary::Column::ArrayType array_type =
                attr->getColumn()->getArrayType();
        const size_t attr_bytes = attr->get_size_in_bytes();
        const char* aRef = attr->aRef();

        switch (array_type) {
            case NdbDictionary::Column::ArrayTypeFixed:
                /*
                 No prefix length is stored in aRef. Data starts from aRef's first byte
                 data might be padded with blank or null bytes to fill the whole column
                 */
                first_byte = aRef;
                bytes = attr_bytes;
                return 0;
            case NdbDictionary::Column::ArrayTypeShortVar:
                /*
                 First byte of aRef has the length of data stored
                 Data starts from second byte of aRef
                 */
                first_byte = aRef + 1;
                bytes = (size_t) (aRef[0]);
                return 0;
            case NdbDictionary::Column::ArrayTypeMediumVar:
                /*
                 First two bytes of aRef has the length of data stored
                 Data starts from third byte of aRef
                 */
                first_byte = aRef + 2;
                bytes = (size_t) (aRef[1]) * 256 + (size_t) (aRef[0]);
                return 0;
            default:
                first_byte = NULL;
                bytes = 0;
                return -1;
        }
    }

    /*
     Extracts the string from given NdbRecAttr
     Uses get_byte_array internally
     */
    static const char* get_string(const NdbRecAttr* attr) {
        size_t attr_bytes;
        const char* data_start_ptr = NULL;

        /* get stored length and data using get_byte_array */
        if (get_byte_array(attr, data_start_ptr, attr_bytes) == 0) {
            /* we have length of the string and start location */
            string str = string(data_start_ptr, attr_bytes);
            if (attr->getType() == NdbDictionary::Column::Char) {
                /* Fixed Char : remove blank spaces at the end */
                size_t endpos = str.find_last_not_of(" ");
                if (string::npos != endpos) {
                    str = str.substr(0, endpos + 1);
                }
            }
            return str.c_str();
        }
        return NULL;
    }
}

#endif /* NDBUTILS_H */

