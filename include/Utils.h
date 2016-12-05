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
 * File:   Utils.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef UTILS_H
#define UTILS_H

#include "common.h"
#include "ProjectDatasetINodeCache.h"
#include<cstdlib>
#include<cstring>

typedef boost::posix_time::ptime ptime;

typedef vector<NdbRecAttr*> Row;
typedef boost::unordered_map<int, Row> UIRowMap;

typedef Ndb* SConn;

struct MConn{
    SConn inodeConnection;
    SConn metadataConnection;
};

namespace Utils {

    namespace NdbC {

        inline static const NdbDictionary::Dictionary* getDatabase(Ndb* connection) {
            const NdbDictionary::Dictionary* database = connection->getDictionary();
            if (!database) LOG_NDB_API_ERROR(connection->getNdbError());
            return database;
        }

        inline static const NdbDictionary::Table* getTable(const NdbDictionary::Dictionary* database, const string& table_name) {
            const NdbDictionary::Table* table = database->getTable(table_name.c_str());
            if (!table) LOG_NDB_API_ERROR(database->getNdbError());
            return table;
        }

        inline static const NdbDictionary::Index* getIndex(const NdbDictionary::Dictionary* database, const string& table_name, const string& index_name) {
            const NdbDictionary::Index* index = database->getIndex(index_name.c_str(), table_name.c_str());
            if (!index) LOG_NDB_API_ERROR(database->getNdbError());
            return index;
        }
        
        inline static NdbOperation* getNdbOperation(NdbTransaction* transaction, const NdbDictionary::Table* table) {
            NdbOperation* op = transaction->getNdbOperation(table);
            if (!op) LOG_NDB_API_ERROR(transaction->getNdbError());
            return op;
        }

        inline static NdbScanOperation* getNdbScanOperation(NdbTransaction* transaction, const NdbDictionary::Table* table) {
            NdbScanOperation* op = transaction->getNdbScanOperation(table);
            if (!op) LOG_NDB_API_ERROR(transaction->getNdbError());
            return op;
        }
         
        inline static NdbRecAttr* getNdbOperationValue(NdbOperation* op, const string& column_name) {
            NdbRecAttr* col = op->getValue(column_name.c_str());
            if (!col) LOG_NDB_API_ERROR(op->getNdbError());
            return col;
        }
             
        inline static NdbIndexScanOperation* getNdbIndexScanOperation(NdbTransaction* transaction, const NdbDictionary::Index* index) {
            NdbIndexScanOperation* op = transaction->getNdbIndexScanOperation(index);
            if (!op) LOG_NDB_API_ERROR(transaction->getNdbError());
            return op;
        }
        
        inline static NdbTransaction* startNdbTransaction(Ndb* connection) {
            NdbTransaction* ts = connection->startTransaction();
            if (!ts) LOG_NDB_API_ERROR(connection->getNdbError());
            return ts;
        }

        inline static void executeTransaction(NdbTransaction* transaction, NdbTransaction::ExecType exec_type) {
            if (transaction->execute(exec_type) == -1) {
                LOG_NDB_API_ERROR(transaction->getNdbError());
            }
        }
        
        inline static string get_ndb_varchar(string str, NdbDictionary::Column::ArrayType array_type) {
            stringstream data;
            int len = str.length();

            switch (array_type) {
                case NdbDictionary::Column::ArrayTypeFixed:
                    /*
                     No prefix length is stored in aRef. Data starts from aRef's first byte
                     data might be padded with blank or null bytes to fill the whole column
                     */
                    data << str;
                    break;
                case NdbDictionary::Column::ArrayTypeShortVar:
                    /*
                     First byte of aRef has the length of data stored
                     Data starts from second byte of aRef
                     */
                    data << ((char) len) << str;
                    break;
                case NdbDictionary::Column::ArrayTypeMediumVar:
                    /*
                     First two bytes of aRef has the length of data stored
                     Data starts from third byte of aRef
                     */
                    int m = len / 256;
                    int l = len % 256;
                    data << ((char) l) << ((char) m) << str;
                    break;
            }
            return data.str();
        }

        /*
         * Based on The example file ndbapi_array_simple.cpp found in 
         * the MySQL Cluster source distribution's storage/ndb/ndbapi-examples directory
         */

        /* extracts the length and the start byte of the data stored */
        inline static int get_byte_array(const NdbRecAttr* attr,
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
                    bytes = (size_t) ((unsigned char)aRef[1]) * 256 + (size_t) ((unsigned char)aRef[0]);
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
        inline static string get_string(const NdbRecAttr* attr) {
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
                return string(str);
            }
            return NULL;
        }

        inline static UIRowMap readTableWithIntPK(const NdbDictionary::Dictionary* database, NdbTransaction* transaction,
                const char* table_name, UISet ids, const char** columns_to_read, const int columns_count, const int column_pk_index) {

            UIRowMap res;
            const NdbDictionary::Table* table = getTable(database, table_name);

            for (UISet::iterator it = ids.begin(); it != ids.end(); ++it) {
                NdbOperation* op = getNdbOperation(transaction, table);
                op->readTuple(NdbOperation::LM_CommittedRead);
                op->equal(columns_to_read[column_pk_index], *it);

                for (int c = 0; c < columns_count; c++) {
                    NdbRecAttr* col = getNdbOperationValue(op, columns_to_read[c]);
                    res[*it].push_back(col);
                }
                LOG_TRACE(" Read " << table_name << " row for [" << *it << "]");
            }
            return res;
        }
    }

    inline static ptime getCurrentTime() {
        //return boost::posix_time::microsec_clock::universal_time()
        return boost::posix_time::microsec_clock::local_time();
    }

    inline static float getTimeDiffInMilliseconds(ptime start, ptime end) {
        boost::posix_time::time_duration diff = end - start;
        return diff.total_microseconds() / 1000.0;
    }

    inline static string concat(const char* a, const string b) {
        string buf(a);
        buf.append(b);
        return buf;
    }
}

#endif /* UTILS_H */

