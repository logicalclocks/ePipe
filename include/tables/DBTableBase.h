/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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

#ifndef DBTABLEBASE_H
#define DBTABLEBASE_H
#include "Utils.h"

inline static int DONT_EXIST_INT() {
  return -1;
}

inline const static std::string DONT_EXIST_STR() {
  return "-1";
}

typedef typename std::vector<std::string>::size_type strvec_size_type;

class DBTableBase {
public:
  DBTableBase(const std::string table) : mTableName(table) {

  }

  const std::string getName() const {
    return mTableName;
  }

  const std::string getColumn(unsigned int index) const {
    if (index < mColumns.size()) {
      return mColumns[index];
    }
    LOG_ERROR("---");
    return NULL;
  }

  strvec_size_type getNoColumns() const {
    return mColumns.size();
  }

 const char** getColumns() {
    const char** columns = new const char*[getNoColumns()];
    for (strvec_size_type i = 0; i < getNoColumns(); i++) {
      std::string colStr = getColumn(i);
      char* col = new char[colStr.size()];
      strcpy(col, colStr.c_str());
      columns[i] = col;
    }
    return columns;
  }
  
private:
  const std::string mTableName;
  StrVec mColumns;

protected:

  /*
   * Make sure to add the primary key columns first
   */
  void addColumn(std::string column) {
    mColumns.push_back(column);
  }

  const NdbDictionary::Dictionary* getDatabase(Ndb* connection) {
    const NdbDictionary::Dictionary* database = connection->getDictionary();
    if (!database) LOG_NDB_API_ERROR(connection->getNdbError());
    return database;
  }

  const NdbDictionary::Table* getTable(const NdbDictionary::Dictionary* database) {
    return getTable(database, getName());
  }

  const NdbDictionary::Table* getTable(const NdbDictionary::Dictionary*
  database, std::string name) {
    const NdbDictionary::Table* table = database->getTable(name.c_str());
    if (!table) LOG_NDB_API_ERROR(database->getNdbError());
    return table;
  }

  const NdbDictionary::Index* getIndex(const NdbDictionary::Dictionary* database, const std::string& index_name) {
    const NdbDictionary::Index* index = database->getIndex(index_name.c_str(), getName().c_str());
    if (!index) {
      LOG_ERROR("index:" << index_name << " error");
      LOG_NDB_API_ERROR(database->getNdbError());
    }
    return index;
  }

  NdbOperation* getNdbOperation(NdbTransaction* transaction, const NdbDictionary::Table* table) {
    NdbOperation* op = transaction->getNdbOperation(table);
    if (!op) LOG_NDB_API_ERROR(transaction->getNdbError());
    return op;
  }

  NdbScanOperation* getNdbScanOperation(NdbTransaction* transaction, const NdbDictionary::Table* table) {
    NdbScanOperation* op = transaction->getNdbScanOperation(table);
    if (!op) LOG_NDB_API_ERROR(transaction->getNdbError());
    return op;
  }

  NdbRecAttr* getNdbOperationValue(NdbOperation* op, const std::string& column_name) {
    NdbRecAttr* col = op->getValue(column_name.c_str());
    if (!col) LOG_NDB_API_ERROR(op->getNdbError());
    return col;
  }

  NdbRecAttr* getNdbOperationValue(NdbOperation* op, const NdbDictionary::Column* column) {
    NdbRecAttr* col = op->getValue(column);
    if (!col) LOG_NDB_API_ERROR(op->getNdbError());
    return col;
  }

  NdbIndexScanOperation* getNdbIndexScanOperation(NdbTransaction* transaction, const NdbDictionary::Index* index) {
    NdbIndexScanOperation* op = transaction->getNdbIndexScanOperation(index);
    if (!op) LOG_NDB_API_ERROR(transaction->getNdbError());
    return op;
  }

  NdbTransaction* startNdbTransaction(Ndb* connection) {
    NdbTransaction* ts = connection->startTransaction();
    if (!ts) LOG_NDB_API_ERROR(connection->getNdbError());
    return ts;
  }

  void executeTransaction(NdbTransaction* transaction, NdbTransaction::ExecType exec_type) {
    if (transaction->execute(exec_type) == -1) {
      LOG_NDB_API_ERROR(transaction->getNdbError());
    }
  }

  std::string get_ndb_varchar(std::string str, NdbDictionary::Column::ArrayType array_type) {
    std::stringstream data;
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
  int get_byte_array(const NdbRecAttr* attr,
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
        bytes = (size_t) ((unsigned char) aRef[0]);
        return 0;
      case NdbDictionary::Column::ArrayTypeMediumVar:
        /*
         First two bytes of aRef has the length of data stored
         Data starts from third byte of aRef
         */
        first_byte = aRef + 2;
        bytes = (size_t) ((unsigned char) aRef[1]) * 256 + (size_t) ((unsigned char) aRef[0]);
        return 0;
      default:
        first_byte = NULL;
        bytes = 0;
        return -1;
    }
  }

  std::string latin1_to_utf8(std::string &str) {
    std::string strOut;
    for (std::string::iterator it = str.begin(); it != str.end(); ++it) {
      uint8_t ch = *it;
      if (ch < 0x80) {
        strOut.push_back(ch);
      } else {
        strOut.push_back(0xc0 | ch >> 6);
        strOut.push_back(0x80 | (ch & 0x3f));
      }
    }
    return strOut;
  }

  /*
   Extracts the string from given NdbRecAttr
   Uses get_byte_array internally
   */
  std::string get_string(const NdbRecAttr* attr) {
    size_t attr_bytes;
    const char* data_start_ptr = NULL;

    /* get stored length and data using get_byte_array */
    if (get_byte_array(attr, data_start_ptr, attr_bytes) == 0) {
      /* we have length of the string and start location */
      std::string str = std::string(data_start_ptr, attr_bytes);
      if (attr->getType() == NdbDictionary::Column::Char) {
        /* Fixed Char : remove blank spaces at the end */
        size_t endpos = str.find_last_not_of(" ");
        if (std::string::npos != endpos) {
          str = str.substr(0, endpos + 1);
        }
      }
      return latin1_to_utf8(str);
    }
    return NULL;
  }
};



#endif /* DBTABLEBASE_H */

