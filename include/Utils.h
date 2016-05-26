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
#include<cstdlib>
#include<cstring>
#include <boost/network.hpp>
#include "rapidjson/document.h"

typedef boost::posix_time::ptime ptime;
typedef boost::network::http::client httpclient;

typedef vector<NdbRecAttr*> Row;
typedef boost::unordered_map<int, Row> UIRowMap;

namespace Utils {

    namespace NdbC {

        inline static const NdbDictionary::Dictionary* getDatabase(Ndb* connection) {
            const NdbDictionary::Dictionary* database = connection->getDictionary();
            if (!database) LOG_NDB_API_ERROR(connection->getNdbError());
            return database;
        }

        inline static const NdbDictionary::Table* getTable(const NdbDictionary::Dictionary* database, const char* table_name) {
            const NdbDictionary::Table* table = database->getTable(table_name);
            if (!table) LOG_NDB_API_ERROR(database->getNdbError());
            return table;
        }

        inline static NdbOperation* getNdbOperation(NdbTransaction* transaction, const NdbDictionary::Table* table) {
            NdbOperation* op = transaction->getNdbOperation(table);
            if (!op) LOG_NDB_API_ERROR(transaction->getNdbError());
            return op;
        }

        inline static NdbRecAttr* getNdbOperationValue(NdbOperation* op, const char* column_name) {
            NdbRecAttr* col = op->getValue(column_name);
            if (!col) LOG_NDB_API_ERROR(op->getNdbError());
            return col;
        }
        
        inline static NdbIndexOperation* getNdbIndexOperation(NdbTransaction* transaction, const NdbDictionary::Index* index) {
            NdbIndexOperation* op = transaction->getNdbIndexOperation(index);
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

        inline static const char* get_ndb_varchar(string str, NdbDictionary::Column::ArrayType array_type) {
            char* data = NULL;
            int len = str.length();

            switch (array_type) {
                case NdbDictionary::Column::ArrayTypeFixed:
                    /*
                     No prefix length is stored in aRef. Data starts from aRef's first byte
                     data might be padded with blank or null bytes to fill the whole column
                     */
                    data = new char[len];
                    strcpy(data, str.c_str());
                    break;
                case NdbDictionary::Column::ArrayTypeShortVar:
                    /*
                     First byte of aRef has the length of data stored
                     Data starts from second byte of aRef
                     */
                    data = new char[len + 1];
                    data[0] = (char) len;
                    strcpy(data + 1, str.c_str());
                    break;
                case NdbDictionary::Column::ArrayTypeMediumVar:
                    /*
                     First two bytes of aRef has the length of data stored
                     Data starts from third byte of aRef
                     */
                    data = new char[len + 2];
                    int m = len / 256;
                    int l = len - (m * 256);
                    data[0] = (char) l;
                    data[1] = (char) m;
                    strcpy(data + 2, str.c_str());
                    break;
            }
            return data;
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
                LOG_TRACE() << " Read " << table_name << " row for [" << *it << "]";
            }
            return res;
        }
    }


    namespace ElasticSearch {

        inline static string _getElasticSearchBaseUrl(string elasticUrl) {
            string url = "http://" + elasticUrl;
            return url;
        }

        inline static string getElasticSearchBulkUrl(string elasticUrl) {
            string bulkUrl = _getElasticSearchBaseUrl(elasticUrl) + "/_bulk";
            return bulkUrl;
        }

        inline static string _getElasticSearchUrlonIndex(string elasticUrl, string index) {
            string str = _getElasticSearchBaseUrl(elasticUrl) + "/" + index;
            return str;
        }

        inline static string getElasticSearchUrlOnDoc(string elasticUrl, string index, string type, int doc) {
            stringstream out;
            out << _getElasticSearchUrlonIndex(elasticUrl, index) << "/" << type << "/" << doc;
            return out.str();
        }

        inline static string getElasticSearchUpdateDocUrl(string elasticUrl, string index, string type, int doc) {
            string str = getElasticSearchUrlOnDoc(elasticUrl, index, type, doc) + "/_update";
            return str;
        }

        inline static string getElasticSearchUpdateDocUrl(string elasticUrl, string index, string type, int doc, int parent) {
            stringstream out;
            out << getElasticSearchUpdateDocUrl(elasticUrl, index, type, doc) << "?parent=" << parent;
            return out.str();
        }

        inline static string getElasticSearchDeleteDocUrl(string elasticUrl, string index, string type, int doc, int parent) {
            stringstream out;
            out << getElasticSearchUrlOnDoc(elasticUrl, index, type, doc) << "?parent=" << parent;
            return out.str();
        }

        inline static string getElasticSearchDeleteByQueryUrl(string elasticUrl, string index, int routing) {
            stringstream out;
            out << _getElasticSearchUrlonIndex(elasticUrl, index) << "/_query" << "?routing=" << routing;
            return out.str();
        }
        
        inline static string getElasticSearchDeleteByQueryUrl(string elasticUrl, string index, int parent, int routing) {
            stringstream out;
            out << getElasticSearchDeleteByQueryUrl(elasticUrl, index, routing) << "&parent=" << parent;
            return out.str();
        }

        enum HttpOp {
            HTTP_POST,
            HTTP_DELETE
        };

        inline static bool elasticSearchHttpRequest(HttpOp op, string elasticUrl, string json) {
            //TODO: support retry if server is down
            try {
                httpclient::request request_(elasticUrl);
                request_ << boost::network::header("Connection", "close");

                if (!json.empty()) {
                    request_ << boost::network::header("Content-Type", "application/json");

                    char body_str_len[8];
                    sprintf(body_str_len, "%lu", json.length());

                    request_ << boost::network::header("Content-Length", body_str_len);
                    request_ << boost::network::body(json);
                }
                
                string opString;
                httpclient client_;
                httpclient::response response_;
                switch (op) {
                    case HTTP_POST:
                        response_ = client_.post(request_);
                        opString = "POST";
                        break;
                    case HTTP_DELETE:
                        response_ = client_.delete_(request_);
                        opString = "DELETE";
                        break;
                }
                std::string body_ = boost::network::http::body(response_);
                
                LOG_TRACE() << opString << " " << elasticUrl << endl 
                        << json << endl << "Response::" << endl << body_;
                
                rapidjson::Document d;
                if (!d.Parse<0>(body_.c_str()).HasParseError()) {
                    if (d.HasMember("errors")) {
                        const rapidjson::Value &bulkErrors = d["errors"];
                        if (bulkErrors.IsBool() && bulkErrors.GetBool()) {
                            const rapidjson::Value &items = d["items"];
                            stringstream errors;
                            for (rapidjson::SizeType i = 0; i < items.Size(); ++i) {
                                const rapidjson::Value &obj = items[i];
                                for (rapidjson::Value::ConstMemberIterator itr = obj.MemberBegin(); itr != obj.MemberEnd(); ++itr)
                                {
                                    const rapidjson::Value & opObj = itr->value;
                                    if(opObj.HasMember("error")){
                                        const rapidjson::Value & error = opObj["error"];
                                        errors << error.GetString() << ", ";
                                    }
                                }
                            }
                            string errorsStr = errors.str();
                            LOG_ERROR() << " ES got errors: " << errorsStr;
                            return false;
                        }
                    } else if (d.HasMember("error")) {
                        const rapidjson::Value &error = d["error"];
                        LOG_ERROR() << " ES got error: " << error.GetString();
                        return false;
                    }
                }else{
                    LOG_ERROR() << " ES got json error (" << d.GetParseError() << ") while parsing " << body_;
                    return false;
                }
                   
            } catch (std::exception &e) {
                LOG_ERROR() << e.what();
                return false;
            }
            return true;
        }

        inline static bool elasticSearchPOST(string elasticUrl, string json) {
            return elasticSearchHttpRequest(HTTP_POST, elasticUrl, json);
        }

        inline static bool elasticSearchDELETE(string elasticUrl) {
            return elasticSearchHttpRequest(HTTP_DELETE, elasticUrl, string());
        }

        inline static bool elasticSearchDELETE(string elasticUrl, string json) {
            return elasticSearchHttpRequest(HTTP_DELETE, elasticUrl, json);
        }

    }

    inline static ptime getCurrentTime() {
        return boost::posix_time::microsec_clock::local_time();
    }

    inline static float getTimeDiffInMilliseconds(ptime start, ptime end) {
        boost::posix_time::time_duration diff = end - start;
        return diff.total_microseconds() / 1000.0;
    }

    inline static const char* concat(const char* a, const char* b) {
        std::string buf(a);
        buf.append(b);
        char* data = new char[buf.length()];
        strcpy(data, buf.c_str());
        return data;
    }
}

#endif /* UTILS_H */

