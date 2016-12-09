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
 * File:   SchemalessMetadataReader.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "SchemalessMetadataReader.h"
#include "Tables.h"

SchemalessMetadataReader::SchemalessMetadataReader(MConn* connections,
        const int num_readers, const bool hopsworks, ElasticSearch* elastic,
        ProjectDatasetINodeCache* cache)
: NdbDataReader<MetadataLogEntry, MConn>(connections, num_readers, hopsworks, elastic, cache) {
}

void SchemalessMetadataReader::processAddedandDeleted(MConn connection, MetaQ* data_batch, Bulk& bulk) {
    
    Ndb* metaConn = connection.metadataConnection;
    const NdbDictionary::Dictionary* metaDatabase = getDatabase(metaConn);
    NdbTransaction* metaTransaction = startNdbTransaction(metaConn);
    
    SchemalessMq* data_queue = MetadataLogTailer::readSchemalessMetadataRows(metaDatabase, metaTransaction, data_batch);
    
    if (mHopsworksEnalbed) {
        UISet inodes_ids;
        for (SchemalessMq::iterator it = data_queue->begin(); it != data_queue->end(); ++it) {
            SchemalessMetadataEntry entry = *it;
            inodes_ids.insert(entry.mINodeId);
        }
        mPDICache->refresh(connection, inodes_ids);
    }
             
    createJSON(data_queue, bulk);
    
    MetadataLogTailer::removeLogs(metaDatabase, metaTransaction, data_batch);
    executeTransaction(metaTransaction, NdbTransaction::Commit);
    
    metaConn->closeTransaction(metaTransaction);  
}

void SchemalessMetadataReader::createJSON(SchemalessMq* data_batch, Bulk& bulk) {

    stringstream empty_doc_stream;
    empty_doc_stream << "{\"doc\" : {\"" << XATTR_FIELD_NAME << "\" : {} }, \"doc_as_upsert\" : true}";
    const char* emptyDoc = empty_doc_stream.str().c_str();

    stringstream scriptStream;
    scriptStream << "ctx._source.remove(\"" << XATTR_FIELD_NAME << "\")";
    const char* removeXattrScript = scriptStream.str().c_str();
    
    vector<ptime> arrivalTimes(data_batch->size());
    stringstream out;
    int i = 0;
    for (SchemalessMq::iterator it = data_batch->begin(); it != data_batch->end(); ++it, i++) {
        SchemalessMetadataEntry entry = *it;
        LOG_TRACE("create JSON for " << entry.to_string());
        arrivalTimes[i] = entry.mEventCreationTime;
           
        // INode Operation
        rapidjson::StringBuffer sbOp;
        rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

        opWriter.StartObject();

        opWriter.String("update");
        opWriter.StartObject();


        if (mHopsworksEnalbed) {
            int datasetId = mPDICache->getDatasetId(entry.mINodeId);
            // set project (rounting) and dataset (parent) ids 
            opWriter.String("_parent");
            opWriter.Int(datasetId);

            opWriter.String("_routing");
            opWriter.Int(mPDICache->getProjectId(datasetId));

            if (datasetId == entry.mINodeId) {
                opWriter.String("_type");
                opWriter.String(mElasticSearch->getDatasetType());
            }

        }

        opWriter.String("_id");
        opWriter.Int(entry.mINodeId);

        opWriter.EndObject();
        opWriter.EndObject();

        out << sbOp.GetString() << endl;
        
        rapidjson::StringBuffer sbDoc;
        rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);
        
        if(entry.mOperation == Delete) {
            docWriter.StartObject();
            docWriter.String("script");
            docWriter.String(removeXattrScript);
            docWriter.EndObject();
        } else {
            rapidjson::Document doc;
            doc.Parse(emptyDoc);
            rapidjson::Document xattr(&doc.GetAllocator());
            if (!xattr.Parse(entry.mJSONData.c_str()).HasParseError()) {
                mergeDoc(doc, xattr);
            } else {
                LOG_ERROR("JSON Parsing error: " << entry.mJSONData);
            }
            doc.Accept(docWriter);
        }

        out << sbDoc.GetString() << endl;
    }

    bulk.mArrivalTimes = arrivalTimes;
    bulk.mJSON = out.str();
}

void SchemalessMetadataReader::mergeDoc(rapidjson::Document& target, rapidjson::Document& source) {
    for (rapidjson::Document::MemberIterator itr = source.MemberBegin(); itr != source.MemberEnd(); ++itr) {
        target["doc"][XATTR_FIELD_NAME].AddMember(itr->name, itr->value, target.GetAllocator());
    }
}

SchemalessMetadataReader::~SchemalessMetadataReader() {

}

