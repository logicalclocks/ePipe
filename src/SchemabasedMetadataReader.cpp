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
 * File:   MetadataReader.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "SchemabasedMetadataReader.h"
#include "Tables.h"

SchemabasedMetadataReader::SchemabasedMetadataReader(MConn* connections, const int num_readers, const bool hopsworks, 
        ElasticSearch* elastic, ProjectDatasetINodeCache* cache, SchemaCache* schemaCache) 
            : NdbDataReader<MetadataLogEntry, MConn>(connections, num_readers, hopsworks, elastic, cache), mSchemaCache(schemaCache) {

}


void SchemabasedMetadataReader::processAddedandDeleted(MConn connection, MetaQ* data_batch, Bulk& bulk) {
        
    Ndb* metaConn = connection.metadataConnection;
    
    const NdbDictionary::Dictionary* metaDatabase = getDatabase(metaConn);
    NdbTransaction* metaTransaction = startNdbTransaction(metaConn);
    
    SchemabasedMq* data_queue = MetadataLogTailer::readSchemaBasedMetadataRows(metaDatabase, 
            metaTransaction, data_batch, bulk.mMetaPKs);

    UIRowMap tuples = readMetadataColumns(metaDatabase, metaTransaction, data_queue);
    
    if(mHopsworksEnalbed){
        //read inodes to datasets from inodes datatbase to enable
        //parent/child relationship
        refreshProjectDatasetINodeCache(connection.inodeConnection, tuples,
                metaDatabase, metaTransaction);
    }
        
    createJSON(tuples, data_queue, bulk);
    
    metaConn->closeTransaction(metaTransaction);
}

UIRowMap SchemabasedMetadataReader::readMetadataColumns(const NdbDictionary::Dictionary* database,
        NdbTransaction* transaction, SchemabasedMq* data_batch) { 
    
    UISet fields_ids;
    UISet tuple_ids;
    
    for (SchemabasedMq::iterator it = data_batch->begin(); it != data_batch->end(); ++it) {
       SchemabasedMetadataEntry entry = *it;
       fields_ids.insert(entry.mFieldId);
       tuple_ids.insert(entry.mTupleId);
    }
    
    mSchemaCache->refresh(database, transaction, fields_ids);
    
    // Read the tuples
    UIRowMap tuples = readTableWithIntPK(database, transaction, META_TUPLE_TO_FILE, 
            tuple_ids, TUPLES_COLS_TO_READ, NUM_TUPLES_COLS, TUPLE_ID_COL);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);
        
    return tuples;
}

void SchemabasedMetadataReader::refreshProjectDatasetINodeCache(SConn inode_connection, UIRowMap tuples, 
        const NdbDictionary::Dictionary* metaDatabase, NdbTransaction* metaTransaction) {

    UISet inodes_ids;
    
    for (UIRowMap::iterator it = tuples.begin(); it != tuples.end(); ++it) {
        int tupleId = it->second[TUPLE_ID_COL]->int32_value();
        if (it->first != tupleId) {
            //TODO: update elastic?!
            LOG_ERROR("Tuple " << it->first << " doesn't exist, got tupleId " 
                    << tupleId << " was expecting " << it->first);
            continue;
        }

        int inodeId = it->second[TUPLE_INODE_ID_COL]->int32_value();
        inodes_ids.insert(inodeId);
    }

    mPDICache->refresh(inode_connection, metaDatabase, metaTransaction, inodes_ids);
}

void SchemabasedMetadataReader::createJSON(UIRowMap tuples, SchemabasedMq* data_batch, Bulk& bulk) {

    vector<ptime> arrivalTimes(data_batch->size());
    stringstream out;
    int i=0;
    for(SchemabasedMq::iterator it=data_batch->begin(); it != data_batch->end(); ++it, i++){
        SchemabasedMetadataEntry entry = *it;
        LOG_TRACE("create JSON for " << entry.to_string());
        arrivalTimes[i] = entry.mEventCreationTime;
        
        boost::optional<Field> _fres = mSchemaCache->getField(entry.mFieldId);
        
        if(!_fres){
            LOG_ERROR(" Field " << entry.mFieldId << " is not in the cache");
            continue;
        }
        
        Field field = *_fres;
        boost::optional<Table> _tres = mSchemaCache->getTable(field.mTableId);
        
        if (!_tres) {
            LOG_ERROR(" Table " << field.mTableId << " is not in the cache");
            continue;
        }

        Table table = *_tres;
        
        boost::optional<string> _ttres = mSchemaCache->getTemplate(table.mTemplateId);

        if (!_ttres) {
            LOG_ERROR(" Template " << table.mTemplateId << " is not in the cache");
            continue;
        }
        
        string templateName = *_ttres;
        
        int inodeId = tuples[entry.mTupleId][TUPLE_INODE_ID_COL]->int32_value();

         // INode Operation
        rapidjson::StringBuffer sbOp;
        rapidjson::Writer<rapidjson::StringBuffer> opWriter(sbOp);

        opWriter.StartObject();

        opWriter.String("update");
        opWriter.StartObject();


        if(mHopsworksEnalbed){
            int datasetId = mPDICache->getDatasetId(inodeId);
            // set project (rounting) and dataset (parent) ids 
            opWriter.String("_parent");
            opWriter.Int(datasetId);

            opWriter.String("_routing");
            opWriter.Int(mPDICache->getProjectId(datasetId));  
            
            if(datasetId == inodeId){
                opWriter.String("_type");
                opWriter.String(mElasticSearch->getDatasetType());
            }
            
        }

        opWriter.String("_id");
        opWriter.Int(inodeId);
        
        opWriter.EndObject();
        opWriter.EndObject();
        
        out << sbOp.GetString() << endl;
        
        rapidjson::StringBuffer sbDoc;
        rapidjson::Writer<rapidjson::StringBuffer> docWriter(sbDoc);

        docWriter.StartObject();
        docWriter.String("doc");
        docWriter.StartObject();

        docWriter.String(XATTR_FIELD_NAME);
        docWriter.StartObject();
        
        docWriter.String(templateName.c_str());
        docWriter.StartObject();
        
        docWriter.String(table.mName.c_str());
        docWriter.StartObject();
        
        docWriter.String(field.mName.c_str());

        switch (field.mType) {
            case BOOL:
            {
                bool boolVal = entry.mMetadata == "true" || entry.mMetadata == "1";
                docWriter.Bool(boolVal);
                break;
            }
            case INT:
            {
                try {
                    int intVal = entry.mOperation == Delete ? DONT_EXIST_INT : boost::lexical_cast<int>(entry.mMetadata);
                    docWriter.Int(intVal);
                } catch (boost::bad_lexical_cast &e) {
                    LOG_ERROR("Error while casting [" << entry.mMetadata << "] to int" << e.what());
                }

                break;
            }
            case DOUBLE:
            {
                try {
                    double doubleVal = entry.mOperation == Delete ? DONT_EXIST_INT : boost::lexical_cast<double>(entry.mMetadata);
                    docWriter.Double(doubleVal);
                } catch (boost::bad_lexical_cast &e) {
                    LOG_ERROR("Error while casting [" << entry.mMetadata << "] to double" << e.what());
                }

                break;
            }
            case TEXT:
            {
                docWriter.String(entry.mOperation == Delete ? DONT_EXIST_STR : entry.mMetadata.c_str());
                break;
            }
        }
        
        docWriter.EndObject();
         
        docWriter.EndObject();
        
        docWriter.EndObject();
        docWriter.EndObject();

        docWriter.String("doc_as_upsert");
        docWriter.Bool(true);

        docWriter.EndObject();

        out << sbDoc.GetString() << endl;
    }
    
    bulk.mArrivalTimes = arrivalTimes;
    bulk.mJSON = out.str();
}

SchemabasedMetadataReader::~SchemabasedMetadataReader() {
    for(int i=0; i< mNumReaders; i++){
        delete mNdbConnections[i].inodeConnection;
        delete mNdbConnections[i].metadataConnection;
    }
}

