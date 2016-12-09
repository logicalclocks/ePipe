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
 * File:   SchemaCache.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "SchemaCache.h"
#include "Tables.h"

using namespace Utils::NdbC;

SchemaCache::SchemaCache(const int lru_cap) : mFieldsCache(lru_cap, "Field"), 
        mTablesCache(lru_cap, "Table"), mTemplatesCache(lru_cap, "Template"){
    
}

bool SchemaCache::containsField(int fieldId) {
    return mFieldsCache.contains(fieldId);
}

boost::optional<Field> SchemaCache::getField(int fieldId) {
    return mFieldsCache.get(fieldId);
}

boost::optional<Table> SchemaCache::getTable(int tableId) {
    return mTablesCache.get(tableId);
}

boost::optional<string> SchemaCache::getTemplate(int templateId) {
    return mTemplatesCache.get(templateId);
}

void SchemaCache::refresh(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet fields_ids) {
    UISet tables_to_read = readFields(database, transaction, fields_ids);
    UISet templates_to_read = readTables(database, transaction, tables_to_read);
    readTemplates(database, transaction, templates_to_read);
}

void SchemaCache::refresTemplate(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, int templateId) {
    UISet templates_to_read;
    templates_to_read.insert(templateId);
    readTemplates(database, transaction, templates_to_read);
}

UISet SchemaCache::readFields(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet fields_ids) {
    
    UISet tables_to_read;
    
    if(fields_ids.empty())
        return tables_to_read;
    
    UIRowMap fields = readTableWithIntPK(database, transaction, META_FIELDS, 
            fields_ids, FIELDS_COLS_TO_READ, NUM_FIELDS_COLS, FIELD_ID_COL);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);
    
    for(UIRowMap::iterator it=fields.begin(); it != fields.end(); ++it){
        int fieldId = it->second[FIELD_ID_COL]->int32_value();
        if(it->first != fieldId){
            // TODO: update elastic?!            
            LOG_ERROR("Field " << it->first << " doesn't exist, got fieldId " 
                    << fieldId << " was expecting " << it->first);
            continue;
        }
        
        Field field;
        field.mFieldId = it->second[FIELD_ID_COL]->int32_value();
        field.mName = get_string(it->second[FIELD_NAME_COL]);
        field.mSearchable = it->second[FIELD_SEARCHABLE_COL]->int8_value() == 1;
        field.mTableId = it->second[FIELD_TABLE_ID_COL]->int32_value();
        field.mType = (FieldType) it->second[FIELD_TYPE_COL]->short_value();
        mFieldsCache.put(it->first, field);
        
        if(field.mSearchable && !mTablesCache.contains(field.mTableId)){
            tables_to_read.insert(field.mTableId);
        }
    }
    
    return tables_to_read;
}

UISet SchemaCache::readTables(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet tables_ids) {
    
    UISet templates_to_read;
    
    if(tables_ids.empty())
        return templates_to_read;
        
    UIRowMap tables = readTableWithIntPK(database, transaction, META_TABLES, 
            tables_ids,TABLES_COLS_TO_READ, NUM_TABLES_COLS, TABLE_ID_COL);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);

    for(UIRowMap::iterator it=tables.begin(); it != tables.end(); ++it){
        int tableId = it->second[TABLE_ID_COL]->int32_value();
        if(it->first != tableId){
            //TODO: update elastic?!
            LOG_ERROR("Table " << it->first << " doesn't exist, got tableId " 
                    << tableId << " was expecting " << it->first);
            continue;
        }
        
        Table table;
        table.mName = get_string(it->second[TABLE_NAME_COL]);
        table.mTemplateId = it->second[TABLE_TEMPLATE_ID_COL]->int32_value();
        
        mTablesCache.put(it->first, table);
        
        if(!mTemplatesCache.contains(table.mTemplateId)){
            templates_to_read.insert(table.mTemplateId);
        }
    }
    
    return templates_to_read;
}

void SchemaCache::readTemplates(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet templates_ids) {

    if(templates_ids.empty())
        return;
    
    UIRowMap templates = readTableWithIntPK(database, transaction, META_TEMPLATES, 
            templates_ids, TEMPLATE_COLS_TO_READ, NUM_TEMPLATE_COLS, TEMPLATE_ID_COL);
    
    executeTransaction(transaction, NdbTransaction::NoCommit);
    
     for(UIRowMap::iterator it=templates.begin(); it != templates.end(); ++it){
        int templateId = it->second[TEMPLATE_ID_COL]->int32_value(); 
        if(it->first != templateId){
            //TODO: update elastic?!
            LOG_ERROR("Template " << it->first << " doesn't exist, got templateId " 
                    << templateId << " was expecting " << it->first);
            continue;
        }
        
        string templateName = get_string(it->second[TEMPLATE_NAME_COL]);
        
        mTemplatesCache.put(it->first, templateName);
    }
}

SchemaCache::~SchemaCache() {
}

