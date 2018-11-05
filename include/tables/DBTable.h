/*
 * Copyright (C) 2018 Hops.io
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
 * File:   DBTable.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef DBTABLE_H
#define DBTABLE_H

#include <boost/any.hpp>
#include "DBTableBase.h"

typedef NdbRecAttr** Row;
typedef vector<Row> Rows;
typedef boost::unordered_map<int, Row> UIRowMap;
typedef boost::any Any;
typedef boost::unordered_map<int, Any> AnyMap;
typedef vector<AnyMap> AnyVec;

template<typename TableRow>
class DBTable : public DBTableBase {
public:
  DBTable(const string table);
  DBTable(const string table, bool readGCI);

  void getAll(Ndb* connection);
  bool next();
  TableRow currRow();
  Uint64 currGCI();

  virtual TableRow getRow(NdbRecAttr* values[]) = 0;

  virtual ~DBTable();

private:
  bool mReadGCI;
  const NdbDictionary::Dictionary* mDatabase;
  const NdbDictionary::Table* mTable;
  const NdbDictionary::Index* mIndex;

  NdbTransaction* mCurrentTransaction;
  NdbOperation* mCurrentOperation;
  NdbRecAttr** mCurrentRow;

  void close();
  void applyConditionOnOperation(NdbOperation* operation, AnyMap& any);
  
protected:
  NdbRecAttr** getColumnValues(NdbOperation* op);
  void start(Ndb* connection);
  void end();

  TableRow doRead(Ndb* connection, Any any);
  TableRow doRead(Ndb* connection, AnyMap& any);
  vector<TableRow> doRead(Ndb* connection, AnyVec& pks);
  boost::unordered_map<int, TableRow> doRead(Ndb* connection, UISet& ids);
  vector<TableRow> doRead(Ndb* connection, string index, AnyMap& anys);
  
  void doDelete(Any any);
  void doDelete(AnyMap& any);

  void getAll(Ndb* connection, string index);
  void setReadGCI(bool readGCI);
  
  int getColumnIdInDB(int colIndex);
  int getColumnIdInDB(const char* colName);
  
  virtual void applyConditionOnGetAll(NdbScanFilter& filter);
  void convert(UISet& ids, AnyVec& resultAny, IVec& resultVec);
};

template<typename TableRow>
DBTable<TableRow>::DBTable(const string table)
: DBTableBase(table), mReadGCI(false) {

}

template<typename TableRow>
void DBTable<TableRow>::setReadGCI(bool readGCI) {
  mReadGCI = readGCI;
  LOG_DEBUG(getName() << " -- ReadGCI : " << mReadGCI);
}

template<typename TableRow>
NdbRecAttr** DBTable<TableRow>::getColumnValues(NdbOperation* op) {
  int numCols = mReadGCI ? getNoColumns() + 1 : getNoColumns();
  NdbRecAttr** values = new NdbRecAttr*[numCols];
  for (unsigned int i = 0; i < getNoColumns(); i++) {
    values[i] = getNdbOperationValue(op, getColumn(i).c_str());
  }
  if (mReadGCI) {
    values[getNoColumns()] = getNdbOperationValue(op, NdbDictionary::Column::ROW_GCI);
  }
  return values;
}

template<typename TableRow>
void DBTable<TableRow>::getAll(Ndb* connection) {
  start(connection);
  LOG_DEBUG(getName() << " -- GetAll");
  NdbScanOperation* operation = getNdbScanOperation(mCurrentTransaction, mTable);
  operation->readTuples(NdbOperation::LM_CommittedRead);
  mCurrentOperation = operation;
  NdbScanFilter filter(mCurrentOperation);
  applyConditionOnGetAll(filter);
  mCurrentRow = getColumnValues(mCurrentOperation);
  executeTransaction(mCurrentTransaction, NdbTransaction::Commit);
}

template<typename TableRow>
void DBTable<TableRow>::getAll(Ndb* connection, string index) {
  start(connection);
  LOG_DEBUG(getName() << " -- GetAll with index : " << index);
  mIndex = getIndex(mDatabase, index);
  NdbIndexScanOperation* operation = getNdbIndexScanOperation(mCurrentTransaction, mIndex);
  operation->readTuples(NdbOperation::LM_CommittedRead, NdbScanOperation::SF_OrderBy);
  mCurrentOperation = operation;
  mCurrentRow = getColumnValues(mCurrentOperation);
  executeTransaction(mCurrentTransaction, NdbTransaction::Commit);
}

template<typename TableRow>
void DBTable<TableRow>::start(Ndb* connection) {
  mDatabase = getDatabase(connection);
  mTable = getTable(mDatabase);
  mCurrentTransaction = startNdbTransaction(connection);
  LOG_DEBUG(getName() << " -- Start Transaction");
}

template<typename TableRow>
void DBTable<TableRow>::end() {
  executeTransaction(mCurrentTransaction, NdbTransaction::Commit);
  close();
}

template<typename TableRow>
void DBTable<TableRow>::close() {
  mCurrentTransaction->close();
  mCurrentOperation = NULL;
  mCurrentTransaction = NULL;
  LOG_DEBUG(getName() << " -- Close Transaction");
}

template<typename TableRow>
bool DBTable<TableRow>::next() {
  if (mCurrentOperation != NULL) {
    if (mCurrentOperation->getType() != NdbOperation::PrimaryKeyAccess) {
      NdbScanOperation* operation = dynamic_cast<NdbScanOperation*> (mCurrentOperation);
      bool hasNext = operation->nextResult(true) == 0;
      if (!hasNext) {
        operation->close();
        close();
      }
      return hasNext;
    } else {
      LOG_ERROR("Next should only be called on scan or index operations");
    }
  } else {
    LOG_INFO("Operation is null");
  }
  return false;
}

template<typename TableRow>
TableRow DBTable<TableRow>::currRow() {
  return getRow(mCurrentRow);
}

template<typename TableRow>
Uint64 DBTable<TableRow>::currGCI() {
  Uint64 gci = 0;
  if (mReadGCI) {
    gci = mCurrentRow[getNoColumns()]->u_64_value();
  }
  return gci;
}

template<typename TableRow>
TableRow DBTable<TableRow>::doRead(Ndb* connection, Any any){
  AnyMap a;
  a[0] = any; 
  return doRead(connection, a);
}

template<typename TableRow>
TableRow DBTable<TableRow>::doRead(Ndb* connection, AnyMap& any) {
  start(connection);
  LOG_DEBUG(getName() << " -- doRead ");
  mCurrentOperation = getNdbOperation(mCurrentTransaction, mTable);
  mCurrentOperation->readTuple(NdbOperation::LM_CommittedRead);
  applyConditionOnOperation(mCurrentOperation, any);
  mCurrentRow = getColumnValues(mCurrentOperation);
  executeTransaction(mCurrentTransaction, NdbTransaction::Commit);
  TableRow row = getRow(mCurrentRow);
  close();
  return row;
}

template<typename TableRow>
vector<TableRow> DBTable<TableRow>::doRead(Ndb* connection, string index, AnyMap& any){
  start(connection);
  LOG_DEBUG(getName() << " -- doRead with index : " << index);
  mIndex = getIndex(mDatabase, index);
  NdbIndexScanOperation* operation = getNdbIndexScanOperation(mCurrentTransaction, mIndex);
  operation->readTuples(NdbOperation::LM_CommittedRead);
  mCurrentOperation = operation;
  applyConditionOnOperation(operation, any);
  mCurrentRow = getColumnValues(mCurrentOperation);
  executeTransaction(mCurrentTransaction, NdbTransaction::Commit);
  vector<TableRow> results;
  while (operation->nextResult(true) == 0){
    TableRow row = getRow(mCurrentRow);
    results.push_back(row);
  }
  close();
  return results;
}

template<typename TableRow>
void DBTable<TableRow>::doDelete(Any any) {
  AnyMap a;
  a[0] = any;
  doDelete(a);
}

template<typename TableRow>
void DBTable<TableRow>::doDelete(AnyMap& any) {
  LOG_DEBUG(getName() << " -- doDelete ");
  mCurrentOperation = getNdbOperation(mCurrentTransaction, mTable);
  mCurrentOperation->deleteTuple();
  applyConditionOnOperation(mCurrentOperation, any);
}

template<typename TableRow>
boost::unordered_map<int, TableRow> DBTable<TableRow>::doRead(Ndb* connection, UISet& ids){
  AnyVec anyVec;
  IVec idsVec;
  convert(ids, anyVec, idsVec);
  vector<TableRow> rows = doRead(connection, anyVec);
  boost::unordered_map<int, TableRow> results;
  int i=0;
  for(typename vector<TableRow>::iterator it = rows.begin(); it!=rows.end(); ++it, i++){
    results[idsVec[i]]=*it;
  }
  return results;
}

template<typename TableRow>
vector<TableRow> DBTable<TableRow>::doRead(Ndb* connection, AnyVec& pks){
  start(connection);
  LOG_DEBUG(getName() << " -- doRead : " << pks.size() << " rows");
  Rows rows;
  for(AnyVec::iterator it=pks.begin(); it != pks.end(); ++it){
    AnyMap pk = *it;
    NdbOperation* op = getNdbOperation(mCurrentTransaction, mTable);
    op->readTuple(NdbOperation::LM_CommittedRead);
    applyConditionOnOperation(op, pk);
    rows.push_back(getColumnValues(op));
  }
  executeTransaction(mCurrentTransaction, NdbTransaction::Commit);
  
  vector<TableRow> results;
  for(Rows::iterator it=rows.begin(); it != rows.end(); ++it){
    results.push_back(getRow(*it));
  }
  close();
  return results;
}

template<typename TableRow>
int DBTable<TableRow>::getColumnIdInDB(int colIndex) {
  return getColumnIdInDB(getColumn(colIndex).c_str());
}

template<typename TableRow>
int DBTable<TableRow>::getColumnIdInDB(const char* colName) {
  if(mTable != NULL){
    int id = mTable->getColumn(colName)->getColumnNo();
    LOG_DEBUG(getName() << " -- Got id [" << id << "] for Column [" << colName << "]");
    return id;
  }
  return DONT_EXIST_INT();
}

template<typename TableRow>
void DBTable<TableRow>::applyConditionOnGetAll(NdbScanFilter& filter) {
//Do nothing, override to add conditions
}

template<typename TableRow>
void DBTable<TableRow>::applyConditionOnOperation(NdbOperation* operation, AnyMap& any) {
  stringstream log;
  for (AnyMap::iterator it = any.begin(); it != any.end(); ++it) {
    int i = it->first;
    Any a = it->second;
    string colName = getColumn(i);
    if (a.type() == typeid (int)) {
      int pk = boost::any_cast<int>(a);
      log << colName << " = " << pk << endl;
      operation->equal(colName.c_str(), pk);
    } else if (a.type() == typeid (string)) {
      string pk = boost::any_cast<string>(a);
      log << colName << " = " << pk << endl;
      operation->equal(colName.c_str(), get_ndb_varchar(pk,
              mTable->getColumn(colName.c_str())->getArrayType()).c_str());
    }else{
      LOG_ERROR(getName() << " -- apply where unkown type" << a.type().name());
    }
  }
  LOG_DEBUG(getName() << " apply conditions on operation " << endl << log.str());
}

template<typename TableRow>
void DBTable<TableRow>::convert(UISet& ids, AnyVec& resultAny, IVec& resultVec){
  for(UISet::iterator it=ids.begin(); it != ids.end(); ++it){
    int id = *it;
    AnyMap a;
    a[0]=id;
    resultAny.push_back(a);
    resultVec.push_back(id);
  }
}

template<typename TableRow>
DBTable<TableRow>::~DBTable() {

}
#endif /* TABLE_H */
