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

#ifndef DBTABLE_H
#define DBTABLE_H

#include <boost/any.hpp>
#include "boost/optional.hpp"
#include "DBTableBase.h"

typedef NdbRecAttr** Row;
typedef std::vector<Row> Rows;
typedef boost::unordered_map<int, Row> UIRowMap;
typedef boost::any Any;
typedef boost::unordered_map<int, Any> AnyMap;
typedef std::vector<AnyMap> AnyVec;

template<typename TableRow>
class DBTable : public DBTableBase {
public:
  DBTable(const std::string table);
  DBTable(const std::string table, DBTableBase* companionTableBase);

  void getAll(Ndb* connection);
  bool next();
  TableRow currRow();
  Uint64 currEpoch();

  virtual TableRow getRow(NdbRecAttr* values[]) = 0;

  virtual ~DBTable();

private:
  bool mReadEpoch;
  const NdbDictionary::Dictionary* mDatabase;
  const NdbDictionary::Table* mTable;
  const NdbDictionary::Index* mIndex;

  NdbTransaction* mCurrentTransaction;
  NdbOperation* mCurrentOperation;
  NdbRecAttr** mCurrentRow;

  const NdbDictionary::Table* mCompanionTable;

  void close();
  void applyConditionOnOperation(NdbOperation* operation, AnyMap& any);
  void applyConditionOnOperationOnCompanion(NdbOperation* operation, AnyMap& any);
  void applyValueOnOperation(NdbOperation* operation, AnyMap& any);

protected:
  DBTableBase* mCompanionTableBase;

  NdbRecAttr** getColumnValues(NdbOperation* op);
  void start(Ndb* connection);
  void start(Ndb* connection, boost::optional<Int64> partitionId);
  void end();

  TableRow doRead(Ndb* connection, Any any);
  TableRow doRead(Ndb* connection, AnyMap& any);
  std::vector<TableRow> doRead(Ndb* connection, AnyVec& pks);
  boost::unordered_map<int, TableRow> doRead(Ndb* connection, UISet& ids);
  boost::unordered_map<Int64, TableRow> doRead(Ndb* connection, ULSet& ids);
  
  std::vector<TableRow> doRead(Ndb* connection, std::string index, AnyMap& anys);
  std::vector<TableRow> doRead(Ndb* connection, std::string index, AnyMap& anys, boost::optional<Int64> partitionId);
  bool rowsExists(Ndb* connection, std::string index, AnyMap& anys);

  void doDelete(Any any);
  void doDelete(AnyMap& any);
  void doDeleteOnCompanion(AnyMap& any);
  int deleteByIndex(Ndb* connection, std::string index, AnyMap& anys);
  int deleteByIndex(Ndb* connection, std::string index, AnyMap& anys, boost::optional<Int64> partitionId);

  void getAll(Ndb* connection, std::string index);
  void setReadEpoch(bool readEpoch);
  
  int getColumnIdInDB(int colIndex);
  int getColumnIdInDB(const char* colName);
  
  virtual void applyConditionOnGetAll(NdbScanFilter& filter);
  void convert(UISet& ids, AnyVec& resultAny, IVec& resultVec);
  void convert(ULSet& ids, AnyVec& resultAny, LVec& resultVec);
  Int64 getRandomPartitionId();

  void doWrite(Ndb* connection, Any any, AnyMap& values);
};

template<typename TableRow>
DBTable<TableRow>::DBTable(const std::string table)
: DBTableBase(table), mReadEpoch(false), mCompanionTableBase(nullptr) {

}

template<typename TableRow>
DBTable<TableRow>::DBTable(const std::string table, DBTableBase* companionTableBase)
    : DBTableBase(table), mReadEpoch(false), mCompanionTableBase(companionTableBase) {
}

template<typename TableRow>
void DBTable<TableRow>::setReadEpoch(bool readEpoch) {
  mReadEpoch = readEpoch;
  LOG_DEBUG(getName() << " -- ReadEpoch : " << mReadEpoch);
}
template<typename TableRow>
NdbRecAttr** DBTable<TableRow>::getColumnValues(NdbOperation* op) {
  int numCols = mReadEpoch ? getNoColumns() + 1 : getNoColumns();
  NdbRecAttr** values = new NdbRecAttr*[numCols];
  for (strvec_size_type i = 0; i < getNoColumns(); i++) {
    values[i] = getNdbOperationValue(op, getColumn(i).c_str());
  }
  if (mReadEpoch) {
    values[getNoColumns()] = getNdbOperationValue(op,
        NdbDictionary::Column::ROW_GCI64);
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
void DBTable<TableRow>::getAll(Ndb* connection, std::string index) {
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
  start(connection, boost::none);
}

template<typename TableRow>
void DBTable<TableRow>::start(Ndb* connection, boost::optional<Int64> partitionId) {
  mDatabase = getDatabase(connection);
  mTable = getTable(mDatabase);
  if(mCompanionTableBase != nullptr){
    mCompanionTable = getTable(mDatabase, mCompanionTableBase->getName());
  }
  if(partitionId){
    Int64 partId = partitionId.get();
    Ndb::Key_part_ptr distkey[2];
    distkey[0].ptr= (const void*) &partId;
    distkey[0].len= sizeof(partId);
    distkey[1].ptr= NULL;
    distkey[1].len= 0;

    LOG_DEBUG(getName() << " -- Starting Transaction with partitionId " << partId << " of size " << distkey[0].len);
    mCurrentTransaction = startNdbTransaction(connection, mTable, distkey);
    LOG_DEBUG(getName() << " -- Start Transaction with partitionId " << partId);
  }else{
    mCurrentTransaction = startNdbTransaction(connection);
    LOG_DEBUG(getName() << " -- Start Transaction ");
  }
}

template<typename TableRow>
void DBTable<TableRow>::end() {
  try{
    executeTransaction(mCurrentTransaction, NdbTransaction::Commit);
    close();
  }catch(NdbTupleDidNotExist& e){
    close();
    throw e;
  }
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
Uint64 DBTable<TableRow>::currEpoch() {
  Uint64 epoch = 0;
  if (mReadEpoch) {
    epoch = mCurrentRow[getNoColumns()]->u_64_value();
  }
  return epoch;
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
void DBTable<TableRow>::doWrite(Ndb* connection, Any pk, AnyMap& values) {
  AnyMap pkMap;
  pkMap[0] = pk; 
  start(connection);
  LOG_DEBUG(getName() << " -- doWrite ");
  mCurrentOperation = getNdbOperation(mCurrentTransaction, mTable);
  mCurrentOperation->insertTuple();
  applyConditionOnOperation(mCurrentOperation, pkMap);
  applyValueOnOperation(mCurrentOperation, values);
  executeTransaction(mCurrentTransaction, NdbTransaction::Commit);
  close();
}

template<typename TableRow>
std::vector<TableRow> DBTable<TableRow>::doRead(Ndb* connection, std::string index, AnyMap& any){
  return doRead(connection, index, any, boost::none);
}

template<typename TableRow>
std::vector<TableRow> DBTable<TableRow>::doRead(Ndb* connection, std::string index, AnyMap& any, boost::optional<Int64> partitionId){
  start(connection, partitionId);
  LOG_DEBUG(getName() << " -- doRead with index : " << index);
  mIndex = getIndex(mDatabase, index);
  NdbIndexScanOperation* operation = getNdbIndexScanOperation(mCurrentTransaction, mIndex);
  operation->readTuples(NdbOperation::LM_CommittedRead);
  mCurrentOperation = operation;
  applyConditionOnOperation(operation, any);
  mCurrentRow = getColumnValues(mCurrentOperation);
  executeTransaction(mCurrentTransaction, NdbTransaction::Commit);
  std::vector<TableRow> results;
  while (operation->nextResult(true) == 0){
    TableRow row = getRow(mCurrentRow);
    results.push_back(row);
  }
  close();
  return results;
}

template<typename TableRow>
int DBTable<TableRow>::deleteByIndex(Ndb* connection, std::string index, AnyMap& any) {
  return deleteByIndex(connection, index, any, boost::none);
}

template<typename TableRow>
int DBTable<TableRow>::deleteByIndex(Ndb* connection, std::string index, AnyMap& any, boost::optional<Int64> partitionId) {
  start(connection, partitionId);
  LOG_INFO(getName() << " -- deleteByIndex with index : " << index);
  mIndex = getIndex(mDatabase, index);
  NdbIndexScanOperation* operation = getNdbIndexScanOperation(mCurrentTransaction, mIndex);
  operation->readTuples(NdbOperation::LM_Exclusive);
  mCurrentOperation = operation;
  applyConditionOnOperation(operation, any);
  executeTransaction(mCurrentTransaction, NdbTransaction::NoCommit);

  int count = 0;
  int check;
  while((check = operation->nextResult(true)) == 0) // Outer loop for each batch of rows
  {
    do
    {
      operation->deleteCurrentTuple(); // Inner loop for each row within batch
      count++;
    } while((check = operation->nextResult(false)) == 0); 
    mCurrentTransaction->execute(NoCommit); // When no more rows in batch, exeute all defined deletes       
  }

  executeTransaction(mCurrentTransaction, NdbTransaction::Commit);
  close();
  return count;
}

template<typename TableRow>
bool DBTable<TableRow>::rowsExists(Ndb* connection, std::string index,
    AnyMap& any){
  start(connection);
  LOG_DEBUG(getName() << " -- hasResults with index : " << index);
  mIndex = getIndex(mDatabase, index);
  NdbIndexScanOperation* operation = getNdbIndexScanOperation(mCurrentTransaction, mIndex);
  operation->readTuples(NdbOperation::LM_CommittedRead);
  mCurrentOperation = operation;
  applyConditionOnOperation(operation, any);
  mCurrentRow = getColumnValues(mCurrentOperation);
  executeTransaction(mCurrentTransaction, NdbTransaction::Commit);
  bool hasMoreRows = operation->nextResult(true) == 0;
  close();
  return hasMoreRows;
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
void DBTable<TableRow>::doDeleteOnCompanion(AnyMap& any) {
  LOG_DEBUG(getName() << " -- doDelete companion");
  mCurrentOperation = getNdbOperation(mCurrentTransaction, mCompanionTable);
  mCurrentOperation->deleteTuple();
  applyConditionOnOperationOnCompanion(mCurrentOperation, any);
}


template<typename TableRow>
boost::unordered_map<int, TableRow> DBTable<TableRow>::doRead(Ndb* connection, UISet& ids){
  AnyVec anyVec;
  IVec idsVec;
  convert(ids, anyVec, idsVec);
  std::vector<TableRow> rows = doRead(connection, anyVec);
  boost::unordered_map<int, TableRow> results;
  int i=0;
  for(typename std::vector<TableRow>::iterator it = rows.begin(); it!=rows.end(); ++it, i++){
    results[idsVec[i]]=*it;
  }
  return results;
}

template<typename TableRow>
boost::unordered_map<Int64, TableRow> DBTable<TableRow>::doRead(Ndb* connection, ULSet& ids){
  AnyVec anyVec;
  LVec idsVec;
  convert(ids, anyVec, idsVec);
  std::vector<TableRow> rows = doRead(connection, anyVec);
  boost::unordered_map<Int64, TableRow> results;
  int i=0;
  for(typename std::vector<TableRow>::iterator it = rows.begin(); it!=rows.end(); ++it, i++){
    results[idsVec[i]]=*it;
  }
  return results;
}

template<typename TableRow>
std::vector<TableRow> DBTable<TableRow>::doRead(Ndb* connection, AnyVec& pks){
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
  
  std::vector<TableRow> results;
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
  std::stringstream log;
  LOG_DEBUG(getName() << " -- apply condition");
  for (AnyMap::iterator it = any.begin(); it != any.end(); ++it) {
    int i = it->first;
    Any a = it->second;
    std::string colName = getColumn(i);
    if (a.type() == typeid (int)) {
      int pk = boost::any_cast<int>(a);
      log << colName << " = " << pk << std::endl;
      operation->equal(colName.c_str(), pk);
    } else if(a.type() == typeid(Int64)){
      Int64 pk = boost::any_cast<Int64>(a);
      log << colName << " = " << pk << std::endl;
      operation->equal(colName.c_str(), pk);
    } else if(a.type() == typeid(Int8)){
      Int8 pk = boost::any_cast<Int8>(a);
      log << colName << " = " << (int) pk << std::endl;
      operation->equal(colName.c_str(), pk);
    } else if(a.type() == typeid(Int16)){
      Int16 pk = boost::any_cast<Int16>(a);
      log << colName << " = " << pk << std::endl;
      operation->equal(colName.c_str(), pk);
    } else if (a.type() == typeid (std::string)) {
      std::string pk = boost::any_cast<std::string>(a);
      log << colName << " = " << pk << std::endl;
      operation->equal(colName.c_str(), get_ndb_varchar(pk,
              mTable->getColumn(colName.c_str())->getArrayType()).c_str());
    }else{
      LOG_ERROR(getName() << " -- apply where unknown type" << a.type().name());
    }
  }
  LOG_DEBUG(getName() << " -- apply conditions on operation " << std::endl << log.str());
}

template<typename TableRow>
void DBTable<TableRow>::applyValueOnOperation(NdbOperation* operation, AnyMap& any) {
  std::stringstream log;
  LOG_DEBUG(getName() << " -- apply value");
  for (AnyMap::iterator it = any.begin(); it != any.end(); ++it) {
    int i = it->first;
    Any a = it->second;
    std::string colName = getColumn(i);
    if (a.type() == typeid (int)) {
      int value = boost::any_cast<int>(a);
      log << colName << " = " << value << std::endl;
      operation->setValue(colName.c_str(), value);
    } else if(a.type() == typeid(Int64)){
      Int64 value = boost::any_cast<Int64>(a);
      log << colName << " = " << value << std::endl;
      operation->setValue(colName.c_str(), value);
    } else if(a.type() == typeid(Int8)){
      Int8 value = boost::any_cast<Int8>(a);
      log << colName << " = " << (int) value << std::endl;
      operation->setValue(colName.c_str(), value);
    } else if(a.type() == typeid(Int16)){
      Int16 value = boost::any_cast<Int16>(a);
      log << colName << " = " << value << std::endl;
      operation->setValue(colName.c_str(), value);
    } else if (a.type() == typeid (std::string)) {
      std::string value = boost::any_cast<std::string>(a);
      log << colName << " = " << value << std::endl;
      operation->setValue(colName.c_str(), get_ndb_varchar(value,
              mTable->getColumn(colName.c_str())->getArrayType()).c_str());
    }else{
      LOG_ERROR(getName() << " -- apply where unknown type" << a.type().name());
    }
  }
  LOG_DEBUG(getName() << " -- apply values on operation " << std::endl << log.str());
}

template<typename TableRow>
void DBTable<TableRow>::applyConditionOnOperationOnCompanion(NdbOperation* operation, AnyMap& any) {
  std::stringstream log;
  LOG_DEBUG(getName() << " : " << mCompanionTableBase->getName() << " -- apply condition");
  for (AnyMap::iterator it = any.begin(); it != any.end(); ++it) {
    int i = it->first;
    Any a = it->second;
    std::string colName = mCompanionTableBase->getColumn(i);
    if (a.type() == typeid (int)) {
      int pk = boost::any_cast<int>(a);
      log << colName << " = " << pk << std::endl;
      operation->equal(colName.c_str(), pk);
    } else if(a.type() == typeid(Int64)){
      Int64 pk = boost::any_cast<Int64>(a);
      log << colName << " = " << pk << std::endl;
      operation->equal(colName.c_str(), pk);
    } else if(a.type() == typeid(Int8)){
      Int8 pk = boost::any_cast<Int8>(a);
      log << colName << " = " << (int) pk << std::endl;
      operation->equal(colName.c_str(), pk);
    } else if(a.type() == typeid(Int16)){
      Int16 pk = boost::any_cast<Int16>(a);
      log << colName << " = " << pk << std::endl;
      operation->equal(colName.c_str(), pk);
    } else if (a.type() == typeid (std::string)) {
      std::string pk = boost::any_cast<std::string>(a);
      log << colName << " = " << pk << std::endl;
      operation->equal(colName.c_str(), get_ndb_varchar(pk,
          mCompanionTable->getColumn(colName.c_str())->getArrayType()).c_str());
    }else{
      LOG_ERROR(getName() << " : " << mCompanionTableBase->getName() << " -- apply where unknown type" << a.type().name());
    }
  }
  LOG_DEBUG(getName()  << " : " << mCompanionTableBase->getName() << " -- apply conditions on operation " << std::endl << log.str());
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
void DBTable<TableRow>::convert(ULSet& ids, AnyVec& resultAny, LVec& resultVec){
  for(ULSet::iterator it=ids.begin(); it != ids.end(); ++it){
    Int64 id = *it;
    AnyMap a;
    a[0]=id;
    resultAny.push_back(a);
    resultVec.push_back(id);
  }
}

template<typename TableRow>
Int64 DBTable<TableRow>:: getRandomPartitionId(){
  std::srand(std::time(nullptr));
  Int64 partitionId = 1 + std::rand()/((RAND_MAX + 1u)/6);
  return partitionId;
}

template<typename TableRow>
DBTable<TableRow>::~DBTable() {

}
#endif /* TABLE_H */

