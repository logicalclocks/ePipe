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

#ifndef DBWATCHTABLE_H
#define DBWATCHTABLE_H
#include "DBTable.h"

#define PRIMARY_INDEX "PRIMARY"

typedef std::vector<NdbDictionary::Event::TableEvent> TEventVec;
typedef typename TEventVec::size_type evtvec_size_type;

template<typename TableRow>
struct EpochsRowsMap{
  std::vector<Uint64>* mEpochs;
  boost::unordered_map<Uint64,std::queue<TableRow>* >* mRowsByEpoch;
};

enum LogType{
  FSLOG,
  METALOG,
  PROVAPPLOG,
  PROVFILELOG,
  HOPSWORKSLOG
};

struct LogHandler{
  virtual void removeLog(Ndb* connection) const= 0;
  virtual LogType getType() const = 0;
  virtual std::string getDescription() const = 0;
};

template<typename TableRow>
class DBWatchTable : public DBTable<TableRow> {
public:
  DBWatchTable(const std::string table);
  DBWatchTable(const std::string table, DBTableBase* companionTable);
  evtvec_size_type getNoEvents() const;
  NdbDictionary::Event::TableEvent getEvent(evtvec_size_type index) const;
  EpochsRowsMap<TableRow> getAllForRecovery(Ndb* connection);
  virtual ~DBWatchTable();
  virtual std::string getPKStr(TableRow row);
  virtual LogHandler* getLogRemovalHandler(TableRow row);

private:
  TEventVec mWatchEvents;
  std::string mRecoveryIndex;

protected:
  void addWatchEvent(NdbDictionary::Event::TableEvent event);
  void addRecoveryIndex(const std::string recovery);

};

template<typename TableRow>
DBWatchTable<TableRow>::DBWatchTable(const std::string table) : DBTable<TableRow>(table) {
}

template<typename TableRow>
DBWatchTable<TableRow>::DBWatchTable(const std::string table, DBTableBase* companionTable) :
DBTable<TableRow>(table, companionTable) {
}

template<typename TableRow>
void DBWatchTable<TableRow>::addWatchEvent(NdbDictionary::Event::TableEvent event) {
  mWatchEvents.push_back(event);
}

template<typename TableRow>
evtvec_size_type DBWatchTable<TableRow>::getNoEvents() const {
  return mWatchEvents.size();
}

template<typename TableRow>
NdbDictionary::Event::TableEvent DBWatchTable<TableRow>::getEvent(evtvec_size_type index) const {
  if (index < mWatchEvents.size()) {
    return mWatchEvents[index];
  }
  return NdbDictionary::Event::TE_INSERT;
}

template<typename TableRow>
void DBWatchTable<TableRow>::addRecoveryIndex(const std::string recovery) {
  mRecoveryIndex = recovery;
}

template<typename TableRow>
DBWatchTable<TableRow>::~DBWatchTable() {
}

template<typename TableRow>
EpochsRowsMap<TableRow> DBWatchTable<TableRow>::getAllForRecovery(Ndb* connection) {
  typedef boost::unordered_map<Uint64, std::queue<TableRow>* > EpochRows;
  typedef typename EpochRows::iterator EpochRowIterator;

  ptime start = Utils::getCurrentTime();

  this->setReadEpoch(true);

  EpochRows* rowsByEpoch = new EpochRows();
  std::vector<Uint64>* epochs = new std::vector<Uint64>();

  if(mRecoveryIndex != ""){
    LOG_DEBUG("Read all for " << this->getName() << " recovery sorted by " <<
    mRecoveryIndex);
    this->getAll(connection, mRecoveryIndex);
  }else{
    LOG_DEBUG("Read all for " << this->getName() << " recovery");
    this->getAll(connection);
  }

  while (this->next()) {
    TableRow row = this->currRow();
    Uint64 epoch = this->currEpoch();

    EpochRowIterator curr = rowsByEpoch->find(epoch);
    std::queue<TableRow>* currRows;
    if (curr == rowsByEpoch->end()) {
      currRows = new std::queue<TableRow>();
      rowsByEpoch->emplace(epoch, currRows);
      epochs->push_back(epoch);
    } else {
      currRows = curr->second;
    }
    currRows->push(row);
  }

  this->setReadEpoch(false);

  std::sort(epochs->begin(), epochs->end());

  LOG_DEBUG("ePipe done reading/sorting for " << this->getName()
  << " recovery in " << Utils::getTimeDiffInMilliseconds(start, Utils::getCurrentTime()) << " msec");

  EpochsRowsMap<TableRow> map {epochs, rowsByEpoch};
  return map;
}

template<typename TableRow>
std::string DBWatchTable<TableRow>::getPKStr(TableRow row) {
  return "";
}

template<typename TableRow>
LogHandler* DBWatchTable<TableRow>::getLogRemovalHandler(TableRow row) {
  return nullptr;
}
#endif /* DBWATCHTABLE_H */

