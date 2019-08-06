/*
 * Copyright (C) 2018 Logical Clocks AB
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
 * File:   DBWatchTable.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef DBWATCHTABLE_H
#define DBWATCHTABLE_H
#include "DBTable.h"

#define PRIMARY_INDEX "PRIMARY"

typedef std::vector<NdbDictionary::Event::TableEvent> TEventVec;
typedef typename TEventVec::size_type evtvec_size_type;

template<typename TableRow>
class DBWatchTable : public DBTable<TableRow> {
public:
  DBWatchTable(const std::string table);
  const std::string getRecoveryIndex() const;
  evtvec_size_type getNoEvents() const;
  NdbDictionary::Event::TableEvent getEvent(evtvec_size_type index) const;
  void getAllSortedByRecoveryIndex(Ndb* connection);
  boost::tuple<std::vector<Uint64>*, boost::unordered_map<Uint64, std::vector<TableRow>* >* > getAllByGCI(Ndb* connection);
  virtual ~DBWatchTable();
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
const std::string DBWatchTable<TableRow>::getRecoveryIndex() const {
  return mRecoveryIndex;
}

template<typename TableRow>
DBWatchTable<TableRow>::~DBWatchTable() {
}

template<typename TableRow>
void DBWatchTable<TableRow>::getAllSortedByRecoveryIndex(Ndb* connection) {
  this->getAll(connection, mRecoveryIndex);
}

template<typename TableRow>
boost::tuple<std::vector<Uint64>*, boost::unordered_map<Uint64, std::vector<TableRow>* >* >
DBWatchTable<TableRow>::getAllByGCI(Ndb* connection) {
  typedef boost::unordered_map<Uint64, std::vector<TableRow>* > GCIRows;
  typedef typename GCIRows::iterator GCIRowIterator;

  ptime start = Utils::getCurrentTime();

  this->setReadGCI(true);

  GCIRows* rowsByGCI = new GCIRows();
  std::vector<Uint64>* gcis = new std::vector<Uint64>();

  this->getAll(connection);
  while (this->next()) {
    TableRow row = this->currRow();
    Uint64 gci = this->currGCI();

    GCIRowIterator curr = rowsByGCI->find(gci);
    std::vector<TableRow>* currRows;
    if (curr == rowsByGCI->end()) {
      currRows = new std::vector<TableRow>();
      rowsByGCI->emplace(gci, currRows);
      gcis->push_back(gci);
    } else {
      currRows = curr->second;
    }
    currRows->push_back(row);
  }

  this->setReadGCI(false);

  std::sort(gcis->begin(), gcis->end());

  LOG_INFO("ePipe done reading/sorting for " << this->getName() << " recovery in " << Utils::getTimeDiffInMilliseconds(start, Utils::getCurrentTime()) << " msec");

  return boost::make_tuple(gcis, rowsByGCI);
}

#endif /* DBWATCHTABLE_H */

