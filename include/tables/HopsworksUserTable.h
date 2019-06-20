/*
 * Copyright (C) 2019 Hops.io
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
 * File:   HopsworksUserTable.h
 * Author: Mahmoud Ismail <maism@kth.se>
 *
 */

#ifndef EPIPE_HOPSWORKSUSERTABLE_H
#define EPIPE_HOPSWORKSUSERTABLE_H

#include "DBTable.h"

struct HopsworksUserRow {
  string mUserName;
  string mEmail;
  string mFirstName;
  string mLastName;

  string getUser(){
    stringstream out;
    out << mFirstName << " " << mLastName;
    return out.str();
  }
};


class HopsworksUserTable : public DBTable<HopsworksUserRow> {
public:

  HopsworksUserTable() : DBTable("users") {
    addColumn("username");
    addColumn("email");
    addColumn("fname");
    addColumn("lname");
  }

  HopsworksUserRow getByUserName(Ndb* connection, string username) {
    AnyMap keys;
    keys[0] = username;
    vector<HopsworksUserRow> results = doRead(connection, "username", keys);
    if(results.size() != 1){
      LOG_ERROR("Unique index violation in users for username " << username);
    }
    return results[0];
  }

  HopsworksUserRow getByEmail(Ndb* connection, string email) {
    AnyMap keys;
    keys[1] = email;
    vector<HopsworksUserRow> results = doRead(connection, "email", keys);
    if(results.size() != 1){
      LOG_ERROR("Unique index violation in users for email " << email);
    }
    return results[0];
  }

  HopsworksUserRow getRow(NdbRecAttr* values[]) {
    HopsworksUserRow row;
    row.mUserName = get_string(values[0]);
    row.mEmail = get_string(values[1]);
    row.mFirstName = get_string(values[2]);
    row.mLastName = get_string(values[3]);
    return row;
  }
};

#endif //EPIPE_HOPSWORKSUSERTABLE_H
