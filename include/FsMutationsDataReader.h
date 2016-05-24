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
 * File:   FsMutationsDataReader.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef FSMUTATIONSDATAREADER_H
#define FSMUTATIONSDATAREADER_H

#include "FsMutationsTableTailer.h"
#include "NdbDataReader.h"

typedef vector<Row> Rows;

class FsMutationsDataReader : public NdbDataReader<FsMutationRow, MConn>{
public:
    FsMutationsDataReader(MConn* connections, const int num_readers, string elastic_ip,
            const bool hopsworks, const string elastic_index, const string elastic_inode_type,
            ProjectDatasetINodeCache* cache, const int lru_cap);
    virtual ~FsMutationsDataReader();
private:
    Cache<int, string> mUsersCache;
    Cache<int, string> mGroupsCache;
    
    virtual ReadTimes readData(MConn connection, Fmq* data_batch);
    string processAddedandDeleted(MConn connection, Fmq* data_batch, ReadTimes& rt);
    void updateProjectIds(Ndb* metaConnection, Fmq* data_batch);
    
    void readINodes(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, Fmq* data_batch, Rows& inodes);
    void getUsersAndGroups(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, Rows& inodes);
    UIRowMap getUsersFromDB(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet ids);
    UIRowMap getGroupsFromDB(const NdbDictionary::Dictionary* database, NdbTransaction* transaction, UISet ids);
    
    string createJSON(Fmq* pending, Rows& inodes);
};

#endif /* FSMUTATIONSDATAREADER_H */

