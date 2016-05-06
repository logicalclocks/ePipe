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

#include "NdbDataReader.h"

class FsMutationsDataReader : public NdbDataReader<Cus_Cus>{
public:
    FsMutationsDataReader(Ndb** connections, const int num_readers, string elastic_ip);
    virtual ~FsMutationsDataReader();
private:
    virtual void readData(Ndb* connection, Cus_Cus data_batch);
    string createJSON(std::vector<FsMutationRow> pending, std::vector<NdbRecAttr*> inodes[], boost::unordered_map<int, NdbRecAttr*> users,
    boost::unordered_map<int, NdbRecAttr*> groups);
};

#endif /* FSMUTATIONSDATAREADER_H */

