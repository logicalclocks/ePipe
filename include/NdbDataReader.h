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
 * File:   NdbDataReader.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef NDBDATAREADER_H
#define NDBDATAREADER_H

#include "FsMutationsTableTailer.h"
#include "ConcurrentQueue.h"
#include "vector"

class NdbDataReader {
public:
    NdbDataReader(Ndb** connections, const int num_readers);
    void start();
    void process_batch(Cus_Cus added_deleted_batch);
    virtual ~NdbDataReader();
private:
    Ndb** mNdbConnections;
    const int mNumReaders;

    bool mStarted;
    std::vector<boost::thread* > mThreads;
    
    ConcurrentQueue<Cus_Cus>* mBatchedQueue;
    
    void readerThread(int connectionId);
    void readData(Ndb* connection, Cus* added);
};

#endif /* NDBDATAREADER_H */

