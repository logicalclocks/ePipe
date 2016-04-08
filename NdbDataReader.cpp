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
 * File:   NdbDataReader.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "NdbDataReader.h"

NdbDataReader::NdbDataReader(const Ndb** connections, const int num_readers) : mNdbConnections(connections), mNumReaders(num_readers){
    mStarted = false;
    mBatchedQueue = new ConcurrentQueue<Cus_Cus>();
}

void NdbDataReader::start() {
    if (mStarted) {
        return;
    }
    mThread = boost::thread(&NdbDataReader::run, this);
    mStarted = true;
}

void NdbDataReader::run() {
    while(true){
        Cus_Cus curr;
        mBatchedQueue->wait_and_pop(curr);
        
    }
}

void NdbDataReader::process_batch(Cus_Cus added_deleted_batch) {
    mBatchedQueue->push(added_deleted_batch);
}

NdbDataReader::~NdbDataReader() {
    for(int i=0; i< mNumReaders; i++){
        delete mNdbConnections[i];
    }
    delete mBatchedQueue;
}

