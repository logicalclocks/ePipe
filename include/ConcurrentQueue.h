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
 * File:   ConcurrentQueue.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef CONCURRENTQUEUE_H
#define CONCURRENTQUEUE_H
#include "common.h"
#include "queue"

template<typename Data>
class ConcurrentQueue{
public:
    ConcurrentQueue();
    void push(Data data);
    void wait_and_pop(Data &result);
    bool empty();
    virtual ~ConcurrentQueue();
private:
    std::queue<Data> mQueue;
    mutable boost::mutex mLock;
    boost::condition_variable mQueueUpdated;
};


template<typename Data>
ConcurrentQueue<Data>::ConcurrentQueue() {
}

template<typename Data>
void ConcurrentQueue<Data>::push(Data data){
    boost::mutex::scoped_lock lock(mLock);
    mQueue.push(data);
    lock.unlock();
    mQueueUpdated.notify_one();
}

template<typename Data>
void ConcurrentQueue<Data>::wait_and_pop(Data& result){
    boost::mutex::scoped_lock lock(mLock);
    while(mQueue.empty()){
        mQueueUpdated.wait(lock);
    }
    result = mQueue.front();
    mQueue.pop();
    
}

template<typename Data>
bool ConcurrentQueue<Data>::empty(){
    boost::mutex::scoped_lock lock(mLock);
    return mQueue.empty();
}

template<typename Data>
ConcurrentQueue<Data>::~ConcurrentQueue() {
}


#endif /* CONCURRENTQUEUE_H */

