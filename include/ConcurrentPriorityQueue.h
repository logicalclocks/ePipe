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
 * File:   ConcurrentPriorityQueue.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef CONCURRENTPRIORITYQUEUE_H
#define CONCURRENTPRIORITYQUEUE_H
#include "common.h"
#include <boost/heap/priority_queue.hpp>

template<typename Data, typename DataCompartor>
class ConcurrentPriorityQueue {
public:
  ConcurrentPriorityQueue();
  void push(Data data);
  void pop(Data& result);
  void pop();
  void wait_and_pop(Data &result);
  bool empty();
  int size();
  virtual ~ConcurrentPriorityQueue();
private:
  boost::heap::priority_queue<Data, boost::heap::compare<DataCompartor> > mQueue;
  mutable boost::mutex mLock;
  boost::condition_variable mQueueUpdated;
};

template<typename Data, typename DataCompartor>
ConcurrentPriorityQueue<Data, DataCompartor>::ConcurrentPriorityQueue() {
}

template<typename Data, typename DataCompartor>
void ConcurrentPriorityQueue<Data, DataCompartor>::push(Data data) {
  boost::mutex::scoped_lock lock(mLock);
  mQueue.push(data);
  lock.unlock();
  mQueueUpdated.notify_one();
}

template<typename Data, typename DataCompartor>
void ConcurrentPriorityQueue<Data, DataCompartor>::wait_and_pop(Data& result) {
  boost::mutex::scoped_lock lock(mLock);
  while (mQueue.empty()) {
    mQueueUpdated.wait(lock);
  }
  result = mQueue.top();
  mQueue.pop();

}

template<typename Data, typename DataCompartor>
void ConcurrentPriorityQueue<Data, DataCompartor>::pop(Data& result) {
  boost::mutex::scoped_lock lock(mLock);
  result = mQueue.top();
  mQueue.pop();
}

template<typename Data, typename DataCompartor>
void ConcurrentPriorityQueue<Data, DataCompartor>::pop() {
  boost::mutex::scoped_lock lock(mLock);
  mQueue.pop();
}

template<typename Data, typename DataCompartor>
bool ConcurrentPriorityQueue<Data, DataCompartor>::empty() {
  boost::mutex::scoped_lock lock(mLock);
  return mQueue.empty();
}

template<typename Data, typename DataCompartor>
int ConcurrentPriorityQueue<Data, DataCompartor>::size() {
  boost::mutex::scoped_lock lock(mLock);
  return mQueue.size();
}


template<typename Data, typename DataCompartor>
ConcurrentPriorityQueue<Data, DataCompartor>::~ConcurrentPriorityQueue() {
}

#endif /* PRIORITYQUEUE_H */

