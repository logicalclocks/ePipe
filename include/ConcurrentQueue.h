/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
 *
 * ePipe is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * ePipe is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef CONCURRENTQUEUE_H
#define CONCURRENTQUEUE_H
#include "common.h"
#include "queue"

template<typename Data>
class ConcurrentQueue {
public:
  ConcurrentQueue();
  void push(Data data);
  void wait_and_pop(Data &result);
  bool empty();
  unsigned int size();
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
void ConcurrentQueue<Data>::push(Data data) {
  boost::mutex::scoped_lock lock(mLock);
  mQueue.push(data);
  lock.unlock();
  mQueueUpdated.notify_one();
}

template<typename Data>
void ConcurrentQueue<Data>::wait_and_pop(Data& result) {
  boost::mutex::scoped_lock lock(mLock);
  while (mQueue.empty()) {
    mQueueUpdated.wait(lock);
  }
  result = mQueue.front();
  mQueue.pop();

}

template<typename Data>
bool ConcurrentQueue<Data>::empty() {
  boost::mutex::scoped_lock lock(mLock);
  return mQueue.empty();
}

template<typename Data>
unsigned int ConcurrentQueue<Data>::size() {
  boost::mutex::scoped_lock lock(mLock);
  return mQueue.size();
}

template<typename Data>
ConcurrentQueue<Data>::~ConcurrentQueue() {
}


#endif /* CONCURRENTQUEUE_H */

