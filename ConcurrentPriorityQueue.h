/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
#include <boost/thread/pthread/mutex.hpp>
#include <boost/thread/pthread/condition_variable_fwd.hpp>

template<typename Data, typename DataCompartor>
class ConcurrentPriorityQueue{
public:
    ConcurrentPriorityQueue();
    void push(Data data);
    void wait_and_pop(Data &result);
    bool empty();
    virtual ~ConcurrentPriorityQueue();
private:
    boost::heap::priority_queue<Data,  boost::heap::compare<DataCompartor> > mQueue;
    mutable boost::mutex mLock;
    boost::condition_variable mQueueUpdated;
};


template<typename Data, typename DataCompartor>
ConcurrentPriorityQueue<Data, DataCompartor>::ConcurrentPriorityQueue() {
}

template<typename Data, typename DataCompartor>
void ConcurrentPriorityQueue<Data, DataCompartor>::push(Data data){
    boost::mutex::scoped_lock lock(mLock);
    mQueue.push(data);
    lock.unlock();
    mQueueUpdated.notify_one();
}

template<typename Data, typename DataCompartor>
void ConcurrentPriorityQueue<Data, DataCompartor>::wait_and_pop(Data& result){
    boost::mutex::scoped_lock lock(mLock);
    while(mQueue.empty()){
        mQueueUpdated.wait(lock);
    }
    result = mQueue.top();
    mQueue.pop();
    
}

template<typename Data, typename DataCompartor>
bool ConcurrentPriorityQueue<Data, DataCompartor>::empty(){
    boost::mutex::scoped_lock lock(mLock);
    return mQueue.empty();
}

template<typename Data, typename DataCompartor>
ConcurrentPriorityQueue<Data, DataCompartor>::~ConcurrentPriorityQueue() {
}

#endif /* PRIORITYQUEUE_H */

