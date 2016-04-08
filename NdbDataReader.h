/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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

class NdbDataReader {
public:
    NdbDataReader(const Ndb** connections, const int num_readers);
    void start();
    void process_batch(Cus_Cus added_deleted_batch);
    virtual ~NdbDataReader();
private:
    const Ndb** mNdbConnections;
    const int mNumReaders;
    
    bool mStarted;
    boost::thread mThread;
    
    ConcurrentQueue<Cus_Cus>* mBatchedQueue;
    
    void run();
};

#endif /* NDBDATAREADER_H */

