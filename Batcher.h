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
 * File:   Batcher.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef BATCHER_H
#define BATCHER_H

#include "common.h"

class Batcher {
public:
    Batcher(const int time_before_issuing_ndb_reqs, const int batch_size);
    void start();
    void waitToFinish();
    virtual ~Batcher();
    
protected:
    virtual void run() = 0;
    virtual void processBatch() = 0;
    
    const int mBatchSize;
    bool mTimerProcessing;
    
private:
    boost::thread mThread;
    bool mStarted;
    
    const int mTimeBeforeIssuingNDBReqs;
    boost::thread mTimerThread;
    
    void startTimer();
    void timerThread();
    void timerExpired();

};

#endif /* BATCHER_H */

