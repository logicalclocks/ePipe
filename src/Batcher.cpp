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
 * File:   Batcher.cpp
 * Author: Mahmoud Ismail<maism@kth.se>
 * 
 */

#include "Batcher.h"

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

Batcher::Batcher(const int time_before_issuing_ndb_reqs, const int batch_size) 
    : mBatchSize(batch_size), mTimerProcessing(false), mStarted(false), mTimeBeforeIssuingNDBReqs(time_before_issuing_ndb_reqs) {
}


void Batcher::start() {
    if(mStarted){
        LOG_INFO( "Batcher is already started");
        return;
    }
    
    startTimer();
    mThread = boost::thread(&Batcher::run, this);
    mStarted = true;
}

void Batcher::waitToFinish() {
    if(mStarted){
        mThread.join();
    }
}

void Batcher::startTimer() {
   LOG_DEBUG("start timer");
   mTimerThread = boost::thread(&Batcher::timerThread, this);
}

void Batcher::timerThread() {
    while (true) {
        boost::asio::io_service io;
        boost::asio::deadline_timer timer(io, boost::posix_time::milliseconds(mTimeBeforeIssuingNDBReqs));
        timer.async_wait(boost::bind(&Batcher::timerExpired, this));
        io.run();
    }
}

void Batcher::timerExpired() {
    LOG_TRACE("time expired before reaching the batch size");
    mTimerProcessing=true;
    processBatch();
    mTimerProcessing=false;
}

Batcher::~Batcher() {
}

