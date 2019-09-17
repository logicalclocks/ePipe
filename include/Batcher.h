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

#ifndef BATCHER_H
#define BATCHER_H

#include "Utils.h"
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

class Batcher {
public:
  Batcher(const int time_to_wait, const int batch_size);
  void start();
  void shutdown();
  void waitToFinish();
  virtual ~Batcher();

protected:
  virtual void run() = 0;
  virtual void processBatch() = 0;
  void resetTimer();

  const int mBatchSize;
  bool mTimerProcessing;
  const int mTimeToWait;

private:
  boost::thread mThread;
  bool mStarted;
  bool mFirstTimer;
  bool mScheduleShutdown;
  bool mShutdownTimer;
  boost::thread mTimerThread;

  boost::asio::io_service mIOService;

  void startTimer();
  void timerThread();
  void timerExpired(const boost::system::error_code& e);

};

#endif /* BATCHER_H */

