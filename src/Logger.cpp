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

#include "Logger.h"

int Logger::mLoggerLevel = LOG_LEVEL_TRACE;

void Logger::setLoggerLevel(int level) {
  mLoggerLevel = level;
}

void Logger::trace(const char* msg) {
  if (mLoggerLevel <= LOG_LEVEL_TRACE) {
    log("trace", msg);
  }
}

void Logger::debug(const char* msg) {
  if (mLoggerLevel <= LOG_LEVEL_DEBUG) {
    log("debug", msg);
  }
}

void Logger::info(const char* msg) {
  if (mLoggerLevel <= LOG_LEVEL_INFO) {
    log("info", msg);
  }
}

void Logger::warn(const char* msg) {
  if (mLoggerLevel <= LOG_LEVEL_WARN) {
    log("warn", msg);
  }
}

void Logger::error(const char* msg) {
  if (mLoggerLevel <= LOG_LEVEL_ERROR) {
    log("error", msg);
  }
}

void Logger::fatal(const char* msg) {
  if (mLoggerLevel <= LOG_LEVEL_FATAL) {
    log("fatal", msg);
  }
  exit(EXIT_FAILURE);
}

bool Logger::isTrace() {
  return mLoggerLevel == LOG_LEVEL_TRACE;
}

void Logger::log(const char* level, const char* msg) {
  std::cout << getTimestamp() << " <" << level << "> " << msg << std::endl;
}

std::string Logger::getTimestamp() {
  return boost::posix_time::to_iso_extended_string(boost::posix_time::second_clock::local_time());
}