/*
 * This file is part of ePipe
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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

#include "Logger.h"
#include <boost/log/common.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/attributes.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sinks/async_frontend.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/exception_handler.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/exceptions.hpp>

// The formatting logic for the severity level
template< typename CharT, typename TraitsT >
inline std::basic_ostream< CharT, TraitsT >& operator<< (
    std::basic_ostream< CharT, TraitsT >& strm, LogSeverityLevel lvl)
{
    static const char* const str[] =
    {
        "trace",
        "debug",
        "info",
        "warn",
        "error",
        "fatal"
    };
    if (static_cast< std::size_t >(lvl) < (sizeof(str) / sizeof(*str)))
        strm << str[lvl];
    else
        strm << static_cast< int >(lvl);
    return strm;
}


namespace logging = boost::log;
namespace attrs = boost::log::attributes;
namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace expr = boost::log::expressions;
namespace keywords = boost::log::keywords;

BOOST_LOG_ATTRIBUTE_KEYWORD(timestamp, "TimeStamp", boost::posix_time::ptime)
BOOST_LOG_ATTRIBUTE_KEYWORD(severity, "Severity", LogSeverityLevel)

BOOST_LOG_INLINE_GLOBAL_LOGGER_DEFAULT(lg, src::severity_logger_mt<LogSeverityLevel>)

struct LoggerExceptionHandler {
  void operator()(const logging::runtime_error &ex) const{
    std::cerr << "boost::log::runtime_error: " << ex.what() << '\n';
  }

  void operator()(const std::exception &ex) const{
    std::cerr << "std::exception: " << ex.what() << '\n';
  }
};

LogSeverityLevel Logger::mLoggerLevel = LogSeverityLevel::trace;

void Logger::initLogging(const std::string logPrefix, const std::string logDir, int rotationSize, int maxFiles, LogSeverityLevel logLevel){
  mLoggerLevel = logLevel;

  std::string _logDir = logDir;
  if(_logDir.at(logDir.length() - 1) != '/'){
    _logDir += "/";
  }
  std::string _fileName = _logDir + logPrefix + "_%Y_%m_%d__%H_%M_%S_%3N.log";
  typedef sinks::asynchronous_sink<sinks::text_file_backend> file_sink;
  boost::shared_ptr<file_sink> sink(new file_sink(
      keywords::file_name = _fileName,
      keywords::rotation_size = rotationSize,
      keywords::auto_flush = true));

  sink->locked_backend()->set_file_collector(sinks::file::make_collector(
      keywords::target = _logDir,
      keywords::max_files = maxFiles
  ));

  // Upon restart, scan the target directory for files matching the file_name pattern
  sink->locked_backend()->scan_for_files();
  sink->set_formatter(
      expr::format("%1%:[%2%]- %3%") 
      % timestamp
      % severity
      % expr::smessage
  );

  sink->set_filter(severity >= logLevel);

  logging::core::get()->add_sink(sink);

  logging::core::get()->add_global_attribute("TimeStamp", attrs::local_clock());
  
  logging::core::get()->set_exception_handler(
    logging::make_exception_handler<logging::runtime_error, std::exception>(LoggerExceptionHandler{})
    );
}

void Logger::trace(const char* msg) {
  log(LogSeverityLevel::trace, msg);
}

void Logger::debug(const char* msg) {
  log(LogSeverityLevel::debug, msg);
}

void Logger::info(const char* msg) {
  log(LogSeverityLevel::info, msg);
}

void Logger::warn(const char* msg) {
  log(LogSeverityLevel::warn, msg);
}

void Logger::error(const char* msg) {
  log(LogSeverityLevel::error, msg);
}

void Logger::fatal(const char* msg) {
  log(LogSeverityLevel::fatal, msg);
  exit(EXIT_FAILURE);
}

bool Logger::isTrace() {
  return mLoggerLevel == LogSeverityLevel::trace;
}

void Logger::log(LogSeverityLevel level, const char* msg) {
  BOOST_LOG_SEV(lg::get(), level) << msg;
}

