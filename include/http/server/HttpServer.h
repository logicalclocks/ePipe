/*
 * Copyright (C) 2019 Logical Clocks AB
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

#ifndef EPIPE_HTTPSERVER_H
#define EPIPE_HTTPSERVER_H

#include "HttpConnection.h"

class HttpServer{
public:
  HttpServer(const std::string addr, const
  MetricsProvider& metricsProvider) : mMetricsProvider
  (metricsProvider){
    std::size_t i = addr.find(":");
    mIP = addr.substr(0,i);
    mPort = static_cast<unsigned short>(std::atoi(addr.substr(i+1,addr.length
    ()).c_str()));
  }

  void run(){
    LOG_INFO("Running server at " << mIP << ":" << mPort);
    auto const address = net::ip::make_address(mIP);
    net::io_context ioc{1};
    tcp::acceptor acceptor{ioc, {address, mPort}};
    tcp::socket socket{ioc};
    start(acceptor, socket, mMetricsProvider);
    ioc.run();
  }

private:
  std::string mIP;
  unsigned short mPort;
  const MetricsProvider& mMetricsProvider;

  void start(tcp::acceptor &acceptor, tcp::socket &socket, const
  MetricsProvider& metrics) {
    acceptor.async_accept(socket,
                          [&](beast::error_code ec) {
                            if (!ec)
                              std::make_shared<http_connection>(
                                  std::move(socket), metrics)->start();
                            start(acceptor, socket, metrics);
                          });
  }
};
#endif //EPIPE_HTTPSERVER_H
