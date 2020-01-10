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
//------------------------------------------------------------------------------
//
// Based on the HTTP server small example provided by Boost:Beast
//
//------------------------------------------------------------------------------

#ifndef EPIPE_HTTPCONNECTION_H
#define EPIPE_HTTPCONNECTION_H

#include "common.h"
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio.hpp>
#include <chrono>
#include <ctime>
#include <memory>
#include "MetricsProvider.h"

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

class http_connection : public std::enable_shared_from_this<http_connection> {
public:
  http_connection(tcp::socket socket, MetricsProvider& metrics)
      : socket_(std::move(socket)), metricsProvider_(metrics) {
  }

  // Initiate the asynchronous operations associated with the connection.
  void start() {
    read_request();
    check_deadline();
  }

private:
  // The socket for the currently connected client.
  tcp::socket socket_;

  // Metrics
  MetricsProvider& metricsProvider_;

  // The buffer for performing reads.
  beast::flat_buffer buffer_{8192};

  // The request message.
  http::request<http::dynamic_body> request_;

  // The response message.
  http::response<http::dynamic_body> response_;

  // The timer for putting a deadline on connection processing.
  net::basic_waitable_timer<std::chrono::steady_clock> deadline_{
      socket_.get_executor(), std::chrono::seconds(60)};

  // Asynchronously receive a complete request message.
  void read_request() {
    auto self = shared_from_this();

    http::async_read(
        socket_,
        buffer_,
        request_,
        [self](beast::error_code ec,
               std::size_t bytes_transferred) {
          boost::ignore_unused(bytes_transferred);
          if (!ec)
            self->process_request();
        });
  }

  // Determine what needs to be done with the request message.
  void process_request() {
    response_.version(request_.version());
    response_.keep_alive(false);

    switch (request_.method()) {
      case http::verb::get:
        response_.result(http::status::ok);
        response_.set(http::field::server, "Beast");
        create_response();
        break;

      default:
        // We return responses indicating an error if
        // we do not recognize the request method.
        response_.result(http::status::bad_request);
        response_.set(http::field::content_type, "text/plain");
        beast::ostream(response_.body())
            << "Invalid request-method '"
            << std::string(request_.method_string())
            << "'";
        break;
    }

    write_response();
  }

  // Construct a response message based on the program state.
  void create_response() {
    if (request_.target() == "/metrics") {
      response_.set(http::field::content_type, "text/plain");
      beast::ostream(response_.body())
          << metricsProvider_.getMetrics();
    }  else {
      response_.result(http::status::not_found);
      response_.set(http::field::content_type, "text/plain");
      beast::ostream(response_.body()) << "Invalid target\r\n";
    }
  }

  // Asynchronously transmit the response message.
  void write_response() {
    auto self = shared_from_this();

    response_.set(http::field::content_length, response_.body().size());

    http::async_write(
        socket_,
        response_,
        [self](beast::error_code ec, std::size_t) {
          self->socket_.shutdown(tcp::socket::shutdown_send, ec);
          self->deadline_.cancel();
        });
  }

  // Check whether we have spent enough time on this connection.
  void check_deadline() {
    auto self = shared_from_this();

    deadline_.async_wait(
        [self](beast::error_code ec) {
          if (!ec) {
            // Close socket to cancel any outstanding operation.
            self->socket_.close(ec);
          }
        });
  }
};

#endif //EPIPE_HTTPCONNECTION_H
