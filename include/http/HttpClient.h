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

#ifndef EPIPE_HTTPCLIENT_H
#define EPIPE_HTTPCLIENT_H

#include "common.h"
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/ssl/error.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/algorithm/string.hpp>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = net::ip::tcp;
namespace ssl = net::ssl;

struct HttpResponse{
  bool mSuccess;
  unsigned int mCode;
  std::string mResponse;
};

struct HttpClientConfig{
  std::string mAddr;
  bool mSSLEnabled;
  std::string mCAPath;
  std::string mUserName;
  std::string mPassword;

  bool isValidUserAndPass(){
    return mUserName != "" && mPassword != "";
  }

  std::string getAuthorization(){
    std::string userNP = mUserName + ":" + mPassword;
    std::string authorization = "Basic " + encode64(userNP);
    return authorization;
  }

private:
  std::string encode64(const std::string &val) {
    using namespace boost::archive::iterators;
    using It = base64_from_binary<transform_width<std::string::const_iterator, 6, 8>>;
    auto tmp = std::string(It(std::begin(val)), It(std::end(val)));
    return tmp.append((3 - val.size() % 3) % 3, '=');
  }

  std::string decode64(const std::string &val) {
    using namespace boost::archive::iterators;
    using It = transform_width<binary_from_base64<std::string::const_iterator>, 8, 6>;
    return boost::algorithm::trim_right_copy_if(std::string(It(std::begin(val)), It(std::end(val))), [](char c) {
      return c == '\0';
    });
  }
};

class HttpClient{
public:
  HttpClient(const HttpClientConfig config){
    std::string addr = config.mAddr;
    try {
      auto const i = addr.find(":");
      auto const ip = addr.substr(0, i);
      auto const port = static_cast<unsigned short>(std::atoi(
          addr.substr(i + 1, addr.length()).c_str()));
      mEndpoint = {net::ip::make_address(ip), port};
    } catch(const boost::system::system_error & ex){
      LOG_FATAL("error in http address format [" << addr << "]"
          << " only ips allowed : " <<  ex.code() << " " << ex.what());
    }
    mConfig = config;
  }

  HttpResponse get(std::string target){
    return request(http::verb::get, target);
  }

  HttpResponse post(std::string target, std::string data){
    return request(http::verb::post, target, data);
  }

  HttpResponse delete_(std::string target){
    return request(http::verb::delete_, target);
  }
private:
  tcp::endpoint mEndpoint;
  HttpClientConfig mConfig;

  HttpResponse request(http::verb verb, std::string
  target){
    return request(verb, target, "");
  }

  HttpResponse request(http::verb verb, std::string
  target, std::string data){
    if(mConfig.mSSLEnabled){
      return request_with_ssl(verb, target, data);
    }else{
      return request_no_ssl(verb, target, data);
    }
  }

  HttpResponse request_no_ssl(http::verb verb, std::string
  target, std::string data){

    std::string responseBody;
    bool succeed = true;
    unsigned int code = 0;

    try
    {
      net::io_context ioc;
      beast::tcp_stream stream(ioc);

      stream.connect(mEndpoint);

      http::request<http::string_body> req{verb, target, 11};
      req.set(http::field::host, mEndpoint.address().to_string());
      req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
      req.set(http::field::content_type, "application/json");

      if(mConfig.isValidUserAndPass()) {
        req.set(http::field::authorization, mConfig.getAuthorization());
      }

      if(data.length() != 0){
        req.body() = data;
        req.set(http::field::content_length, data.length());
        req.prepare_payload();
      }

      http::write(stream, req);

      beast::flat_buffer buffer;

      http::response<http::dynamic_body> response;

      http::read(stream, buffer, response);

      responseBody = beast::buffers_to_string(response.body().data());
      code = response.result_int();

      beast::error_code ec;
      stream.socket().shutdown(tcp::socket::shutdown_both, ec);

      if(ec && ec != beast::errc::not_connected){
        LOG_ERROR("Error in http connection with error code "<< ec);
        succeed = false;
      }
    }catch(std::exception const& e)
    {
      LOG_ERROR("Error in http connection : " << e.what());
      succeed = false;
    }

    return {succeed, code, responseBody};
  }

  HttpResponse request_with_ssl(http::verb verb, std::string
  target, std::string data){

    std::string responseBody;
    bool succeed = true;
    unsigned int code = 0;

    try
    {
      net::io_context ioc;

      // The SSL context is required, and holds certificates
      ssl::context ctx(ssl::context::tlsv12_client);
      load_ca_certificates(ctx);
      ctx.set_verify_mode(ssl::verify_peer);

      beast::ssl_stream<beast::tcp_stream> stream(ioc, ctx);

      // Set SNI Hostname (many hosts need this to handshake successfully)
      if(! SSL_set_tlsext_host_name(stream.native_handle(), mEndpoint.address
      ().to_string().c_str()))
      {
        beast::error_code ec{-1, net::error::get_ssl_category()};
        throw beast::system_error{ec};
      }

      beast::get_lowest_layer(stream).connect(mEndpoint);
      stream.handshake(ssl::stream_base::client);

      http::request<http::string_body> req{verb, target, 11};
      req.set(http::field::host, mEndpoint.address().to_string());
      req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
      req.set(http::field::content_type, "application/json");

      if(mConfig.isValidUserAndPass()) {
        req.set(http::field::authorization, mConfig.getAuthorization());
      }

      if(data.length() != 0){
        req.body() = data;
        req.set(http::field::content_length, data.length());
        req.prepare_payload();
      }

      http::write(stream, req);

      beast::flat_buffer buffer;

      http::response<http::dynamic_body> response;

      http::read(stream, buffer, response);

      responseBody = beast::buffers_to_string(response.body().data());
      code = response.result_int();

      beast::error_code ec;
      stream.shutdown(ec);

      if(ec && ec != beast::errc::not_connected){
        LOG_ERROR("Error in http connection with error code "<< ec);
        succeed = false;
      }
    }catch(std::exception const& e)
    {
      LOG_ERROR("Error in http connection : " << e.what());
      succeed = false;
    }

    return {succeed, code, responseBody};
  }


  void load_ca_certificates(ssl::context& ctx){
    std::ifstream ifs(mConfig.mCAPath.c_str());
    std::string const cacert = std::string(std::istreambuf_iterator<char>(ifs),
        std::istreambuf_iterator<char>());

    boost::system::error_code ec;

    ctx.add_certificate_authority(
        boost::asio::buffer(cacert.data(), cacert.size()), ec);
    if(ec)
      LOG_ERROR("Error while loading the ca certificates : " << ec);
  }
};
#endif //EPIPE_HTTPCLIENT_H
