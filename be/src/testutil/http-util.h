// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <gutil/strings/substitute.h>
#include <iosfwd>
#include <map>
#include <string>

#include "common/status.h"
#include "gflags/gflags_declare.h"

DECLARE_int32(webserver_port);

using boost::asio::ip::tcp;

namespace impala {

struct HttpRequest {
  std::string url_path = "/";
  std::string host = "localhost";
  int32_t port = FLAGS_webserver_port;
  std::map<std::string, std::string> headers = {};

  // Adapted from:
  // http://stackoverflow.com/questions/10982717/get-html-without-header-with-boostasio
  Status Do(std::ostream* out, int expected_code, const std::string& method) {
    try {
      tcp::iostream request_stream;
      request_stream.connect(host, boost::lexical_cast<std::string>(port));
      if (!request_stream) return Status("Could not connect request_stream");

      request_stream << method << " " << url_path << " HTTP/1.1\r\n";
      request_stream << "Host: " << host << ":" << port <<  "\r\n";
      request_stream << "Accept: */*\r\n";
      request_stream << "Cache-Control: no-cache\r\n";
      if (method == "POST") {
        request_stream << "Content-Length: 0\r\n";
      }
      for (const auto& header : headers) {
        request_stream << header.first << ": " << header.second << "\r\n";
      }

      request_stream << "Connection: close\r\n\r\n";
      request_stream.flush();

      std::string line1;
      getline(request_stream, line1);
      if (!request_stream) return Status("No response");

      std::stringstream response_stream(line1);
      std::string http_version;
      response_stream >> http_version;

      unsigned int status_code;
      response_stream >> status_code;

      std::string status_message;
      getline(response_stream, status_message);
      if (!response_stream || http_version.substr(0,5) != "HTTP/") {
        return Status("Malformed response");
      }

      if (status_code != expected_code) {
        return Status(strings::Substitute("Unexpected status code: $0", status_code));
      }

      (*out) << request_stream.rdbuf();
      return Status::OK();
    } catch (const std::exception& e){
      return Status(e.what());
    }
  }

  Status Get(ostream* out, int expected_code = 200) {
    return Do(out, expected_code, "GET");
  }

  Status Post(ostream* out, int expected_code = 200) {
    return Do(out, expected_code, "POST");
  }
}; // struct HttpRequest

Status HttpGet(const std::string& host, const int32_t& port, const std::string& url_path,
    ostream* out, int expected_code = 200, const std::string& method = "GET") {
  return HttpRequest{url_path, host, port}.Do(out, expected_code, method);

}
} // namespace impala
