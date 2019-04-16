/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef IMPALA_TRANSPORT_THTTPSERVER_H
#define IMPALA_TRANSPORT_THTTPSERVER_H

#include "transport/THttpTransport.h"

namespace apache {
namespace thrift {
namespace transport {

/*
 * Implements server side work for http connections, including support for BASIC auth.
 */
class THttpServer : public THttpTransport {
public:

  // Function that takes a base64 encoded string of the form 'username:password' and
  // returns true if authentication is successful.
  typedef std::function<bool(const char*)> BasicAuthFn;

  THttpServer(boost::shared_ptr<TTransport> transport, bool requireBasicAuth = false);

  virtual ~THttpServer();

  virtual void flush();

  void setAuthFn(const BasicAuthFn& fn) { authFn_ = fn; }

protected:
  void readHeaders();
  virtual void parseHeader(char* header);
  virtual bool parseStatusLine(char* status);
  virtual void headersDone();
  std::string getTimeRFC1123();
  // Returns a '401 - Unauthorized' to the client.
  void returnUnauthorized();

 private:
  static bool dummyAuthFn(const char*) { return false; }

  // If true, a '401' will be returned and a TTransportException thrown unless each set
  // of headers contains a valid 'Authorization: Basic...'.
  bool requireBasicAuth_ = false;

  // Called with the base64 encoded authorization from a 'Authorization: Basic' header.
  BasicAuthFn authFn_ = &dummyAuthFn;

  // The value from the 'Authorization' header.
  std::string authValue_ = "";
};

/**
 * Wraps a transport into HTTP protocol
 */
class THttpServerTransportFactory : public TTransportFactory {
public:

  explicit THttpServerTransportFactory(bool requireBasicAuth = false)
    : requireBasicAuth_(requireBasicAuth) {}

  virtual ~THttpServerTransportFactory() {}

  /**
   * Wraps the transport into a buffered one.
   */
  virtual boost::shared_ptr<TTransport> getTransport(boost::shared_ptr<TTransport> trans) {
    return boost::shared_ptr<TTransport>(new THttpServer(trans, requireBasicAuth_));
  }

 private:
  bool requireBasicAuth_ = false;
};
}
}
} // apache::thrift::transport

#endif // #ifndef IMPALA_TRANSPORT_THTTPSERVER_H
