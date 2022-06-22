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

#ifndef IMPALA_TRANSPORT_THTTPTRANSPORT_H
#define IMPALA_TRANSPORT_THTTPTRANSPORT_H

#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TVirtualTransport.h>

namespace apache {
namespace thrift {
namespace transport {

/**
 * HTTP implementation of the thrift transport. This was irritating
 * to write, but the alternatives in C++ land are daunting. Linking CURL
 * requires 23 dynamic libraries last time I checked (WTF?!?). All we have
 * here is a VERY basic HTTP/1.1 client which supports HTTP 100 Continue,
 * chunked transfer encoding, keepalive, etc. Tested against Apache.
 */
class THttpTransport : public TVirtualTransport<THttpTransport> {
public:
  THttpTransport(std::shared_ptr<TTransport> transport);

  virtual ~THttpTransport();

  void open() override { transport_->open(); }

  bool isOpen() const override { return transport_->isOpen(); }

  bool peek() override { return transport_->peek(); }

  void close() override { transport_->close(); }

  uint32_t read(uint8_t* buf, uint32_t len);

  uint32_t readEnd() override;

  void write(const uint8_t* buf, uint32_t len);

  virtual void flush() override = 0;

  virtual const std::string getOrigin() const override;

  std::shared_ptr<TTransport> getUnderlyingTransport() { return transport_; }

protected:
  std::shared_ptr<TTransport> transport_;
  std::string origin_;

  TMemoryBuffer writeBuffer_;
  TMemoryBuffer readBuffer_;

  bool readHeaders_;
  bool readWholeBodyForAuth_;
  bool chunked_;
  bool chunkedDone_;
  uint32_t chunkSize_;
  uint32_t contentLength_;

  char* httpBuf_;
  uint32_t httpPos_;
  uint32_t httpBufLen_;
  uint32_t httpBufSize_;

  // Set to 'true' for a request if the "Expect: 100-continue" header was present.
  // Indicates that we should return a "100 Continue" response if the headers are
  // successfully validated before reading the contents of the request.
  bool continue_ = false;

  void init();

  uint32_t readMoreData();
  char* readLine();

  void readHeaders();
  virtual void parseHeader(char* header) = 0;
  virtual bool parseStatusLine(char* status) = 0;
  // Called each time we finish reading a set of headers. Allows subclasses to do
  // verification, eg. of authorization, before proceeding.
  virtual void headersDone() {}
  virtual void bodyDone(uint32_t size) {}

  uint32_t readChunked();
  void readChunkedFooters();
  uint32_t parseChunkSize(char* line);

  uint32_t readContent(uint32_t size);

  void refill();
  void shift();

  static const char* CRLF;
  static const int CRLF_LEN;
};
}
}
} // apache::thrift::transport

#endif // #ifndef IMPALA_TRANSPORT_THTTPCLIENT_H
