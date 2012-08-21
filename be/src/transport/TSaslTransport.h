// This file will be removed when the code is accepted into the Thrift library.
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

#ifndef _THRIFT_TRANSPORT_TSSLTRANSPORT_H_
#define _THRIFT_TRANSPORT_TSSLTRANSPORT_H_ 1

#include <string>

#include <boost/shared_ptr.hpp>
#include <transport/TTransport.h>
#include <transport/TVirtualTransport.h>
#include <transport/TSasl.h>
#include <transport/TBufferTransports.h>

namespace apache { namespace thrift { namespace transport {

enum NegotiationStatus {
  TSASL_INVALID = -1,
  TSASL_START = 1,
  TSASL_OK = 2,
  TSASL_BAD = 3,
  TSASL_ERROR = 4,
  TSASL_COMPLETE = 5
};

static const int MECHANISM_NAME_BYTES = 1;
static const int STATUS_BYTES = 1;
static const int PAYLOAD_LENGTH_BYTES = 4;
static const int HEADER_LENGTH = STATUS_BYTES + PAYLOAD_LENGTH_BYTES;

class SaslResponse {
 public:
  SaslResponse(NegotiationStatus status, uint8_t* payload) {
    this->status = status;
    this->payload = payload;
  }

  NegotiationStatus status;
  uint8_t* payload;
};


/**
 * This transport implements the Simple Authentication and Security Layer (SASL).
 * see: http://www.ietf.org/rfc/rfc2222.txt.  It is based on and depends
 * on the presence of the cyrus-sasl library.
 *
 */
class TSaslTransport : public TVirtualTransport<TSaslTransport> {
 public:
  /**
   * Constructs a new TSaslTransport to act as a server.
   * SetSaslServer must be called later to initialize the SASL endpoint underlying this
   * transport.
   *
   */
  TSaslTransport(boost::shared_ptr<TTransport> transport);

  /**
   * Constructs a new TSaslTransport to act as a client.
   *
   */
  TSaslTransport(sasl::TSasl* saslClient, boost::shared_ptr<TTransport> transport);

  /**
   * Destroys the TSasl object.
   */
  virtual ~TSaslTransport();

  /**
   * Whether this transport is open.
   */
  virtual bool isOpen();

  /**
   * Tests whether there is more data to read or if the remote side is
   * still open.
   */
  virtual bool peek();

  /**
   * Opens the transport for communications.
   *
   * @throws TTransportException if opening failed
   */
  virtual void open();

  /**
   * Closes the transport.
   */
  virtual void close();

   /**
   * Attempt to read up to the specified number of bytes into the string.
   *
   * @param buf  Reference to the location to write the data
   * @param len  How many bytes to read
   * @return How many bytes were actually read
   * @throws TTransportException If an error occurs
   */
  uint32_t read(uint8_t* buf, uint32_t len);

  /**
   * Writes the string in its entirety to the buffer.
   *
   * Note: You must call flush() to ensure the data is actually written,
   * and available to be read back in the future.  Destroying a TTransport
   * object does not automatically flush pending data--if you destroy a
   * TTransport object with written but unflushed data, that data may be
   * discarded.
   *
   * @param buf  The data to write out
   * @throws TTransportException if an error occurs
   */
  void write(const uint8_t* buf, uint32_t len);

  /**
   * Flushes any pending data to be written. Typically used with buffered
   * transport mechanisms.
   *
   * @throws TTransportException if an error occurs
   */
  virtual void flush();

 protected:
  // Underlying transport
  boost::shared_ptr<TTransport> transport_;

  // Buffer for reading and writing.
  TMemoryBuffer* memBuf_;

  // Sasl implimentation class.
  sasl::TSasl* sasl_;

  // IF true we wrap data in encryption.
  bool shouldWrap_;

  // If true swap bytes to put them in network order.
  bool needByteSwap_;

  // True if this is a client.
  bool isClient_;

  // Buffer to hold protocol info.
  boost::scoped_array<uint8_t> protoBuf_;

  uint8_t* receiveSaslMessage(NegotiationStatus &status , uint32_t& length);
  void sendSaslMessage(NegotiationStatus status,
                       uint8_t* payload, uint32_t length, bool flush = true);
  void encodeInt (uint32_t x, uint8_t* buf, uint32_t offset);
  uint32_t decodeInt (uint8_t* buf, uint32_t offset);
  uint32_t readLength();
  void writeLength(uint32_t length);
  virtual void handleSaslStartMessage() = 0;
 
 private:
  void TSaslTransportInit();
};

}}} // apache::thrift::transport

#endif // #ifndef _THRIFT_TRANSPORT_TSSLTRANSPORT_H_
