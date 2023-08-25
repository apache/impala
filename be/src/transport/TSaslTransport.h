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

#ifndef IMPALA_TRANSPORT_TSSLTRANSPORT_H
#define IMPALA_TRANSPORT_TSSLTRANSPORT_H

#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <thrift/transport/TTransport.h>
#include <thrift/transport/TVirtualTransport.h>
#include <thrift/transport/TBufferTransports.h>

#include "transport/TSasl.h"

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
  TSaslTransport(std::shared_ptr<TTransport> transport);

  /**
   * Constructs a new TSaslTransport to act as a client.
   *
   */
  TSaslTransport(std::shared_ptr<sasl::TSasl> saslClient,
                 std::shared_ptr<TTransport> transport);

  /**
   * Destroys the TSasl object.
   */
  virtual ~TSaslTransport();

  /**
   * Whether this transport is open.
   */
  virtual bool isOpen() const;

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

  /**
   * Returns the transport underlying this one
   */
  std::shared_ptr<TTransport> getUnderlyingTransport() {
    return transport_;
  }

  /**
   * Returns the username associated with the underlying sasl connection.
   *
   * @throws TTransportException if an error occurs
   */
  std::string getUsername();

  /**
   * Returns the IANA-registered mechanism name from underlying sasl connection.
   *
   * @throws TTransportException if an error occurs
   */
  std::string getMechanismName();

 protected:
  /// Underlying transport
  std::shared_ptr<TTransport> transport_;

  /// Buffer for reading and writing.
  TMemoryBuffer* memBuf_;

  /// Sasl implementation class. This is passed in to the transport constructor
  /// initialized for either a client or a server.
  std::shared_ptr<sasl::TSasl> sasl_;

  /// IF true we wrap data in encryption.
  bool shouldWrap_;

  /// True if this is a client.
  bool isClient_;

  /// Buffer to hold protocol info.
  boost::scoped_array<uint8_t> protoBuf_;

  /* store the big endian format int to given buffer */
  void encodeInt(uint32_t x, uint8_t* buf, uint32_t offset) {
    *(reinterpret_cast<uint32_t*>(buf + offset)) = htonl(x);
  }

  /* load the big endian format int to given buffer */
  uint32_t decodeInt (uint8_t* buf, uint32_t offset) {
    return ntohl(*(reinterpret_cast<uint32_t*>(buf + offset)));
  }

  /**
   * Performs the SASL negotiation.
   */
  void doSaslNegotiation();

  /**
   * Create the Sasl context for a server/client connection.
   */
  virtual void setupSaslNegotiationState() = 0;

  /**
   * Reset the negotiation state.
   */
  virtual void resetSaslNegotiationState() = 0;

  /**
   * Read a complete Thrift SASL message.
   *
   * @return The SASL status and payload from this message.
   *    Is valid only to till the next call.
   * @throws TTransportException
   *           Thrown if there is a failure reading from the underlying
   *           transport, or if a status code of BAD or ERROR is encountered.
   */
  uint8_t* receiveSaslMessage(NegotiationStatus* status , uint32_t* length);

  /**
   * send message with SASL transport headers.
   * status is put before the payload.
   * If flush is false we delay flushing the underlying transport so
   * that the following message will be in the same packet if necessary.
   */
  void sendSaslMessage(const NegotiationStatus status,
                       const uint8_t* payload, const uint32_t length, bool flush = true);

  /**
   * Opens the transport for communications.
   *
   * @return bool Whether the transport was successfully opened
   * @throws TTransportException if opening failed
   */
  uint32_t readLength();

  /**
   * Write the given integer as 4 bytes to the underlying transport.
   *
   * @param length
   *          The length prefix of the next SASL message to write.
   * @throws TTransportException
   *           Thrown if writing to the underlying transport fails.
   */
  void writeLength(uint32_t length);
  virtual void handleSaslStartMessage() = 0;

  /// If memBuf_ is filled with bytes that are already read, and has crossed a size
  /// threshold (see implementation for exact value), resize the buffer to a default value.
  void shrinkBuffer();

};

}}} // apache::thrift::transport

#endif // #ifndef IMPALA_TRANSPORT_TSSLTRANSPORT_H
