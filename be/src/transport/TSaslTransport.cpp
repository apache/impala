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

#include "config.h"
#ifdef HAVE_SASL_SASL_H
#include <stdint.h>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>

#include <transport/TBufferTransports.h>
#include <transport/TSaslTransport.h>

namespace apache { namespace thrift { namespace transport {

  TSaslTransport::TSaslTransport(boost::shared_ptr<TTransport> transport) 
      : transport_(transport),
        memBuf_(new TMemoryBuffer()),
        sasl_(NULL),
        shouldWrap_(false),
        isClient_(false) {
    TSaslTransportInit();
  }

  TSaslTransport::TSaslTransport(sasl::TSasl* saslClient,
                                 boost::shared_ptr<TTransport> transport)
      : transport_(transport),
        memBuf_(new TMemoryBuffer()),
        sasl_(saslClient),
        shouldWrap_(false),
        isClient_(true) {
    TSaslTransportInit();
  }

  void TSaslTransport::TSaslTransportInit() {
    unsigned int i = 1;
    char* c = (char*)&i;
    if (*c) {
      needByteSwap_ = true;
    } else {
      needByteSwap_ = false;
    }
  }

  TSaslTransport::~TSaslTransport() {
    delete memBuf_;
  }

  /**
   * Whether this transport is open.
   */
  bool TSaslTransport::isOpen() {
    return transport_->isOpen();
  }

  /**
   * Tests whether there is more data to read or if the remote side is
   * still open.
   */
  bool TSaslTransport::peek(){
    return (transport_->peek());
  }

  /**
   * send message with SASL transport headers.
   * status is put before the payload.
   * If flush is false we delay flushing the underlying transport so
   * that the following message will be in the same packet if necessary.
   */
  void TSaslTransport::sendSaslMessage(NegotiationStatus status, uint8_t* payload,
                                       uint32_t length, bool flush) {
    uint8_t messageHeader[STATUS_BYTES + PAYLOAD_LENGTH_BYTES];
    uint8_t dummy = 0;
    if (payload == NULL) {
      payload = &dummy;
    }
    messageHeader[0] = (uint8_t)status;
    encodeInt(length, messageHeader, STATUS_BYTES);
    transport_->write(messageHeader, HEADER_LENGTH);
    transport_->write(payload, length);
    if (flush) transport_->flush();
  }

  /**
   * Opens the transport for communications.
   *
   * @return bool Whether the transport was successfully opened
   * @throws TTransportException if opening failed
   */
  void TSaslTransport::open() {
    NegotiationStatus status = TSASL_INVALID;
    uint32_t resLength;

    if (!transport_->isOpen()) {
      transport_->open();
    }

    // initiate  SASL message
    handleSaslStartMessage();

    // SASL connection handshake
    while (!sasl_->isComplete()) {
      uint8_t* message = receiveSaslMessage(status, resLength);
      if (status == TSASL_COMPLETE) {
        if (isClient_) break; // handshake complete
      } else if (status != TSASL_OK) {
        throw TTransportException("Expected COMPLETE or OK, got " + status);
      }
      uint8_t* challenge = sasl_->evaluateChallengeOrResponse(message, resLength);
      sendSaslMessage(sasl_->isComplete() ? TSASL_COMPLETE : TSASL_OK,
                      challenge, resLength);
    }

    // If the server isn't complete yet, we need to wait for its response.
    // This will occur with ANONYMOUS auth, for example, where we send an
    // initial response and are immediately complete.
    if ((isClient_ && (status == TSASL_INVALID)) || status == TSASL_OK) {
      boost::shared_ptr<uint8_t> message(receiveSaslMessage(status, resLength));
      if (status != TSASL_COMPLETE) {
        throw TTransportException("Expected SASL COMPLETE, but got " + status);
      }
    }

    // TODO : need to set the shouldWrap_ based on QOP
    /*
    String qop = (String) sasl.getNegotiatedProperty(Sasl.QOP);
    if (qop != null && !qop.equalsIgnoreCase("auth"))
      shouldWrap_ = true;
    */
  }

  /**
   * Closes the transport.
   */
  void TSaslTransport::close() {
    transport_->close();
    if (sasl_ != NULL) sasl_->dispose();
  }

    /**
   * Read a 4-byte word from the underlying transport and interpret it as an
   * integer.
   * 
   * @return The length prefix of the next SASL message to read.
   * @throws TTransportException
   *           Thrown if reading from the underlying transport fails.
   */
  uint32_t TSaslTransport::readLength() {
    uint8_t lenBuf[PAYLOAD_LENGTH_BYTES];

    transport_->readAll(lenBuf, PAYLOAD_LENGTH_BYTES);
    return decodeInt(lenBuf, 0);
  }


  /**
   * Attempt to read up to the specified number of bytes into the string.
   *
   * @param buf  Reference to the location to write the data
   * @param len  How many bytes to read
   * @return How many bytes were actually read
   * @throws TTransportException If an error occurs
   */
  uint32_t TSaslTransport::read(uint8_t* buf, uint32_t len) {

    // if there's not enough data in cache, read from underlying transport
    if (memBuf_->available_read() < len) {
      uint32_t dataLength = readLength();

      // Fast path
      if (len == dataLength && !shouldWrap_) {
        return transport_->read(buf, len);
      }

      uint8_t* tmpBuf = new uint8_t[dataLength];
      dataLength = transport_->read(tmpBuf, dataLength);
      if (shouldWrap_) {
        tmpBuf = sasl_->unwrap(tmpBuf, 0, dataLength);
      }

      // We will consume all the data, no need to put it in the memory buffer.
      if (len == dataLength) {
        memcpy(buf, tmpBuf, len);
        delete tmpBuf;
        return len;
      }

      memBuf_->write(tmpBuf, dataLength);
      memBuf_->flush();
      delete tmpBuf;
    }
    return memBuf_->read(buf, len);
  }

  /**
   * Write the given integer as 4 bytes to the underlying transport.
   * 
   * @param length
   *          The length prefix of the next SASL message to write.
   * @throws TTransportException
   *           Thrown if writing to the underlying transport fails.
   */
  void TSaslTransport::writeLength(uint32_t length) {
    uint8_t lenBuf[PAYLOAD_LENGTH_BYTES];

    encodeInt(length, lenBuf, 0);
    transport_->write(lenBuf, PAYLOAD_LENGTH_BYTES);
  }

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
  void TSaslTransport::write(const uint8_t* buf, uint32_t len) {
    const uint8_t* newBuf;

    if (shouldWrap_) {
      newBuf = sasl_->wrap((uint8_t*)buf, 0, len);
    } else {
      newBuf = buf;
    }
    writeLength(len);
    transport_->write(newBuf, len);
  }

  /**
   * Flushes any pending data to be written. Typically used with buffered
   * transport mechanisms.
   *
   * @throws TTransportException if an error occurs
   */
  void TSaslTransport::flush() {
    transport_->flush();
  }

  /**
   * Read a complete Thrift SASL message.
   * 
   * @return The SASL status and payload from this message.
   *    Is valid only to till the next call.
   * @throws TTransportException
   *           Thrown if there is a failure reading from the underlying
   *           transport, or if a status code of BAD or ERROR is encountered.
   */
  uint8_t* TSaslTransport::receiveSaslMessage(NegotiationStatus &status,
                                              uint32_t& length) {
    uint8_t messageHeader[HEADER_LENGTH];

    // read header
    transport_->readAll(messageHeader, HEADER_LENGTH);

    // get payload status
    status = (NegotiationStatus)messageHeader[0];
    if ((status < TSASL_START) || (status > TSASL_COMPLETE)) {
      throw TTransportException("invalid sasl status");
    } else if (status == TSASL_BAD || status == TSASL_ERROR) {
        throw TTransportException("sasl Peer indicated failure: ");
    }

    // get the length
    length = decodeInt(messageHeader, STATUS_BYTES);

    // get payload
    protoBuf_.reset(new uint8_t[length]);
    transport_->readAll(protoBuf_.get(), length);

    return protoBuf_.get();
  }


  /* store the big endian format int to given buffer */
  void TSaslTransport::encodeInt(uint32_t x, uint8_t* buf, uint32_t offset) {
    if (needByteSwap_) {
      buf[offset]     = (uint8_t) (0xff & (x >> 24));
      buf[offset + 1] = (uint8_t) (0xff & (x >> 16));
      buf[offset + 2] = (uint8_t) (0xff & (x >> 8));
      buf[offset + 3] = (uint8_t) (0xff & (x));
    } else {
      buf[offset]     = (uint8_t) (0xff & (x));
      buf[offset + 1] = (uint8_t) (0xff & (x >> 8));
      buf[offset + 2] = (uint8_t) (0xff & (x >> 16));
      buf[offset + 3] = (uint8_t) (0xff & (x >> 24));
    }
  }

  /* load the big endian format int to given buffer */
  uint32_t TSaslTransport::decodeInt (uint8_t* buf, uint32_t offset) {
    if (needByteSwap_) {
      return ((buf[offset] & 0xff) << 24) | ((buf[offset + 1] & 0xff) << 16)
            | ((buf[offset + 2] & 0xff) << 8) | ((buf[offset + 3] & 0xff));
    } else {
      return ((buf[offset + 3] & 0xff)) | ((buf[offset + 2] & 0xff) << 8) 
            | ((buf[offset + 1] & 0xff) << 16) | ((buf[offset] & 0xff) << 24);
    }
  }

}
}
}

#endif
