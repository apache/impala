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


#ifndef IMPALA_RPC_THRIFT_UTIL_H
#define IMPALA_RPC_THRIFT_UTIL_H

#include <boost/shared_ptr.hpp>
#include <thrift/protocol/TBinaryProtocol.h>
#include <sstream>
#include <vector>
#include <thrift/TApplicationException.h>
#include <thrift/TConfiguration.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TTransportException.h>

#include "common/status.h"

namespace impala {

class TColumnValue;
class TNetworkAddress;
class ThriftServer;

/// Default max message size from Thrift library.
inline int64_t ThriftDefaultMaxMessageSize() {
  return apache::thrift::TConfiguration::DEFAULT_MAX_MESSAGE_SIZE;
}

/// Return the effective max message size based on 'thrift_rpc_max_message_size' flag.
/// This is for trusted internal communication between cluster components such as
/// as statestore/catalog/HMS. Because messages are coming from trusted sources, a
/// higher limit does not introduce security issues.
int64_t ThriftInternalRpcMaxMessageSize();

/// Return the effective max message size based on 'thrift_external_rpc_max_message_size'
/// flag. This is for untrusted communication with clients. Using a lower limit protects
/// against malicious messages.
int64_t ThriftExternalRpcMaxMessageSize();

/// Return the default Thrift TConfiguration using the limit for trusted internal
/// communication.
std::shared_ptr<apache::thrift::TConfiguration> DefaultInternalTConfiguration();

/// Return the default Thrift TConfiguration using the limit for untrusted external
/// communication.
std::shared_ptr<apache::thrift::TConfiguration> DefaultExternalTConfiguration();

/// Set the max message size of a given TTransport to the specified value.
/// The value should be either ThriftInternalRpcMaxMessageSize() or
/// ThriftExternalRpcMaxMessageSize().
void SetMaxMessageSize(apache::thrift::transport::TTransport* transport,
    int64_t max_message_size);

/// Verify that the max message size has been inherited properly. The source transport
/// (i.e. the one being wrapped) must already have the max message size set to either the
/// internal limit or the external limit. The destination transport (i.e. the wrapping
/// transport) must have that same limit. This DCHECKs if these conditions do not hold.
void VerifyMaxMessageSizeInheritance(apache::thrift::transport::TTransport* source,
    apache::thrift::transport::TTransport* dest);

/// Utility class to serialize thrift objects to a binary format.  This object
/// should be reused if possible to reuse the underlying memory.
/// Note: thrift will encode NULLs into the serialized buffer so it is not valid
/// to treat it as a string.
class ThriftSerializer {
 public:
  /// If compact, the objects will be serialized using the Compact Protocol.  Otherwise,
  /// we'll use the binary protocol.
  /// Note: the deserializer must be matching.
  ThriftSerializer(bool compact, int initial_buffer_size = 1024);

  /// Serializes obj into result.  Result will contain a copy of the memory.
  template <class T>
  Status SerializeToVector(const T* obj, std::vector<uint8_t>* result) {
    uint32_t len;
    uint8_t* buffer;
    RETURN_IF_ERROR(SerializeToBuffer(obj, &len, &buffer));
    result->assign(buffer, buffer + len);
    return Status::OK();
  }

  /// Serialize obj into a memory buffer.  The result is returned in buffer/len.  The
  /// memory returned is owned by this object and will be invalid when another object
  /// is serialized.
  template <class T>
  Status SerializeToBuffer(const T* obj, uint32_t* len, uint8_t** buffer) {
    try {
      mem_buffer_->resetBuffer();
      obj->write(protocol_.get());
    } catch (std::exception& e) {
      std::stringstream msg;
      msg << "Couldn't serialize thrift object beyond "
          << mem_buffer_->getBufferSize() << " bytes:\n" << e.what();
      return Status(msg.str());
    }
    mem_buffer_->getBuffer(buffer, len);
    return Status::OK();
  }

  template <class T>
  Status SerializeToString(const T* obj, std::string* result) {
    try {
      mem_buffer_->resetBuffer();
      obj->write(protocol_.get());
    } catch (std::exception& e) {
      std::stringstream msg;
      msg << "Couldn't serialize thrift object beyond "
          << mem_buffer_->getBufferSize() << " bytes:\n" << e.what();
      return Status(msg.str());
    }
    *result = mem_buffer_->getBufferAsString();
    return Status::OK();
  }

 private:
  std::shared_ptr<apache::thrift::transport::TMemoryBuffer> mem_buffer_;
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_;
};

/// Utility to create a protocol (deserialization) object for 'mem'.
std::shared_ptr<apache::thrift::protocol::TProtocol>
CreateDeserializeProtocol(
    std::shared_ptr<apache::thrift::transport::TMemoryBuffer> mem, bool compact);

/// Deserialize a thrift message from buf/len.  buf/len must at least contain
/// all the bytes needed to store the thrift message.  On return, len will be
/// set to the actual length of the header.
template <class T>
Status DeserializeThriftMsg(const uint8_t* buf, uint32_t* len, bool compact,
    T* deserialized_msg) {
  /// Deserialize msg bytes into c++ thrift msg using memory
  /// transport. TMemoryBuffer is not const-safe, although we use it in
  /// a const-safe way, so we have to explicitly cast away the const.
  ///
  /// This uses the external max message size limit, because this is often used
  /// for small data structures that don't need a higher limit. It is used for a
  /// few untrusted data structures like Parquet headers.
  std::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(const_cast<uint8_t*>(buf), *len,
          apache::thrift::transport::TMemoryBuffer::MemoryPolicy::OBSERVE,
          DefaultExternalTConfiguration()));
  std::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
      CreateDeserializeProtocol(tmem_transport, compact);
  try {
    deserialized_msg->read(tproto.get());
  } catch (std::exception& e) {
    std::stringstream msg;
    msg << "couldn't deserialize thrift msg:\n" << e.what();
    return Status::Expected(msg.str());
  } catch (...) {
    /// TODO: Find the right exception for 0 bytes
    return Status("Unknown exception");
  }
  uint32_t bytes_left = tmem_transport->available_read();
  *len = *len - bytes_left;
  return Status::OK();
}

class ImpalaTlsSocketFactory : public apache::thrift::transport::TSSLSocketFactory {
 public:
  ImpalaTlsSocketFactory(apache::thrift::transport::SSLProtocol version)
    : TSSLSocketFactory(version) {}

  // 'cipher_list': TLS1.2 and below cipher list
  // 'tls_ciphersuites': TLS1.3 and above cipher suites
  // 'disable_tls12': Whether to disable TLS1.2 (used for testing TLS1.3).
  void configureCiphers(const string& cipher_list, const string& tls_ciphersuites,
      bool disable_tls12);
};


/// Redirects all Thrift logging to VLOG(1)
void InitThriftLogging();

/// Wait for a server that is running locally to start accepting
/// connections, up to a maximum timeout
Status WaitForLocalServer(const ThriftServer& server, int num_retries,
   int retry_interval_ms);

/// Wait for a server to start accepting connections, up to a maximum timeout
Status WaitForServer(const std::string& host, int port, int num_retries,
   int retry_interval_ms);

/// Print a TColumnValue. If null, print "NULL".
void PrintTColumnValue(std::ostream& out, const TColumnValue& colval);

/// Returns true if the TTransportException corresponds to a TCP socket read timeout.
bool IsReadTimeoutTException(const apache::thrift::transport::TTransportException& e);

/// Returns true if the TTransportException corresponds to a TCP socket peek timeout.
bool IsPeekTimeoutTException(const apache::thrift::transport::TTransportException& e);

/// Returns true if the exception indicates the other end of the TCP socket was closed.
bool IsConnResetTException(const apache::thrift::transport::TTransportException& e);
}

#endif
