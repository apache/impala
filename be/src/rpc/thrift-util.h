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

#include <sstream>
#include <vector>

#include <boost/shared_ptr.hpp>
#include <thrift/TApplicationException.h>
#include <thrift/TConfiguration.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSSLSocket.h>
#include <thrift/transport/TTransportException.h>

#include "common/status.h"
#include "util/codec.h"

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

// Helper function to set keepalive options on the provided THRIFT_SOCKET.
// These options are only effective if keepalive is enabled separately (by Thrift).
Status SetKeepAliveOptionsForSocket(THRIFT_SOCKET, int32_t probe_period_s,
    int32_t retry_period_s, int32_t retry_count);

// Impala uses TServerSocket and TSSLServerSocket for external client connections.
// Thrift has a built-in ability to turn on keepalive for the TCP socket. However, it
// does not have an ability to tune the keepalive options, so the socket would use the
// OS default settings. ImpalaKeepAliveServerSocket is a templated class that takes
// in an underlying TServerSocket / TSSLServerSocket type. It behaves like the
// underlying type (and uses the same constructor signatures) except that it sets these
// keepalive options on the socket returned by createSocket():
//  - probe period / TCP_KEEPIDLE: Time before first keepalive probe
//  - retry period / TCP_KEEPINTVL: Time between retries after keepalive starts
//  - number of retries / TCP_KEEPCNT: Maximum number of retries
template <typename ThriftServerSocketType>
class ImpalaKeepAliveServerSocket : public ThriftServerSocketType {
 public:
  using ThriftServerSocketType::ThriftServerSocketType;

  // This is called immediately after calling the constructor to store the
  // keepalive settings to apply to the future sockets. This will set
  // Thrift's keepalive setting as well. It must be called before anything
  // will call createSocket().
  void setKeepAliveOptions(int32_t probe_period_s, int32_t retry_period_s,
      int32_t retry_count) {
    keepalive_enabled_ = probe_period_s > 0;
    ThriftServerSocketType::setKeepAlive(keepalive_enabled_);
    keepalive_probe_period_s_ = probe_period_s;
    keepalive_retry_period_s_ = retry_period_s;
    keepalive_retry_count_ = retry_count;
  }

 protected:
  // Get a socket from the underlying TServerSocket / TSSLServerSocket type, then set the
  // keepalive options on the socket before returning it. Note: The THRIFT_SOCKET
  // type on Linux is a standard socket file descriptor.
  std::shared_ptr<apache::thrift::transport::TSocket> createSocket(THRIFT_SOCKET socket) {
    std::shared_ptr<apache::thrift::transport::TSocket> tsocket =
        ThriftServerSocketType::createSocket(socket);
    if (keepalive_enabled_) {
      Status status = SetKeepAliveOptionsForSocket(socket, keepalive_probe_period_s_,
          keepalive_retry_period_s_, keepalive_retry_count_);
      if (!status.ok()) {
        throw apache::thrift::transport::TTransportException(
            apache::thrift::transport::TTransportException::INTERNAL_ERROR,
            status.msg().msg());
      }
    }
    return tsocket;
  }

 private:
  bool keepalive_enabled_ = false;
  int32_t keepalive_probe_period_s_ = 0;
  int32_t keepalive_retry_period_s_ = 0;
  int32_t keepalive_retry_count_ = 0;
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

// Helper that compresses a raw buffer using LZ4 and creates a TCompressed object.
//
// TCompressed must have:
//  - int64_t uncompressed_size;
//  - binary compressed_data;
//
// Params:
//   serialized_buf: Raw uncompressed data to compress.
//   serialized_len: Length of the uncompressed data in bytes.
//   dest: Output parameter for the compressed thrift object.
template <typename TCompressed>
Status CreateCompressedThrift(
    const uint8_t* serialized_buf, uint32_t serialized_len, TCompressed* dest) {
  DCHECK(serialized_buf != nullptr);
  DCHECK(dest != nullptr);

  // Prepare compressor.
  boost::scoped_ptr<Codec> compressor;
  Codec::CodecInfo codec_info(THdfsCompression::LZ4);
  RETURN_IF_ERROR(Codec::CreateCompressor(nullptr, false, codec_info, &compressor));

  // Calculate buffer size and resize string.
  int64_t max_out = compressor->MaxOutputLen(serialized_len);
  std::string compressed_str(max_out, '\0');

  // Perform Compression.
  uint8_t* out_ptr = reinterpret_cast<uint8_t*>(compressed_str.data());
  int64_t actual_len = max_out;
  RETURN_IF_ERROR(compressor->ProcessBlock(
      true, serialized_len, serialized_buf, &actual_len, &out_ptr));
  // Validate output length.
  if (actual_len <= 0 || actual_len > std::numeric_limits<uint32_t>::max()) {
    return Status(strings::Substitute(
        "Invalid actual compressed length in compressed request: $0B", actual_len));
  }
  compressed_str.resize(actual_len);

  // Write to the TCompressed object.
  dest->uncompressed_size = static_cast<int64_t>(serialized_len);
  dest->compressed_data = std::move(compressed_str);

  return Status::OK();
}

// Serializes a T object 'src', creates a compressed thrift by the serialized bytes,
// and writes to a compressed T object 'dest'.
//
// Params:
//   src: The thrift object to serialize and compress.
//   dest: Output parameter for the compressed thrift object.
template <typename T, typename TCompressed>
Status CompressThrift(const T& src, TCompressed* dest) {
  ThriftSerializer serializer(/* compact */ true);
  uint8_t* serialized_buf = nullptr;
  uint32_t serialized_len = 0;
  RETURN_IF_ERROR(serializer.SerializeToBuffer(&src, &serialized_len, &serialized_buf));
  return CreateCompressedThrift<TCompressed>(serialized_buf, serialized_len, dest);
}

// Decompresses a compressed Thrift object 'src' and deserializes it back into
// the standard Thrift object 'dest'.
//
// TCompressed must have:
//  - int64_t uncompressed_size;
//  - binary compressed_data;
//
// Params:
//   src: The compressed thrift object to decompress and deserialize.
//   dest: Output parameter for the deserialized thrift object.
template <typename TCompressed, typename T>
Status DecompressThrift(const TCompressed& src, T* dest) {
  DCHECK(dest != nullptr);
  int64_t uncompressed_len = src.uncompressed_size;
  if (uncompressed_len <= 0 || uncompressed_len > std::numeric_limits<uint32_t>::max()) {
    return Status(strings::Substitute(
        "Invalid uncompressed size in compressed request: $0B", uncompressed_len));
  }

  // Prepare decompressor and decompress.
  boost::scoped_ptr<Codec> decompressor;
  RETURN_IF_ERROR(
      Codec::CreateDecompressor(nullptr, false, THdfsCompression::LZ4, &decompressor));
  std::vector<uint8_t> uncompressed_buf(uncompressed_len);
  uint8_t* out_ptr = uncompressed_buf.data();
  int64_t actual_out = uncompressed_len;
  const std::string& input_data = src.compressed_data;
  const uint8_t* in_ptr = reinterpret_cast<const uint8_t*>(input_data.data());
  RETURN_IF_ERROR(
      decompressor->ProcessBlock(true, input_data.size(), in_ptr, &actual_out, &out_ptr));
  if (actual_out <= 0 || actual_out > std::numeric_limits<uint32_t>::max()) {
    return Status(strings::Substitute(
        "Invalid actual output size after decompression: $0B", actual_out));
  }
  if (actual_out != uncompressed_len) {
    return Status("Decompressed size did not match expected size from header");
  }

  // Deserialize and write to the T object.
  uint32_t deser_len = static_cast<uint32_t>(actual_out);
  return DeserializeThriftMsg(uncompressed_buf.data(), &deser_len, true, dest);
}
}

#endif
