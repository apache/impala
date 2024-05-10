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

#include "rpc/thrift-util.h"

#include <limits>

#include <gtest/gtest.h>
#include <thrift/config.h>

#include "kudu/security/security_flags.h"
#include "kudu/util/openssl_util.h"
#include "util/hash-util.h"
#include "util/openssl-util.h"
#include "util/time.h"
#include "rpc/thrift-server.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Types_types.h"

// TCompactProtocol requires some #defines to work right.  They also define UNLIKELY
// so we need to undef this.
// TODO: is there a better include to use?
#ifdef UNLIKELY
#undef UNLIKELY
#endif
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1

// Thrift does things like throw exception("some string " + int) which just returns
// garbage.
// TODO: get thrift to fix this.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wstring-plus-int"
#include <gutil/strings/substitute.h>
#include <thrift/TConfiguration.h>
#include <thrift/Thrift.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/protocol/TCompactProtocol.h>
#pragma clang diagnostic pop

#include "common/names.h"

DEFINE_int64(thrift_rpc_max_message_size, 64L * 1024 * 1024 * 1024,
    "The maximum size of a message for intra-cluster RPC communication between Impala "
    "components. Default to a high limit of 64GB. "
    "This must be set to at least the default defined in Thrift (100MB). "
    "Setting 0 or a negative value will use the default defined in Thrift.");

DEFINE_int64(thrift_external_rpc_max_message_size, 2L * 1024 * 1024 * 1024,
    "The maximum size of a message for external client RPC communication. "
    "This defaults to 2GB to limit the impact of untrusted payloads. "
    "This must be set to at least the default defined in Thrift (100MB). "
    "Setting 0 or a negative value will use the default defined in Thrift.");

using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::protocol;
using namespace apache::thrift::concurrency;

// IsReadTimeoutTException(), IsPeekTimeoutTException() and IsConnResetTException() make
// assumption about the implementation of read(), peek(), write() and write_partial() in
// TSocket.cpp and TSSLSocket.cpp. Those functions may change between different versions
// of Thrift.
#define NEW_THRIFT_VERSION_MSG \
  "Thrift 0.16.0 is expected. Please check Thrift error codes during Thrift upgrade."
static_assert(PACKAGE_VERSION[0] == '0', NEW_THRIFT_VERSION_MSG);
static_assert(PACKAGE_VERSION[1] == '.', NEW_THRIFT_VERSION_MSG);
static_assert(PACKAGE_VERSION[2] == '1', NEW_THRIFT_VERSION_MSG);
static_assert(PACKAGE_VERSION[3] == '6', NEW_THRIFT_VERSION_MSG);
static_assert(PACKAGE_VERSION[4] == '.', NEW_THRIFT_VERSION_MSG);
static_assert(PACKAGE_VERSION[5] == '0', NEW_THRIFT_VERSION_MSG);
static_assert(PACKAGE_VERSION[6] == '\0', NEW_THRIFT_VERSION_MSG);

namespace impala {

// The ThriftSerializer uses the DefaultInternalTConfiguration() with the higher limit,
// because this is used on our internal Thrift structures.
ThriftSerializer::ThriftSerializer(bool compact, int initial_buffer_size)
  : mem_buffer_(new TMemoryBuffer(initial_buffer_size, DefaultInternalTConfiguration())) {
  if (compact) {
    TCompactProtocolFactoryT<TMemoryBuffer> factory;
    protocol_ = factory.getProtocol(mem_buffer_);
  } else {
    TBinaryProtocolFactoryT<TMemoryBuffer> factory;
    protocol_ = factory.getProtocol(mem_buffer_);
  }
}

std::shared_ptr<TProtocol> CreateDeserializeProtocol(
    std::shared_ptr<TMemoryBuffer> mem, bool compact) {
  if (compact) {
    TCompactProtocolFactoryT<TMemoryBuffer> tproto_factory;
    return tproto_factory.getProtocol(mem);
  } else {
    TBinaryProtocolFactoryT<TMemoryBuffer> tproto_factory;
    return tproto_factory.getProtocol(mem);
  }
}

void ImpalaTlsSocketFactory::configureCiphers(const string& cipher_list,
    const string& tls_ciphersuites, bool disable_tls12) {
  if (cipher_list.empty() &&
      tls_ciphersuites == kudu::security::SecurityDefaults::kDefaultTlsCipherSuites &&
      !disable_tls12) {
    return;
  }
  if (ctx_.get() == nullptr) {
    throw TSSLException("ImpalaSslSocketFactory was not properly initialized.");
  }
#if OPENSSL_VERSION_NUMBER >= 0x10101000L
  // Disabling TLS 1.2 only makes sense if OpenSSL supports TLS 1.3.
  if (disable_tls12) {
    SCOPED_OPENSSL_NO_PENDING_ERRORS;
    // This is a setting used for testing TLS 1.3 cipher suites.
    LOG(INFO) << "TLS 1.2 is disabled.";
    long options = SSL_CTX_get_options(ctx_->get());
    options |= SSL_OP_NO_TLSv1_2;
    SSL_CTX_set_options(ctx_->get(), options);
  }
  if (tls_ciphersuites != kudu::security::SecurityDefaults::kDefaultTlsCipherSuites) {
    SCOPED_OPENSSL_NO_PENDING_ERRORS;
    if (tls_ciphersuites.empty()) {
      LOG(INFO) << "TLS 1.3 cipher suites are disabled.";
      // If there are no TLS 1.3 cipher suites, disable TLS 1.3. Otherwise, the
      // client/server negotiates TLS 1.3 but then doesn't have any ciphers.
      long options = SSL_CTX_get_options(ctx_->get());
      options |= SSL_OP_NO_TLSv1_3;
      SSL_CTX_set_options(ctx_->get(), options);
    } else {
      LOG(INFO) << "Enabling the following TLS 1.3 cipher suites for the "
                << "ImpalaSslSocketFactory: "
                << tls_ciphersuites;
    }
    int retval = SSL_CTX_set_ciphersuites(ctx_->get(), tls_ciphersuites.c_str());
    const string& openssl_err = kudu::security::GetOpenSSLErrors();
    if (retval <= 0 || !openssl_err.empty()) {
      LOG(INFO) << "SSL_CTX_set_ciphersuites failed: "
                << openssl_err;
      throw TSSLException("SSL_CTX_set_ciphersuites: " + openssl_err);
    }
  }
#endif

  SCOPED_OPENSSL_NO_PENDING_ERRORS;
  if (!cipher_list.empty()) {
    LOG(INFO) << "Enabling the following TLS 1.2 and below ciphers for the "
              << "ImpalaSslSocketFactory: "
              << cipher_list;
    TSSLSocketFactory::ciphers(cipher_list);
  }

  // The following was taken from be/src/kudu/security/tls_context.cc, bugs fixed here
  // may also need to be fixed there.
  // Enable ECDH curves. For OpenSSL 1.1.0 and up, this is done automatically.
#ifndef OPENSSL_NO_ECDH
#if OPENSSL_VERSION_NUMBER < 0x10002000L
  // TODO: OpenSSL 1.0.1 is old. Centos 7.4 and above use 1.0.2. This probably can
  // be removed.
  // OpenSSL 1.0.1 and below only support setting a single ECDH curve at once.
  // We choose prime256v1 because it's the first curve listed in the "modern
  // compatibility" section of the Mozilla Server Side TLS recommendations,
  // accessed Feb. 2017.
  c_unique_ptr<EC_KEY> ecdh{
      EC_KEY_new_by_curve_name(NID_X9_62_prime256v1), &EC_KEY_free};
  if (ecdh == nullptr) {
    throw TSSLException(
        "failed to create prime256v1 curve: " + kudu::security::GetOpenSSLErrors());
  }

  int rc = SSL_CTX_set_tmp_ecdh(ctx_->get(), ecdh.get());
  if (rc <= 0) {
    throw TSSLException(
        "failed to set ECDH curve: " + kudu::security::GetOpenSSLErrors());
  }
#elif OPENSSL_VERSION_NUMBER < 0x10100000L
  // OpenSSL 1.0.2 provides the set_ecdh_auto API which internally figures out
  // the best curve to use.
  int rc = SSL_CTX_set_ecdh_auto(ctx_->get(), 1);
  if (rc <= 0) {
    throw TSSLException(
        "failed to configure ECDH support: " + kudu::security::GetOpenSSLErrors());
  }
#endif
#endif
}

static void ThriftOutputFunction(const char* output) {
  VLOG_QUERY << output;
}

void InitThriftLogging() {
  GlobalOutput.setOutputFunction(ThriftOutputFunction);
}

Status WaitForLocalServer(const ThriftServer& server, int num_retries,
    int retry_interval_ms) {
  return WaitForServer("localhost", server.port(), num_retries, retry_interval_ms);
}

Status WaitForServer(const string& host, int port, int num_retries,
    int retry_interval_ms) {
  int retry_count = 0;
  while (retry_count < num_retries) {
    try {
      TSocket socket(host, port);
      // Timeout is in ms
      socket.setConnTimeout(500);
      socket.open();
      socket.close();
      return Status::OK();
    } catch (const TException& e) {
      VLOG_QUERY << "Connection failed: " << e.what();
    }
    ++retry_count;
    VLOG_QUERY << "Waiting " << retry_interval_ms << "ms for Thrift server at "
               << host << ":" << port
               << " to come up, failed attempt " << retry_count
               << " of " << num_retries;
    SleepForMs(retry_interval_ms);
  }
  return Status("Server did not come up");
}

void PrintTColumnValue(std::ostream& out, const TColumnValue& colval) {
  if (colval.__isset.bool_val) {
    out << ((colval.bool_val) ? "true" : "false");
  } else if (colval.__isset.double_val) {
    out << colval.double_val;
  } else if (colval.__isset.byte_val) {
    out << colval.byte_val;
  } else if (colval.__isset.short_val) {
    out << colval.short_val;
  } else if (colval.__isset.int_val) {
    out << colval.int_val;
  } else if (colval.__isset.long_val) {
    out << colval.long_val;
  } else if (colval.__isset.string_val) {
    out << colval.string_val; // 'string_val' is set for TIMESTAMP and DATE column values.
  } else if (colval.__isset.binary_val) {
    out << colval.binary_val; // Stored as a std::string
  } else {
    out << "NULL";
  }
}

bool IsReadTimeoutTException(const TTransportException& e) {
  // String taken from TSocket::read() Thrift's TSocket.cpp and TSSLSocket.cpp.
  // Specifically, "THRIFT_EAGAIN (timed out)" from TSocket.cpp,
  // and "THRIFT_POLL (timed out)" from TSSLSocket.cpp.
  return (e.getType() == TTransportException::TIMED_OUT
      && strstr(e.what(), "(timed out)") != nullptr);
}

bool IsPeekTimeoutTException(const TTransportException& e) {
  // String taken from TSocket::peek() Thrift's TSocket.cpp and TSSLSocket.cpp.
  return (e.getType() == TTransportException::UNKNOWN
             && strstr(e.what(), "recv(): Resource temporarily unavailable") != nullptr)
      || (e.getType() == TTransportException::TIMED_OUT
             && strstr(e.what(), "THRIFT_POLL (timed out)") != nullptr);
}

bool IsConnResetTException(const TTransportException& e) {
  // Strings taken from TTransport::readAll(). This happens iff TSocket::read() returns 0.
  // As readAll() is reading non-zero length payload, this can only mean recv() called
  // by read() returns 0. According to man page of recv(), this implies a stream socket
  // peer has performed an orderly shutdown.
  return (e.getType() == TTransportException::END_OF_FILE &&
             strstr(e.what(), "No more data to read.") != nullptr) ||
         (e.getType() == TTransportException::INTERNAL_ERROR &&
             strstr(e.what(), "SSL_read: Connection reset by peer") != nullptr);
}

int64_t ThriftInternalRpcMaxMessageSize() {
  return FLAGS_thrift_rpc_max_message_size <= 0 ? ThriftDefaultMaxMessageSize() :
                                                  FLAGS_thrift_rpc_max_message_size;
}

int64_t ThriftExternalRpcMaxMessageSize() {
  return FLAGS_thrift_external_rpc_max_message_size <= 0 ?
      ThriftDefaultMaxMessageSize() : FLAGS_thrift_external_rpc_max_message_size;
}

shared_ptr<TConfiguration> DefaultInternalTConfiguration() {
  return make_shared<TConfiguration>(ThriftInternalRpcMaxMessageSize());
}

shared_ptr<TConfiguration> DefaultExternalTConfiguration() {
  return make_shared<TConfiguration>(ThriftExternalRpcMaxMessageSize());
}

void SetMaxMessageSize(TTransport* transport, int64_t max_message_size) {
  // TODO: Find way to assign TConfiguration through TTransportFactory instead.
  transport->getConfiguration()->setMaxMessageSize(max_message_size);
  transport->updateKnownMessageSize(-1);
  EXPECT_NO_THROW(transport->checkReadBytesAvailable(max_message_size));
}

void VerifyMaxMessageSizeInheritance(TTransport* source, TTransport* dest) {
  // Verify that the source has the max message size set to either the internal
  // limit or the external limit
  int64_t source_max_message_size = source->getConfiguration()->getMaxMessageSize();
  DCHECK(source_max_message_size == ThriftInternalRpcMaxMessageSize() ||
      source_max_message_size == ThriftExternalRpcMaxMessageSize());
  // Verify that the destination transport has the same limit as the source
  DCHECK_EQ(source_max_message_size, dest->getConfiguration()->getMaxMessageSize());
}
}
