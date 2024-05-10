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

#include "rpc/thrift-client.h"

#include <boost/assign.hpp>
#include <boost/lexical_cast.hpp>
#include <ostream>
#include <thrift/Thrift.h>
#include <gutil/strings/substitute.h>

#include "rpc/thrift-util.h"
#include "util/network-util.h"
#include "util/openssl-util.h"
#include "util/time.h"

#include "common/names.h"

using namespace apache::thrift::transport;
using namespace apache::thrift;
using namespace strings;

DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);
DECLARE_string(tls_ciphersuites);

namespace impala {

ThriftClientImpl::ThriftClientImpl(const std::string& ipaddress, int port, bool ssl,
    bool disable_tls12)
  : address_(MakeNetworkAddress(ipaddress, port)), ssl_(ssl),
    disable_tls12_(disable_tls12) {
  if (ssl_) {
    SSLProtocol version;
    init_status_ =
        SSLProtoVersions::StringToProtocol(FLAGS_ssl_minimum_version, &version);
    if (init_status_.ok() && !SSLProtoVersions::IsSupported(version)) {
      string err =
          Substitute("TLS ($0) version not supported (maximum supported version is $1)",
              version, MaxSupportedTlsVersion());
      init_status_ = Status(err);
    }
    if (!init_status_.ok()) return;
    ssl_factory_.reset(new ImpalaTlsSocketFactory(version));
  }
  init_status_ = CreateSocket();
}

Status ThriftClientImpl::Open() {
  RETURN_IF_ERROR(init_status_);
  try {
    if (!transport_->isOpen()) {
      transport_->open();
    }
  } catch (const TException& e) {
    try {
      transport_->close();
    } catch (const TException& e) {
      VLOG(1) << "Error closing socket to: " << TNetworkAddressToString(address_)
              << ", ignoring (" << e.what() << ")";
    }
    // In certain cases in which the remote host is overloaded, this failure can
    // happen quite frequently. Let's print this error message without the stack
    // trace as there aren't many callers of this function.
    const string& err_msg = Substitute("Couldn't open transport for $0 ($1)",
        TNetworkAddressToString(address_), e.what());
    VLOG(1) << err_msg;
    return Status::Expected(err_msg);
  }
  return Status::OK();
}

Status ThriftClientImpl::OpenWithRetry(uint32_t num_tries, uint64_t wait_ms) {
  // Socket creation failures are not recoverable.
  RETURN_IF_ERROR(init_status_);

  uint32_t try_count = 0L;
  while (true) {
    ++try_count;
    Status status = Open();
    if (status.ok()) return status;

    LOG(INFO) << "Unable to connect to " << TNetworkAddressToString(address_);
    if (num_tries == 0) {
      LOG(INFO) << "(Attempt " << try_count << ", will retry indefinitely)";
    } else {
      if (num_tries != 1) {
        // No point logging 'attempt 1 of 1'
        LOG(INFO) << "(Attempt " << try_count << " of " << num_tries << ")";
      }
      if (try_count == num_tries) return status;
    }
    SleepForMs(wait_ms);
  }
}

void ThriftClientImpl::Close() {
  try {
    if (transport_.get() != NULL && transport_->isOpen()) transport_->close();
  } catch (const TException& e) {
    LOG(INFO) << "Error closing connection to: " << TNetworkAddressToString(address_)
              << ", ignoring (" << e.what() << ")";
    // Forcibly close the socket (since the transport may have failed to get that far
    // during close())
    try {
      if (socket_.get() != NULL) socket_->close();
    } catch (const TException& e) {
      LOG(INFO) << "Error closing socket to: " << TNetworkAddressToString(address_)
                << ", ignoring (" << e.what() << ")";
    }
  }
}

Status ThriftClientImpl::CreateSocket() {
  if (!ssl_) {
    socket_.reset(new TSocket(address_.hostname, address_.port));
  } else {
    try {
      ssl_factory_->configureCiphers(FLAGS_ssl_cipher_list, FLAGS_tls_ciphersuites,
          disable_tls12_);
      ssl_factory_->loadTrustedCertificates(FLAGS_ssl_client_ca_certificate.c_str());
      socket_ = ssl_factory_->createSocket(address_.hostname, address_.port);
    } catch (const TException& e) {
      return Status(TErrorCode::SSL_SOCKET_CREATION_FAILED, e.what());
    }
  }
  if (socket_ != nullptr) {
    // ThriftClient is used for internal cluster communication, so we use
    // ThriftInternalRpcMaxMessageSize().
    SetMaxMessageSize(socket_.get(), ThriftInternalRpcMaxMessageSize());
  }

  return Status::OK();
}

}
