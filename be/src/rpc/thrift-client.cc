// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "rpc/thrift-client.h"

#include <boost/assign.hpp>
#include <boost/lexical_cast.hpp>
#include <ostream>
#include <thrift/Thrift.h>
#include <gutil/strings/substitute.h>

#include "util/time.h"

#include "common/names.h"

using namespace apache::thrift::transport;
using namespace apache::thrift;
using namespace strings;

DECLARE_string(ssl_client_ca_certificate);

namespace impala {

Status ThriftClientImpl::Open() {
  if (!socket_create_status_.ok()) return socket_create_status_;
  try {
    if (!transport_->isOpen()) {
      transport_->open();
    }
  } catch (const TException& e) {
    return Status(Substitute("Couldn't open transport for $0 ($1)",
        lexical_cast<string>(address_), e.what()));
  }
  return Status::OK();
}

Status ThriftClientImpl::OpenWithRetry(uint32_t num_tries, uint64_t wait_ms) {
  uint32_t try_count = 0L;
  while (true) {
    ++try_count;
    Status status = Open();
    if (status.ok()) return status;

    LOG(INFO) << "Unable to connect to " << address_;
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
    LOG(INFO) << "Error closing connection to: " << address_ << ", ignoring (" << e.what()
              << ")";
    // Forcibly close the socket (since the transport may have failed to get that far
    // during close())
    try {
      if (socket_.get() != NULL) socket_->close();
    } catch (const TException& e) {
      LOG(INFO) << "Error closing socket to: " << address_ << ", ignoring (" << e.what()
                << ")";
    }
  }
}

Status ThriftClientImpl::CreateSocket() {
  if (!ssl_) {
    socket_.reset(new TSocket(address_.hostname, address_.port));
  } else {
    try {
      ssl_factory_->loadTrustedCertificates(FLAGS_ssl_client_ca_certificate.c_str());
      socket_ = ssl_factory_->createSocket(address_.hostname, address_.port);
    } catch (const TException& e) {
      return Status(Substitute("Failed to create socket: $0", e.what()));
    }
  }

  return Status::OK();
}

}
