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

#include <util/thrift-client.h>
#include <boost/assign.hpp>
#include <ostream>

using namespace std;
using namespace boost;
using namespace apache::thrift::transport;

namespace impala {

Status ThriftClientImpl::Open() {
  try {
    if (!transport_->isOpen()) {
      transport_->open();
    }
  } catch (TTransportException& e) {
    stringstream msg;
    msg << "Couldn't open transport for " << ipaddress() << ":" << port()
        << "(" << e.what() << ")";
    return impala::Status(msg.str());
  }
  return impala::Status::OK;
}

Status ThriftClientImpl::OpenWithRetry(
    int num_tries, int wait_ms) {
  DCHECK_GE(wait_ms, 0);
  Status status;
  int try_count = 0L;
  while (num_tries <= 0 || try_count < num_tries) {
    ++try_count;
    status = Open();
    if (status.ok()) return status;
    LOG(INFO) << "Unable to connect to " << ipaddress_ << ":" << port_;
    if (num_tries < 0) {
      LOG(INFO) << "(Attempt " << try_count << ", will retry indefinitely)";
    } else {
      LOG(INFO) << "(Attempt " << try_count << " of " << num_tries << ")";
    }
    usleep(wait_ms * 1000L);
  }

  return status;
}

Status ThriftClientImpl::Close() {
  if (transport_->isOpen()) {
    transport_->close();
  }
  return Status::OK;
}

}
