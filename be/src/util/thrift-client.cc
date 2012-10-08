// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <util/thrift-client.h>
#include <boost/assign.hpp>

using namespace std;
using namespace boost;

namespace impala {

Status ThriftClientImpl::Open() {
  try {
    if (!transport_->isOpen()) {
      transport_->open();
    }
  } catch (apache::thrift::transport::TTransportException& e) {
    std::stringstream msg;
    msg << "Couldn't open transport for " << ipaddress() << ":" << port()
        << "(" << e.what() << ")";
    return impala::Status(msg.str());
  }
  return impala::Status::OK;
}

Status ThriftClientImpl::OpenWithRetry(
    int num_retries, int wait_ms) {
  DCHECK_GT(num_retries, 0);
  DCHECK_GE(wait_ms, 0);
  Status status;
  for (int i = 0; i < num_retries; ++i) {
    status = Open();
    if (status.ok()) return status;
    LOG(INFO) << "Unable to connect to " << ipaddress_ << ":" << port_ 
              << " (Attempt " << i + 1 << " of " << num_retries << ")";
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
