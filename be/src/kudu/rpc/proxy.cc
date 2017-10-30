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

#include "kudu/rpc/proxy.h"

#include <boost/bind.hpp>
#include <glog/logging.h>
#include <inttypes.h>
#include <memory>
#include <stdint.h>

#include <iostream>
#include <sstream>
#include <vector>

#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/outbound_call.h"
#include "kudu/rpc/messenger.h"
#include "kudu/rpc/remote_method.h"
#include "kudu/rpc/response_callback.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/net/socket.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/status.h"
#include "kudu/util/user.h"

using google::protobuf::Message;
using std::string;
using std::shared_ptr;

namespace kudu {
namespace rpc {

Proxy::Proxy(std::shared_ptr<Messenger> messenger,
             const Sockaddr& remote,
             string hostname,
             string service_name)
    : service_name_(std::move(service_name)),
      messenger_(std::move(messenger)),
      is_started_(false) {
  CHECK(messenger_ != nullptr);
  DCHECK(!service_name_.empty()) << "Proxy service name must not be blank";

  // By default, we set the real user to the currently logged-in user.
  // Effective user and password remain blank.
  string real_user;
  Status s = GetLoggedInUser(&real_user);
  if (!s.ok()) {
    LOG(WARNING) << "Proxy for " << service_name_ << ": Unable to get logged-in user name: "
        << s.ToString() << " before connecting to remote: " << remote.ToString();
  }

  UserCredentials creds;
  creds.set_real_user(std::move(real_user));
  conn_id_ = ConnectionId(remote, std::move(hostname), std::move(creds));
}

Proxy::~Proxy() {
}

void Proxy::AsyncRequest(const string& method,
                         const google::protobuf::Message& req,
                         google::protobuf::Message* response,
                         RpcController* controller,
                         const ResponseCallback& callback) const {
  CHECK(!controller->call_) << "Controller should be reset";
  base::subtle::NoBarrier_Store(&is_started_, true);
  RemoteMethod remote_method(service_name_, method);
  controller->call_.reset(
      new OutboundCall(conn_id_, remote_method, response, controller, callback));
  controller->SetRequestParam(req);
  controller->SetMessenger(messenger_.get());

  // If this fails to queue, the callback will get called immediately
  // and the controller will be in an ERROR state.
  messenger_->QueueOutboundCall(controller->call_);
}


Status Proxy::SyncRequest(const string& method,
                          const google::protobuf::Message& req,
                          google::protobuf::Message* resp,
                          RpcController* controller) const {
  CountDownLatch latch(1);
  AsyncRequest(method, req, DCHECK_NOTNULL(resp), controller,
               boost::bind(&CountDownLatch::CountDown, boost::ref(latch)));

  latch.Wait();
  return controller->status();
}

void Proxy::set_user_credentials(const UserCredentials& user_credentials) {
  CHECK(base::subtle::NoBarrier_Load(&is_started_) == false)
    << "It is illegal to call set_user_credentials() after request processing has started";
  conn_id_.set_user_credentials(user_credentials);
}

std::string Proxy::ToString() const {
  return strings::Substitute("$0@$1", service_name_, conn_id_.ToString());
}

} // namespace rpc
} // namespace kudu
