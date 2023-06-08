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

#pragma once

#include "statestore/statestore-subscriber.h"

namespace impala {

/// Statestore subscriber for Catalog service.
/// Catalog-specific parameters for statestore registration.
class StatestoreSubscriberCatalog : public StatestoreSubscriber {
 public:
  /// Only constructor.
  ///   subscriber_id - should be unique across the cluster, identifies this subscriber
  ///   heartbeat_address - the local address on which the heartbeat service which
  ///                       communicates with the statestore should be started.
  ///   statestore_address - the address of the statestore to register with
  ///   catalogd_address - address of catalog service.
  ///   catalog_protocol_version - protocol version of Catalog service.
  StatestoreSubscriberCatalog(const std::string& subscriber_id,
      const TNetworkAddress& heartbeat_address,
      const TNetworkAddress& statestore_address,
      MetricGroup* metrics,
      CatalogServiceVersion::type catalog_protocol_version,
      const TNetworkAddress& catalogd_address);

  virtual ~StatestoreSubscriberCatalog() {}

  /// Set Register Request
  virtual Status SetRegisterRequest(TRegisterSubscriberRequest* request) {
    request->__set_catalogd_registration(catalogd_registration_);
    return StatestoreSubscriber::SetRegisterRequest(request);
  }

 private:
  /// Additional registration info for catalog daemon.
  TCatalogRegistration catalogd_registration_;
};

}
