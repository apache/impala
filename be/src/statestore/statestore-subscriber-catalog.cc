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

#include "common/logging.h"
#include "statestore/statestore-subscriber-catalog.h"

using namespace impala;

DECLARE_bool(enable_catalogd_ha);
DECLARE_bool(force_catalogd_active);

StatestoreSubscriberCatalog::StatestoreSubscriberCatalog(
    const std::string& subscriber_id,
    const TNetworkAddress& heartbeat_address,
    const TNetworkAddress& statestore_address,
    const TNetworkAddress& statestore2_address,
    MetricGroup* metrics,
    CatalogServiceVersion::type catalog_protocol_version,
    const TNetworkAddress& catalogd_address)
  : StatestoreSubscriber(subscriber_id, heartbeat_address, statestore_address,
        statestore2_address, metrics, TStatestoreSubscriberType::CATALOGD) {
  catalogd_registration_.__set_protocol(catalog_protocol_version);
  catalogd_registration_.__set_address(catalogd_address);
  catalogd_registration_.__set_enable_catalogd_ha(FLAGS_enable_catalogd_ha);
  catalogd_registration_.__set_force_catalogd_active(FLAGS_force_catalogd_active);
}

bool ActiveCatalogdVersionChecker::CheckActiveCatalogdVersion(
    bool is_registration_reply, int64 active_catalogd_version) {
  if (is_registration_reply) {
    last_update_for_registration_ = true;
    if (active_catalogd_version < 0) {
      // Reset the version of last received active catalogd.
      last_active_catalogd_version_ = 0;
      return false;
    }
  } else {
    if (last_active_catalogd_version_ >= active_catalogd_version) {
      if (last_update_for_registration_
          && last_active_catalogd_version_ == active_catalogd_version) {
        VLOG(3) << "Duplicated update of active catalogd";
      } else {
        LOG(INFO) << "Ignore the update of active catalogd since more recent update has "
                  << "been processed (" << last_active_catalogd_version_ << " vs "
                  << active_catalogd_version << ")";
      }
      last_update_for_registration_ = false;
      return false;
    }
    last_update_for_registration_ = false;
  }
  DCHECK(active_catalogd_version >= 0);
  last_active_catalogd_version_ = active_catalogd_version;
  return true;
}
