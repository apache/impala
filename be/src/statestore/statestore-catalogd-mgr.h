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

#include <mutex>
#include <string>

#include "common/status.h"
#include "gen-cpp/StatestoreService_types.h"

namespace impala {

class Status;

/// A randomly generated number which is assigned to subscribers for each registration.
typedef TUniqueId RegistrationId;

/// A SubscriberId uniquely identifies a single subscriber, and is provided by the
/// subscriber at registration time.
typedef std::string SubscriberId;

/// StatestoreCatalogdMgr:
/// Tracks variety of bookkeeping information for Catalog daemon on statestore.
/// It manages the designation of the active catalogd when CatalogD High Availability is
/// enabled.
class StatestoreCatalogdMgr {
 public:
  StatestoreCatalogdMgr(bool enable_catalogd_ha)
    : enable_catalogd_ha_(enable_catalogd_ha),
      is_active_catalogd_assigned_(false),
      num_registered_catalogd_(0),
      first_catalogd_register_time_(0),
      active_catalogd_version_(0L),
      last_update_catalogd_time_(0L) {}

  /// Register one catalogd.
  /// Return true if new active catalogd is designated during this registration.
  /// Note that the active role could be designated to the peer catalogd.
  bool RegisterCatalogd(bool is_reregistering, const SubscriberId& subscriber_id,
      const RegistrationId& registration_id,
      const TCatalogRegistration& catalogd_registration);

  /// Return true if active catalogd is already designated. Otherwise check if preemption
  /// waiting period is expired. If the period is expired, assign the first registered
  /// catalogd in active role and return true.
  bool CheckActiveCatalog();

  /// Unregister one catalogd. If it's active and there is a standby catalogd, fail over
  /// the catalog service to standby catalogd.
  /// Return true if failover of catalog service has happened in this call.
  bool UnregisterCatalogd(const SubscriberId& unregistered_subscriber_id);

  /// Return the protocol version of catalog service and address of active catalogd.
  /// Set *has_active_catalogd as false if the active one is not designated yet.
  const TCatalogRegistration& GetActiveCatalogRegistration(
      bool* has_active_catalogd, int64_t* active_catalogd_version);

  /// Return the subscriber-id of active catalogd.
  /// This function should be called after the active catalogd is designated.
  const SubscriberId& GetActiveCatalogdSubscriberId();

  /// Return the protocol version of catalog service and address of standby catalogd.
  const TCatalogRegistration& GetStandbyCatalogRegistration();

  /// Check if the subscriber with given subscriber_id is active catalogd.
  bool IsActiveCatalogd(const SubscriberId&subscriber_id);

  /// Return the mutex lock.
  std::mutex* GetLock() { return &catalog_mgr_lock_; }

  /// Return the last time when the catalogd is updated.
  int64_t GetLastUpdateCatalogTime();

 private:
  /// Protect all member variables.
  std::mutex catalog_mgr_lock_;

  /// Set to true if CatalogD HA is enabled.
  bool enable_catalogd_ha_;

  /// Indicate if the active catalogd has been assigned.
  bool is_active_catalogd_assigned_;

  /// Number of registered catalogd.
  int num_registered_catalogd_;

  /// The registering time of first catalogd.
  int64_t first_catalogd_register_time_;
  /// subscriber_id of the first registered catalogd
  SubscriberId first_catalogd_subscriber_id_;
  /// RegistrationId of the first registered catalogd
  RegistrationId first_catalogd_registration_id_;
  /// Additional registration info of first registered catalogd.
  TCatalogRegistration first_catalogd_registration_;

  /// subscriber_id of the active catalogd
  SubscriberId active_catalogd_subscriber_id_;
  /// RegistrationId of the active catalogd
  RegistrationId active_catalogd_registration_id_;
  /// Additional registration info of activ catalogd
  TCatalogRegistration active_catalogd_registration_;

  /// Following three variables are only valid if num_registered_catalogd_ == 2.
  /// subscriber_id of the standby catalogd
  SubscriberId standby_catalogd_subscriber_id_;
  /// RegistrationId of the standby catalogd
  RegistrationId standby_catalogd_registration_id_;
  /// Additional registration info of standby catalogd
  TCatalogRegistration standby_catalogd_registration_;

  /// Monotonically increasing version number. The value is increased when a new active
  /// catalogd is designated.
  int64_t active_catalogd_version_;

  /// The time is updated when a new active catalogd is designated.
  int64_t last_update_catalogd_time_;
};

} // namespace impala
