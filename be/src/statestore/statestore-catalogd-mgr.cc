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

#include "statestore/statestore-catalogd-mgr.h"

#include "gen-cpp/Types_types.h"
#include "util/container-util.h"
#include "util/time.h"

using namespace impala;

DECLARE_bool(use_subscriber_id_as_catalogd_priority);
DECLARE_int64(catalogd_ha_preemption_wait_period_ms);

#define COPY_CATALOGD_REGISTRATION_FROM_MEMBER_VARIABLES(NAME1, NAME2)     \
  do {                                                                     \
    NAME1##_catalogd_subscriber_id_ = NAME2##_catalogd_subscriber_id_;     \
    NAME1##_catalogd_registration_id_ = NAME2##_catalogd_registration_id_; \
    NAME1##_catalogd_registration_ = NAME2##_catalogd_registration_;       \
  } while (false)

#define COPY_CATALOGD_REGISTRATION_FROM_LOCAL_VARIABLES(NAME) \
  do {                                                        \
    NAME##_catalogd_subscriber_id_ = subscriber_id;           \
    NAME##_catalogd_registration_id_ = registration_id;       \
    NAME##_catalogd_registration_ = catalogd_registration;    \
  } while (false)

#define RESET_CATALOGD_REGISTRATION_MEMBER_VARIABLES(NAME)    \
  do {                                                        \
    NAME##_catalogd_subscriber_id_ = "";                      \
    NAME##_catalogd_registration_id_ = TUniqueId();           \
    NAME##_catalogd_registration_ = TCatalogRegistration();   \
  } while (false)

bool StatestoreCatalogdMgr::RegisterCatalogd(bool is_reregistering,
    const SubscriberId& subscriber_id,
    const RegistrationId& registration_id,
    const TCatalogRegistration& catalogd_registration) {
  std::lock_guard<std::mutex> l(catalog_mgr_lock_);
  if (!enable_catalogd_ha_) {
    // CatalogD HA is not enabled.
    if (!is_reregistering) num_registered_catalogd_++;
    DCHECK(num_registered_catalogd_ < 2);
    is_active_catalogd_assigned_ = true;
    COPY_CATALOGD_REGISTRATION_FROM_LOCAL_VARIABLES(active);
    ++active_catalogd_version_;
    last_update_catalogd_time_ = UnixMillis();
    return true;
  }

  if (is_reregistering) {
    if (num_registered_catalogd_ == 2) {
      DCHECK(is_active_catalogd_assigned_);
      if (subscriber_id == standby_catalogd_subscriber_id_
          && catalogd_registration.force_catalogd_active) {
        // Re-register standby catalogd as active one.
        COPY_CATALOGD_REGISTRATION_FROM_MEMBER_VARIABLES(standby, active);
        COPY_CATALOGD_REGISTRATION_FROM_LOCAL_VARIABLES(active);
        LOG(INFO) << active_catalogd_subscriber_id_
                  << " is re-registered with FLAGS_force_catalogd_active.";
        ++active_catalogd_version_;
        last_update_catalogd_time_ = UnixMillis();
        return true;
      }
    } else {
      DCHECK(num_registered_catalogd_ == 1 && first_catalogd_register_time_ != 0);
      if (!is_active_catalogd_assigned_
          && (MonotonicMillis() - first_catalogd_register_time_
              >= FLAGS_catalogd_ha_preemption_wait_period_ms)) {
        is_active_catalogd_assigned_ = true;
        COPY_CATALOGD_REGISTRATION_FROM_LOCAL_VARIABLES(active);
        LOG(INFO) << active_catalogd_subscriber_id_
                  << " is re-registered after HA preemption waiting period and "
                  << "is assigned as active catalogd.";
        ++active_catalogd_version_;
        last_update_catalogd_time_ = UnixMillis();
        return true;
      }
    }
    // There is no role change during re-registration.
    VLOG(3) << subscriber_id << " is re-registered, but there is no role change.";
    return false;
  }

  if (num_registered_catalogd_ == 0) {
    DCHECK(!is_active_catalogd_assigned_);
    // First catalogd is registered.
    num_registered_catalogd_++;
    COPY_CATALOGD_REGISTRATION_FROM_LOCAL_VARIABLES(first);
    bool is_waiting_period_expired = false;
    if (first_catalogd_register_time_ == 0) {
      first_catalogd_register_time_ = MonotonicMillis();
      if (FLAGS_catalogd_ha_preemption_wait_period_ms == 0) {
        is_waiting_period_expired = true;
      }
    } else if (MonotonicMillis() - first_catalogd_register_time_ >=
        FLAGS_catalogd_ha_preemption_wait_period_ms) {
      is_waiting_period_expired = true;
    }
    if (catalogd_registration.force_catalogd_active || is_waiting_period_expired) {
      // Don't need to wait second catalogd if force_catalogd_active is true or the
      // waiting period is expired.
      is_active_catalogd_assigned_ = true;
      COPY_CATALOGD_REGISTRATION_FROM_LOCAL_VARIABLES(active);
      LOG(INFO) << active_catalogd_subscriber_id_ << " is assigned as active catalogd.";
      ++active_catalogd_version_;
      last_update_catalogd_time_ = UnixMillis();
      return true;
    }
    // Wait second catalogd to be registered.
    VLOG(3) << "Wait second catalogd to be registered during HA preemption waiting "
            << "period.";
  } else {
    num_registered_catalogd_++;
    DCHECK(num_registered_catalogd_ == 2)
        << "No more than 2 CatalogD registrations are allowed!";
    if (catalogd_registration.force_catalogd_active) {
      // Force to set the current one as active catalogd
      if (is_active_catalogd_assigned_) {
        COPY_CATALOGD_REGISTRATION_FROM_MEMBER_VARIABLES(standby, active);
      } else {
        COPY_CATALOGD_REGISTRATION_FROM_MEMBER_VARIABLES(standby, first);
      }
      is_active_catalogd_assigned_ = true;
      COPY_CATALOGD_REGISTRATION_FROM_LOCAL_VARIABLES(active);
      LOG(INFO) << active_catalogd_subscriber_id_
                << " is registered with FLAGS_force_catalogd_active and is assigned as "
                << "active catalogd.";
      ++active_catalogd_version_;
      last_update_catalogd_time_ = UnixMillis();
      return true;
    } else if (is_active_catalogd_assigned_) {
      // Existing one is already assigned as active catalogd.
      COPY_CATALOGD_REGISTRATION_FROM_LOCAL_VARIABLES(standby);
      VLOG(3) << "There is another catalogd already assigned as active catalogd.";
    } else {
      // Compare priority and assign the catalogd with high priority as active catalogd.
      is_active_catalogd_assigned_ = true;
      bool first_has_high_priority = FLAGS_use_subscriber_id_as_catalogd_priority
          ? first_catalogd_subscriber_id_ < subscriber_id
          : first_catalogd_registration_id_ < registration_id;
      if (first_has_high_priority) {
        COPY_CATALOGD_REGISTRATION_FROM_MEMBER_VARIABLES(active, first);
        COPY_CATALOGD_REGISTRATION_FROM_LOCAL_VARIABLES(standby);
      } else {
        COPY_CATALOGD_REGISTRATION_FROM_LOCAL_VARIABLES(active);
        COPY_CATALOGD_REGISTRATION_FROM_MEMBER_VARIABLES(standby, first);
      }
      LOG(INFO) << active_catalogd_subscriber_id_
                << " has higher priority and is assigned as active catalogd.";
      ++active_catalogd_version_;
      last_update_catalogd_time_ = UnixMillis();
      return true;
    }
  }
  return false;
}

bool StatestoreCatalogdMgr::CheckActiveCatalog() {
  std::lock_guard<std::mutex> l(catalog_mgr_lock_);
  if (is_active_catalogd_assigned_) {
    return true;
  } else if (num_registered_catalogd_ == 0
      || first_catalogd_register_time_ == 0
      || (MonotonicMillis() - first_catalogd_register_time_ <
          FLAGS_catalogd_ha_preemption_wait_period_ms)) {
    return false;
  }
  // Assign the first registered catalogd as active one.
  DCHECK(num_registered_catalogd_ == 1);
  is_active_catalogd_assigned_ = true;
  COPY_CATALOGD_REGISTRATION_FROM_MEMBER_VARIABLES(active, first);
  LOG(INFO) << active_catalogd_subscriber_id_
            << " is assigned as active catalogd after preemption waiting period.";
  ++active_catalogd_version_;
  last_update_catalogd_time_ = UnixMillis();
  return true;
}

bool StatestoreCatalogdMgr::UnregisterCatalogd(
    const SubscriberId& unregistered_subscriber_id) {
  std::lock_guard<std::mutex> l(catalog_mgr_lock_);
  num_registered_catalogd_--;
  if (unregistered_subscriber_id == active_catalogd_subscriber_id_) {
    // Unregister active catalogd.
    DCHECK(is_active_catalogd_assigned_);
    if (num_registered_catalogd_ > 0) {
      // Fail over to standby catalogd
      COPY_CATALOGD_REGISTRATION_FROM_MEMBER_VARIABLES(active, standby);
      RESET_CATALOGD_REGISTRATION_MEMBER_VARIABLES(standby);
      LOG(INFO) << "Fail over active catalogd to " << active_catalogd_subscriber_id_;
      last_update_catalogd_time_ = UnixMillis();
      ++active_catalogd_version_;
      return true;
    } else {
      is_active_catalogd_assigned_ = false;
      // Don't need to wait second one to be registered.
      first_catalogd_register_time_ = MonotonicMillis() -
          FLAGS_catalogd_ha_preemption_wait_period_ms -1;
      LOG(INFO) << "No active catalogd available in the cluster";
    }
  } else if (num_registered_catalogd_ > 0) {
    // Unregister standby catalogd.
    DCHECK(unregistered_subscriber_id == standby_catalogd_subscriber_id_);
    RESET_CATALOGD_REGISTRATION_MEMBER_VARIABLES(standby);
    VLOG(3) << "Unregister standby catalogd " << unregistered_subscriber_id;
  } else {
    // Active catalogd has not been designated.
    DCHECK(!is_active_catalogd_assigned_);
  }
  return false;
}

const TCatalogRegistration& StatestoreCatalogdMgr::GetActiveCatalogRegistration(
    bool* has_active_catalogd, int64_t* active_catalogd_version) {
  std::lock_guard<std::mutex> l(catalog_mgr_lock_);
  *has_active_catalogd = is_active_catalogd_assigned_;
  *active_catalogd_version = active_catalogd_version_;
  return active_catalogd_registration_;
}

const TCatalogRegistration& StatestoreCatalogdMgr::GetStandbyCatalogRegistration() {
  std::lock_guard<std::mutex> l(catalog_mgr_lock_);
  return standby_catalogd_registration_;
}

const SubscriberId& StatestoreCatalogdMgr::GetActiveCatalogdSubscriberId() {
  std::lock_guard<std::mutex> l(catalog_mgr_lock_);
  return active_catalogd_subscriber_id_;
}

bool StatestoreCatalogdMgr::IsActiveCatalogd(const SubscriberId& subscriber_id) {
  std::lock_guard<std::mutex> l(catalog_mgr_lock_);
  return active_catalogd_subscriber_id_ == subscriber_id;
}

int64_t StatestoreCatalogdMgr::GetLastUpdateCatalogTime() {
  std::lock_guard<std::mutex> l(catalog_mgr_lock_);
  return last_update_catalogd_time_;
}
