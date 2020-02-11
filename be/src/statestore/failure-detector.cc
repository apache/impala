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

#include "statestore/failure-detector.h"

#include <mutex>

#include <boost/assign.hpp>

#include "common/logging.h"

#include "common/names.h"

using boost::get_system_time;
using boost::posix_time::time_duration;
using boost::system_time;
using namespace impala;

static const map<FailureDetector::PeerState, string> PEER_STATE_TO_STRING =
    boost::assign::map_list_of
      (FailureDetector::OK, "OK")
      (FailureDetector::SUSPECTED, "SUSPECTED")
      (FailureDetector::UNKNOWN, "UNKNOWN")
      (FailureDetector::FAILED, "FAILED");

const string& FailureDetector::PeerStateToString(FailureDetector::PeerState peer_state) {
  map<FailureDetector::PeerState, string>::const_iterator it =
      PEER_STATE_TO_STRING.find(peer_state);
  DCHECK(it != PEER_STATE_TO_STRING.end());
  return it->second;
}

FailureDetector::PeerState TimeoutFailureDetector::UpdateHeartbeat(
    const string& peer, bool seen) {
  {
    lock_guard<mutex> l(lock_);
    if (seen) peer_heartbeat_record_[peer] = get_system_time();
  }
  return GetPeerState(peer);
}

FailureDetector::PeerState TimeoutFailureDetector::GetPeerState(const string& peer) {
  lock_guard<mutex> l(lock_);
  map<string, system_time>::iterator heartbeat_record = peer_heartbeat_record_.find(peer);
  if (heartbeat_record == peer_heartbeat_record_.end()) return UNKNOWN;

  time_duration duration = get_system_time() - heartbeat_record->second;
  if (duration > failure_timeout_) {
    return FAILED;
  } else if (duration > suspect_timeout_) {
    return SUSPECTED;
  }

  return OK;
}

void TimeoutFailureDetector::EvictPeer(const string& peer) {
  lock_guard<mutex> l(lock_);
  peer_heartbeat_record_.erase(peer);
}

FailureDetector::PeerState MissedHeartbeatFailureDetector::UpdateHeartbeat(
    const string& peer, bool seen) {
  {
    lock_guard<mutex> l(lock_);
    int32_t* missed_heartbeat_count = &missed_heartbeat_counts_[peer];
    if (seen) {
      if (*missed_heartbeat_count != 0) {
        LOG(INFO) << "Heartbeat for '" << peer << "' succeeded after "
                  << *missed_heartbeat_count << " missed heartbeats. "
                  << "Resetting missed heartbeat count.";
        *missed_heartbeat_count = 0;
      }
      return OK;
    } else {
      ++(*missed_heartbeat_count);
      LOG(INFO) << *missed_heartbeat_count << " consecutive heartbeats failed for "
                << "'" << peer << "'. State is "
                << PeerStateToString(ComputePeerState(*missed_heartbeat_count));
    }
  }
  return GetPeerState(peer);
}

FailureDetector::PeerState MissedHeartbeatFailureDetector::GetPeerState(
    const string& peer) {
  lock_guard<mutex> l(lock_);
  auto it = missed_heartbeat_counts_.find(peer);
  if (it == missed_heartbeat_counts_.end()) return UNKNOWN;
  return ComputePeerState(it->second);
}

FailureDetector::PeerState MissedHeartbeatFailureDetector::ComputePeerState(
    int32_t missed_heatbeat_count) {
  if (missed_heatbeat_count >= max_missed_heartbeats_) {
    return FAILED;
  } else if (missed_heatbeat_count >= suspect_missed_heartbeats_) {
    return SUSPECTED;
  }
  return OK;
}

void MissedHeartbeatFailureDetector::EvictPeer(const string& peer) {
  lock_guard<mutex> l(lock_);
  missed_heartbeat_counts_.erase(peer);
}
