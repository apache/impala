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

#include "statestore/failure-detector.h"

#include <boost/assign.hpp>
#include <boost/thread.hpp>

#include "common/logging.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace boost::posix_time;

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
    if (seen) {
      missed_heartbeat_counts_[peer] = 0;
      return OK;
    } else {
      ++missed_heartbeat_counts_[peer];
    }
  }

  return GetPeerState(peer);
}

FailureDetector::PeerState MissedHeartbeatFailureDetector::GetPeerState(
    const string& peer) {
  lock_guard<mutex> l(lock_);
  map<string, int32_t>::iterator heartbeat_record = missed_heartbeat_counts_.find(peer);

  if (heartbeat_record == missed_heartbeat_counts_.end()) {
    return UNKNOWN;
  } else if (heartbeat_record->second > max_missed_heartbeats_) {
    return FAILED;
  } else if (heartbeat_record->second > suspect_missed_heartbeats_) {
    return SUSPECTED;
  }

  return OK;
}

void MissedHeartbeatFailureDetector::EvictPeer(const string& peer) {
  lock_guard<mutex> l(lock_);
  missed_heartbeat_counts_.erase(peer);
}
