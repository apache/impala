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

#include <map>
#include <mutex>
#include <string>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/thread/thread_time.hpp>

namespace impala {

/// A failure detector tracks the liveness of a set of peers which is computed as
/// a function of received 'heartbeat' signals.
/// A peer may be in one of four states:
///   FAILED -> the peer is assumed to be failed
///   SUSPECTED -> the peer may have failed, and may be moved to the FAILED state shortly.
///   OK -> the peer is apparently healthy and sending regular heartbeats
///   UNKNOWN -> nothing has been heard about the peer
class FailureDetector {
 public:
  enum PeerState {
    FAILED = 0,
    SUSPECTED = 1,
    OK = 2,
    UNKNOWN = 3
  };

  virtual ~FailureDetector() {}

  /// Updates the state of a peer according to the most recent heartbeat
  /// state. If 'seen' is true, this method indicates that a heartbeat has been
  /// received. If seen is 'false', this method indicates that a heartbeat has
  /// not been received since the last successful heartbeat receipt.
  //
  /// This method returns the current state of the updated peer. Note that this may be
  /// different from the state implied by seen - a single missed heartbeat, for example, may
  /// not cause a peer to enter the SUSPECTED or FAILED states. The failure detector has
  /// the benefit of looking at all samples, rather than just the most recent one.
  virtual PeerState UpdateHeartbeat(const std::string& peer, bool seen) = 0;

  /// Returns the current estimated state of a peer.
  virtual PeerState GetPeerState(const std::string& peer) = 0;

  /// Remove a peer from the failure detector completely.
  virtual void EvictPeer(const std::string& peer) = 0;

  /// Utility method to convert a PeerState enum to a string.
  static const std::string& PeerStateToString(PeerState peer_state);
};

/// A failure detector based on a maximum time between successful heartbeats.
/// Peers that do not successfully heartbeat before the failure_timeout are
/// considered failed; those that do not heartbeat before the suspect_timeout are
/// considered suspected.
/// A timeout failure detector is most appropriate for a client which is
/// receiving periodic heartbeats.
/// Not thread safe.
class TimeoutFailureDetector : public FailureDetector {
 public:
  TimeoutFailureDetector(boost::posix_time::time_duration failure_timeout,
      boost::posix_time::time_duration suspect_timeout)
      : failure_timeout_(failure_timeout),
        suspect_timeout_(suspect_timeout) { }

  virtual PeerState UpdateHeartbeat(const std::string& peer, bool seen);

  virtual PeerState GetPeerState(const std::string& peer);

  virtual void EvictPeer(const std::string& peer);

 private:
  /// Protects all members
  std::mutex lock_;

  /// Record of last time a successful heartbeat was received
  std::map<std::string, boost::system_time> peer_heartbeat_record_;

  /// The maximum time that may elapse without a heartbeat before a peer is
  /// considered failed
  const boost::posix_time::time_duration failure_timeout_;

  /// The maximum time that may elapse without a heartbeat before a peer is
  /// suspected of failure
  const boost::posix_time::time_duration suspect_timeout_;
};

/// A failure detector based on a maximum number of consecutive heartbeats missed
/// before a peer is considered failed. Clients must call
/// UpdateHeartbeat(..., false) to indicate that a heartbeat has been missed.
/// The MissedHeartbeatFailureDetector is most appropriate when heartbeats are
/// being sent, not being received, because it is easy in that situation to tell
/// when a heartbeat has not been successful.
/// Not thread safe.
class MissedHeartbeatFailureDetector : public FailureDetector {
 public:
  /// max_missed_heartbeats -> the number of heartbeats that can be missed before a
  /// peer is considered failed.
  /// suspect_missed_heartbeats -> the number of heartbeats that can be missed before a
  /// peer is suspected of failure.
  MissedHeartbeatFailureDetector(int32_t max_missed_heartbeats,
      int32_t suspect_missed_heartbeats)
    : max_missed_heartbeats_(max_missed_heartbeats),
      suspect_missed_heartbeats_(suspect_missed_heartbeats) { }

  virtual PeerState UpdateHeartbeat(const std::string& peer, bool seen);

  virtual PeerState GetPeerState(const std::string& peer);

  virtual void EvictPeer(const std::string& peer);

 private:
  /// Computes the PeerState from the missed heartbeat count.
  PeerState ComputePeerState(int32_t missed_heatbeat_count);

  /// Protects all members
  std::mutex lock_;

  /// The maximum number of heartbeats that can be missed consecutively before a
  /// peer is considered failed.
  int32_t max_missed_heartbeats_;

  /// The maximum number of heartbeats that can be missed consecutively before a
  /// peer is suspected of failure.
  int32_t suspect_missed_heartbeats_;

  /// Number of consecutive heartbeats missed by peer.
  std::map<std::string, int32_t> missed_heartbeat_counts_;
};
}
