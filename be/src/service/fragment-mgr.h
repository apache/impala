// Copyright 2014 Cloudera Inc.
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

#ifndef IMPALA_SERVICE_FRAGMENT_MGR_H
#define IMPALA_SERVICE_FRAGMENT_MGR_H

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>

#include "gen-cpp/ImpalaInternalService.h"
#include "common/status.h"
#include "util/spinlock.h"

namespace impala {

/// Manages execution of individual plan fragment instances, which are typically run as a
/// result of ExecPlanFragment() RPCs that arrive via the internal Impala interface.
//
/// A fragment is started in ExecPlanFragment(), which starts a thread which runs
/// FragmentThread() asynchronously and returns. Fragments are Prepare()'d in that thread,
/// and then Exec() is called. At any point a fragment may terminate either by
/// cancellation via CancelPlanFragment(), or when FragmentThread() returns.
//
/// TODO: Remove Thrift args from methods where it would improve readability;
/// ImpalaInternalService can take care of translation to / from Thrift, as it already
/// does for ExecPlanFragment()'s return value.
class FragmentMgr {
 public:
  /// Registers a new FragmentExecState and launches the thread that calls Prepare() and
  /// Exec() on it.
  ///
  /// Returns an error if there was some unrecoverable problem before the fragment was
  /// registered (like low memory). Otherwise, returns OK, which guarantees that the
  /// fragment is registered and must either run to completion or be cancelled.
  ///
  /// After this call returns, it is legal to call CancelPlanFragment() on this
  /// fragment. If this call returns an error, CancelPlanFragment() will be a no-op
  /// (because the fragment is unregistered).
  Status ExecPlanFragment(const TExecPlanFragmentParams& params);

  /// Cancels a plan fragment that is running asynchronously.
  void CancelPlanFragment(TCancelPlanFragmentResult& return_val,
      const TCancelPlanFragmentParams& params);

  class FragmentExecState;

  /// Returns a shared pointer to the FragmentExecState if one can be found for the
  /// given id. If the id is not found, the shared pointer will contain NULL.
  boost::shared_ptr<FragmentExecState> GetFragmentExecState(
      const TUniqueId& fragment_instance_id);

 private:
  /// protects fragment_exec_state_map_
  SpinLock fragment_exec_state_map_lock_;

  /// Map from fragment instance id to exec state; FragmentExecState is owned by us and
  /// referenced as a shared_ptr to allow asynchronous calls to CancelPlanFragment()
  typedef boost::unordered_map<TUniqueId, boost::shared_ptr<FragmentExecState> >
  FragmentExecStateMap;
  FragmentExecStateMap fragment_exec_state_map_;

  /// Retrieves the 'exec_state' corresponding to fragment_instance_id. Calls
  /// exec_state->Prepare() and then exec_state->Exec(). Finally unregisters the
  /// exec_state from fragment_exec_state_map_. The exec_state must previously have been
  /// registered in fragment_exec_state_map_. Runs in the fragment's execution thread.
  void FragmentThread(TUniqueId fragment_instance_id);
};

}

#endif
