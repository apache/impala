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

namespace impala {

// Manages execution of individual plan fragments, which are typically run as a result of
// ExecPlanFragment() RPCs that arrive via the internal Impala interface.
//
// A fragment is started in ExecPlanFragment(), which starts a thread which runs
// FragmentExecThread() asynchronously and returns. After this point a fragment may
// terminate either by cancellation via CancelPlanFragment(), or when FragmentExecThread()
// returns.
//
// TODO: Remove Thrift args from methods where it would improve readability;
// ImpalaInternalService can take care of translation to / from Thrift, as it already does
// for ExecPlanFragment()'s return value.
class FragmentMgr {
 public:
  // Registers a new FragmentExecState, Prepare()'s it, and launches the thread that runs
  // FragmentExecThread() before returning. Returns OK if there was no error, otherwise
  // returns an error if the fragment is malformed (e.g. no sink), or if there is an error
  // during Prepare().
  Status ExecPlanFragment(const TExecPlanFragmentParams& params);

  // Cancels a plan fragment that is running asynchronously.
  void CancelPlanFragment(TCancelPlanFragmentResult& return_val,
      const TCancelPlanFragmentParams& params);

 private:
  class FragmentExecState;

  // Call exec_state->Exec(), and then removes exec_state from the fragment map. Run in
  // the fragment's execution thread.
  void FragmentExecThread(FragmentExecState* exec_state);

  boost::shared_ptr<FragmentExecState> GetFragmentExecState(
      const TUniqueId& fragment_instance_id);

  // protects fragment_exec_state_map_
  boost::mutex fragment_exec_state_map_lock_;

  // map from fragment id to exec state; FragmentExecState is owned by us and
  // referenced as a shared_ptr to allow asynchronous calls to CancelPlanFragment()
  typedef boost::unordered_map<TUniqueId, boost::shared_ptr<FragmentExecState> >
  FragmentExecStateMap;
  FragmentExecStateMap fragment_exec_state_map_;
};

}

#endif
