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


#ifndef IMPALA_TESTUTIL_TEST_EXEC_ENV_H
#define IMPALA_TESTUTIL_TEST_EXEC_ENV_H

#include <string>
#include <vector>
#include <list>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>
#include <boost/scoped_ptr.hpp>

#include "runtime/exec-env.h"
#include "common/status.h"

namespace apache { namespace thrift { namespace server { class TServer; } } }

namespace impala {

class Metrics;
class StateStore;

// Create environment for single-process distributed query execution.
class TestExecEnv : public ExecEnv {
 public:
  TestExecEnv(int num_backends, int start_port);

  // Stop backend threads.
  virtual ~TestExecEnv();

  // Starts 'num_backends' threads, each one exporting ImpalaInternalService,
  // starting on start_port.
  Status StartBackends();

  std::string DebugString();

 private:
  int num_backends_;
  int start_port_;

  // Kept here because the state-store needs a metrics_ object
  boost::shared_ptr<Metrics> metrics_;

  // shared_ptr required to work around the vagaries of Thrift, which
  // requires that we keep a reference to every service handler
  // implementation. Otherwise Thrift takes sole ownership of this
  // object when we pass it to a Processor to initialise, and will
  // delete it when it is finished with it.
  boost::shared_ptr<StateStore> state_store_;

  int state_store_port_;

  struct BackendInfo;
  std::vector<BackendInfo*> backend_info_;  // owned by us
};

}

#endif
