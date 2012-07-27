// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

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

namespace sparrow { class StateStore; }

namespace impala {

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

  // shared_ptr required to work around the vagaries of Thrift, which
  // requires that we keep a reference to every service handler
  // implementation. Otherwise Thrift takes sole ownership of this
  // object when we pass it to a Processor to initialise, and will
  // delete it when it is finished with it.
  boost::shared_ptr<sparrow::StateStore> state_store_;

  int state_store_port_;

  struct BackendInfo;
  std::vector<BackendInfo*> backend_info_;  // owned by us
};

}

#endif
