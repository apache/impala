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

#ifndef IMPALA_SCHEDULING_REQUEST_POOL_SERVICE_H
#define IMPALA_SCHEDULING_REQUEST_POOL_SERVICE_H

#include <jni.h>

#include "gen-cpp/ImpalaInternalService.h"
#include "common/status.h"
#include "util/non-primitive-metrics.h"

namespace impala {

// Used by the SimpleScheduler and AdmissionController to resolve users to pools and to
// get per-pool configurations for admission control. If fair scheduler allocation and
// Llama configuration files are available, then they are used via the Java-side
// RequestPoolService class. Otherwise, a default pool is always returned with pool
// limits configurable via gflags. A single instance of RequestPoolService is created and
// it lives the lifetime of the process.
class RequestPoolService {
 public:
  // Initializes the JNI method stubs if configuration files are specified. If any
  // method can't be found, or if there is any further error, the constructor will
  // terminate the process.
  RequestPoolService(Metrics* metrics);

  // Resolves the user and user-provided pool name to the pool returned by the placement
  // policy and whether or not the user is authorized. If default_pool_only_ is true,
  // then this will always return the default pool and will always be authorized, i.e.
  // pool and user are ignored.
  Status ResolveRequestPool(const std::string& requested_pool_name,
      const std::string& user, TResolveRequestPoolResult* resolved_pool);

  // Gets the pool configuration values for the specified pool. If default_pool_only_ is
  // true, then the returned values are always the default pool values, i.e. pool is
  // ignored.
  Status GetPoolConfig(const std::string& pool_name, TPoolConfigResult* pool_config);

 private:
  // Metrics subsystem access
  Metrics* metrics_;

  // Metric measuring the time ResolveRequestPool() takes, in milliseconds.
  StatsMetric<double>* resolve_pool_ms_metric_;

  // True if the pool configuration files are not provided. ResolveRequestPool() will
  // always return the default-pool and GetPoolConfig() will always return the limits
  // specified by the default pool gflags, which are unlimited unless specified via
  // the 'default_pool_max_XXX' flags (see the flag definitions in
  // request-pool-service.cc).
  bool default_pool_only_;

  // Mem limit (in bytes) of the default pool. Only used if default_pool_only_ is true.
  // Set in the constructor by parsing the 'default_pool_mem_limit' gflag using
  // ParseUtil::ParseMemSpec().
  int64_t default_pool_mem_limit_;

  // The following members are not initialized if default_pool_only_ is true.
  // Descriptor of Java RequestPoolService class itself, used to create a new instance.
  jclass request_pool_service_class_;
  // Instance of com.cloudera.impala.util.RequestPoolService
  jobject request_pool_service_;
  jmethodID resolve_request_pool_id_;  // RequestPoolService.resolveRequestPool()
  jmethodID get_pool_config_id_;  // RequestPoolService.getPoolConfig()
  jmethodID ctor_;
};

}

#endif
