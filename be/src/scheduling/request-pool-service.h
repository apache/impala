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

#ifndef IMPALA_SCHEDULING_REQUEST_POOL_SERVICE_H
#define IMPALA_SCHEDULING_REQUEST_POOL_SERVICE_H

#include <jni.h>

#include "common/status.h"
#include "util/metrics-fwd.h"

namespace impala {

/// Service to resolve incoming requests to resource pools and to provide the pool
/// configurations. A single instance of RequestPoolService is created and it lives the
/// lifetime of the process.
///
/// The resource pools are specified via fair-scheduler.xml and llama-site.xml files if
/// they are available, otherwise all requests are mapped to a single 'default-pool'
/// which is configurable via gflags. The xml files are managed by the Java class
/// RequestPoolService, called via JNI.
class RequestPoolService {
 public:
  // Pool name used when the configuration files are not specified.
  static const std::string DEFAULT_POOL_NAME;

  /// Initializes the JNI method stubs if configuration files are specified. If any
  /// method can't be found, or if there is any further error, the constructor will
  /// terminate the process.
  RequestPoolService(MetricGroup* metrics);

  /// Resolves the request to a resource pool as determined by the policy. Returns an
  /// error if the request cannot be resolved to a pool or if the user does not have
  /// access to submit requests in the resolved pool. If default_pool_only_ is true,
  /// then this will always return the default pool.
  Status ResolveRequestPool(const TQueryCtx& ctx, std::string* resolved_pool);

  /// Gets the pool configuration values for the specified pool. If default_pool_only_ is
  /// true, then the returned values are always the default pool values, i.e. pool_name
  /// is ignored.
  Status GetPoolConfig(const std::string& pool_name, TPoolConfig* pool_config);

 private:
  /// Metric measuring the time ResolveRequestPool() takes, in milliseconds.
  StatsMetric<double>* resolve_pool_ms_metric_;

  /// True if the pool configuration files are not provided. ResolveRequestPool() will
  /// always return the default-pool and GetPoolConfig() will always return the limits
  /// specified by the default pool gflags, which are unlimited unless specified via
  /// the 'default_pool_max_XXX' flags (see the flag definitions in
  /// request-pool-service.cc).
  bool default_pool_only_;

  /// Mem limit (in bytes) of the default pool. Only used if default_pool_only_ is true.
  /// Set in the constructor by parsing the 'default_pool_mem_limit' gflag using
  /// ParseUtil::ParseMemSpec().
  int64_t default_pool_mem_limit_;

  /// The following members are not initialized if default_pool_only_ is true.
  /// Descriptor of Java RequestPoolService class itself, used to create a new instance.
  jclass jni_request_pool_service_class_;
  /// Instance of org.apache.impala.util.RequestPoolService
  jobject jni_request_pool_service_;
  jmethodID resolve_request_pool_id_;  // RequestPoolService.resolveRequestPool()
  jmethodID get_pool_config_id_;  // RequestPoolService.getPoolConfig()
  jmethodID ctor_;
};

}

#endif
