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

#include "scheduling/request-pool-service.h"

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <list>
#include <string>
#include <gutil/strings/substitute.h>

#include "common/constant-strings.h"
#include "common/logging.h"
#include "rpc/jni-thrift-util.h"
#include "service/query-options.h"
#include "util/auth-util.h"
#include "util/collection-metrics.h"
#include "util/mem-info.h"
#include "util/parse-util.h"
#include "util/test-info.h"
#include "util/time.h"

#include "common/names.h"

using namespace impala;

DEFINE_bool(require_username, false, "Requires that a user be provided in order to "
    "schedule requests. If enabled and a user is not provided, requests will be "
    "rejected, otherwise requests without a username will be submitted with the "
    "username 'default'.");
static const string DEFAULT_USER = "default";

DEFINE_string(fair_scheduler_allocation_path, "", "Path to the fair scheduler "
    "allocation file (fair-scheduler.xml).");
// TODO: Rename / cleanup now that Llama is removed (see IMPALA-4159).
DEFINE_string(llama_site_path, "", "Path to the Llama configuration file "
    "(llama-site.xml). If set, fair_scheduler_allocation_path must also be set.");

// The default_pool parameters are used if fair scheduler allocation and Llama
// configuration files are not provided. The default values for this 'default pool'
// are the same as the default values for pools defined via the fair scheduler
// allocation file and Llama configurations.
// TODO: Remove?
DEFINE_int64(default_pool_max_requests, -1, "Maximum number of concurrent outstanding "
    "requests allowed to run before queueing incoming requests. A negative value "
    "indicates no limit. 0 indicates no requests will be admitted. Ignored if "
    "fair_scheduler_config_path and llama_site_path are set.");

static const string default_pool_mem_limit_help_msg = "Maximum amount of memory that all "
    "outstanding requests in this pool may use before new requests to this pool "
    "are queued. " + Substitute(MEM_UNITS_HELP_MSG, "the physical memory") + " "
    "Ignored if fair_scheduler_config_path and llama_site_path are set.";
DEFINE_string(default_pool_mem_limit, "", default_pool_mem_limit_help_msg.c_str());
DEFINE_int64(default_pool_max_queued, 200, "Maximum number of requests allowed to be "
    "queued before rejecting requests. A negative value or 0 indicates requests "
    "will always be rejected once the maximum number of concurrent requests are "
    "executing. Ignored if fair_scheduler_config_path and "
    "llama_site_path are set.");

// Flags to disable the pool limits for all pools.
// TODO: Remove?
DEFINE_bool(disable_pool_mem_limits, false, "Disables all per-pool mem limits.");
DEFINE_bool(disable_pool_max_requests, false, "Disables all per-pool limits on the "
    "maximum number of running requests.");


static const string RESOLVE_POOL_METRIC_NAME = "request-pool-service.resolve-pool-duration-ms";

static const string ERROR_USER_NOT_ALLOWED_IN_POOL = "Request from user '$0' with "
    "requested pool '$1' denied access to assigned pool '$2'";
static const string ERROR_USER_NOT_SPECIFIED = "User must be specified because "
    "-require_username=true.";

const string RequestPoolService::DEFAULT_POOL_NAME = "default-pool";

RequestPoolService::RequestPoolService(MetricGroup* metrics) :
    resolve_pool_ms_metric_(NULL) {
  DCHECK(metrics != NULL);
  resolve_pool_ms_metric_ =
      StatsMetric<double>::CreateAndRegister(metrics, RESOLVE_POOL_METRIC_NAME);

  if (FLAGS_fair_scheduler_allocation_path.empty()) {
    default_pool_only_ = true;
    bool is_percent; // not used
    int64_t bytes_limit = ParseUtil::ParseMemSpec(FLAGS_default_pool_mem_limit,
        &is_percent, MemInfo::physical_mem());
    // -1 indicates an error occurred
    if (bytes_limit < 0) {
      CLEAN_EXIT_WITH_ERROR(Substitute("Unable to parse default pool mem limit from "
          "'$0'.", FLAGS_default_pool_mem_limit));
    }
    // 0 indicates no limit or not set
    if (bytes_limit == 0) {
      default_pool_mem_limit_ = -1;
    } else {
      default_pool_mem_limit_ = bytes_limit;
    }
    return;
  }
  default_pool_only_ = false;

  jmethodID start_id; // JniRequestPoolService.start(), only called in this method.
  JniMethodDescriptor methods[] = {
      {"<init>", "(Ljava/lang/String;Ljava/lang/String;Z)V", &ctor_},
      {"start", "()V", &start_id},
      {"resolveRequestPool", "([B)[B", &resolve_request_pool_id_},
      {"getPoolConfig", "([B)[B", &get_pool_config_id_}};

  JNIEnv* jni_env = JniUtil::GetJNIEnv();
  jni_request_pool_service_class_ =
      jni_env->FindClass("org/apache/impala/util/JniRequestPoolService");
  ABORT_IF_EXC(jni_env);
  uint32_t num_methods = sizeof(methods) / sizeof(methods[0]);
  for (int i = 0; i < num_methods; ++i) {
    ABORT_IF_ERROR(
        JniUtil::LoadJniMethod(jni_env, jni_request_pool_service_class_, &(methods[i])));
  }

  jstring fair_scheduler_config_path =
      jni_env->NewStringUTF(FLAGS_fair_scheduler_allocation_path.c_str());
  ABORT_IF_EXC(jni_env);
  jstring llama_site_path =
      jni_env->NewStringUTF(FLAGS_llama_site_path.c_str());
  ABORT_IF_EXC(jni_env);

  jboolean is_be_test = TestInfo::is_be_test();
  jobject jni_request_pool_service = jni_env->NewObject(jni_request_pool_service_class_,
      ctor_, fair_scheduler_config_path, llama_site_path, is_be_test);
  ABORT_IF_EXC(jni_env);
  ABORT_IF_ERROR(JniUtil::LocalToGlobalRef(
      jni_env, jni_request_pool_service, &jni_request_pool_service_));
  jni_env->CallObjectMethod(jni_request_pool_service_, start_id);
  ABORT_IF_EXC(jni_env);
}

Status RequestPoolService::ResolveRequestPool(const TQueryCtx& ctx,
    string* resolved_pool) {
  if (default_pool_only_) {
    *resolved_pool = DEFAULT_POOL_NAME;
    return Status::OK();
  }
  string user = GetEffectiveUser(ctx.session);
  if (user.empty()) {
    if (FLAGS_require_username) return Status(ERROR_USER_NOT_SPECIFIED);
    // Fall back to a 'default' user if not set so that queries can still run.
    VLOG_RPC << "No user specified: using user=default";
    user = DEFAULT_USER;
  }

  const string& requested_pool = ctx.client_request.query_options.request_pool;
  TResolveRequestPoolParams params;
  params.__set_user(user);
  params.__set_requested_pool(requested_pool);
  TResolveRequestPoolResult result;
  int64_t start_time = MonotonicMillis();
  Status status = JniUtil::CallJniMethod(
      jni_request_pool_service_, resolve_request_pool_id_, params, &result);
  resolve_pool_ms_metric_->Update(MonotonicMillis() - start_time);

  if (result.status.status_code != TErrorCode::OK) {
    return Status(boost::algorithm::join(result.status.error_msgs, "; "));
  }
  if (!result.has_access) {
    return Status(Substitute(ERROR_USER_NOT_ALLOWED_IN_POOL, user,
        requested_pool, result.resolved_pool));
  }
  *resolved_pool = result.resolved_pool;
  return Status::OK();
}

Status RequestPoolService::GetPoolConfig(const string& pool_name,
    TPoolConfig* pool_config) {
  if (default_pool_only_) {
    pool_config->__set_max_requests(
        FLAGS_disable_pool_max_requests ? -1 : FLAGS_default_pool_max_requests);
    pool_config->__set_max_mem_resources(
        FLAGS_disable_pool_mem_limits ? -1 : default_pool_mem_limit_);
    pool_config->__set_max_queued(FLAGS_default_pool_max_queued);
    pool_config->__set_default_query_options("");
    pool_config->__set_min_query_mem_limit(0);
    pool_config->__set_max_query_mem_limit(0);
    pool_config->__set_clamp_mem_limit_query_option(true);
    return Status::OK();
  }

  TPoolConfigParams params;
  params.__set_pool(pool_name);
  RETURN_IF_ERROR(JniUtil::CallJniMethod(
      jni_request_pool_service_, get_pool_config_id_, params, pool_config));
  if (FLAGS_disable_pool_max_requests) pool_config->__set_max_requests(-1);
  if (FLAGS_disable_pool_mem_limits) pool_config->__set_max_mem_resources(-1);
  return Status::OK();
}
