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

#include "daemon-env.h"

#include "common/init.h"
#include "rpc/rpc-trace.h"
#include "util/common-metrics.h"
#include "util/default-path-handlers.h"
#include "util/memory-metrics.h"
#include "util/metrics.h"
#include "util/thread.h"
#include "util/uid-util.h"
#include "util/webserver.h"

#include "common/names.h"

DECLARE_bool(enable_webserver);
DECLARE_string(metrics_webserver_interface);
DECLARE_int32(metrics_webserver_port);
DECLARE_string(webserver_interface);
DECLARE_int32(webserver_port);

namespace impala {

DaemonEnv* DaemonEnv::daemon_env_ = nullptr;

DaemonEnv::DaemonEnv(const string& name)
  : name_(name),
    metrics_(new MetricGroup(name)),
    webserver_(
        new Webserver(FLAGS_webserver_interface, FLAGS_webserver_port, metrics_.get())) {
  UUIDToUniqueIdPB(boost::uuids::random_generator()(), &backend_id_);
  daemon_env_ = this;
}

Status DaemonEnv::Init(bool init_jvm) {
  if (FLAGS_enable_webserver) {
    AddDefaultUrlCallbacks(webserver_.get(), metrics_.get());
    RETURN_IF_ERROR(metrics_->RegisterHttpHandlers(webserver_.get()));
    RETURN_IF_ERROR(webserver_->Start());
  } else {
    LOG(INFO) << "Not starting webserver";
  }

  if (FLAGS_metrics_webserver_port > 0) {
    metrics_webserver_.reset(new Webserver(FLAGS_metrics_webserver_interface,
        FLAGS_metrics_webserver_port, metrics_.get(), Webserver::AuthMode::NONE));
    RETURN_IF_ERROR(metrics_->RegisterHttpHandlers(metrics_webserver_.get()));
    RETURN_IF_ERROR(metrics_webserver_->Start());
  }

  RETURN_IF_ERROR(RegisterMemoryMetrics(metrics_.get(), init_jvm, nullptr, nullptr));
  RETURN_IF_ERROR(StartMemoryMaintenanceThread());
  RETURN_IF_ERROR(StartThreadInstrumentation(metrics_.get(), webserver_.get(), init_jvm));
  InitRpcEventTracing(webserver_.get());
  CommonMetrics::InitCommonMetrics(metrics_.get());

  metrics_->AddProperty<string>(
      Substitute("$0.version", name_), GetVersionString(/* compact */ true));

  return Status::OK();
}

} // namespace impala
