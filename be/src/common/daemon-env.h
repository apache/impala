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

#include <memory>
#include <string>

#include "common/global-types.h"
#include "common/status.h"
#include "gen-cpp/common.pb.h"

namespace impala {

class MetricGroup;
class Webserver;

/// Container for singleton objects needed by all Impala daemons and the initialization
/// code for those objects. Each daemon should have exactly one instance of DaemonEnv,
/// which is accessible via DaemonEnv::GetInstance().
///
/// Currently used by the catalogd and statestored. TODO: apply this to impalads.
class DaemonEnv {
 public:
  static DaemonEnv* GetInstance() { return daemon_env_; }

  DaemonEnv(const std::string& name);
  Status Init(bool init_jvm);

  const BackendIdPB& backend_id() const { return backend_id_; }

  MetricGroup* metrics() { return metrics_.get(); }
  Webserver* webserver() { return webserver_.get(); }
  Webserver* metrics_webserver() { return metrics_webserver_.get(); }
  std::string name() { return name_; }

 private:
  static DaemonEnv* daemon_env_;

  // Used to uniquely identify this daemon.
  BackendIdPB backend_id_;

  // The name of the daemon, i.e. 'catalog' or 'statestore'.
  std::string name_;

  std::unique_ptr<MetricGroup> metrics_;
  std::unique_ptr<Webserver> webserver_;
  std::unique_ptr<Webserver> metrics_webserver_;
};

} // namespace impala
