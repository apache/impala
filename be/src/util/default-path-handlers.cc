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

#include "util/default-path-handlers.h"

#include <fstream>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <gutil/strings/substitute.h>
#include <gnu/libc-version.h>
#include <sys/stat.h>

#include "common/daemon-env.h"
#include "common/logging.h"
#include "kudu/util/flags.h"
#include "rpc/jni-thrift-util.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "service/impala-server.h"
#include "util/cgroup-util.h"
#include "util/common-metrics.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/jni-util.h"
#include "util/mem-info.h"
#include "util/memusage-path-handlers.h"
#include "util/metrics.h"
#include "util/pprof-path-handlers.h"
#include "util/process-state-info.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using namespace google;
using namespace impala;
using namespace rapidjson;
using namespace strings;

DECLARE_bool(enable_process_lifetime_heap_profiling);
DECLARE_bool(use_local_catalog);
DEFINE_int64(web_log_bytes, 1024 * 1024,
    "The maximum number of bytes to display on the debug webserver's log page");

// Writes the last FLAGS_web_log_bytes of the INFO logfile to a webpage
// Note to get best performance, set GLOG_logbuflevel=-1 to prevent log buffering
void LogsHandler(const Webserver::WebRequest& req, Document* document) {
  string logfile;
  impala::GetFullLogFilename(google::INFO, &logfile);
  Value log_path(logfile.c_str(), document->GetAllocator());
  document->AddMember("logfile", log_path, document->GetAllocator());

  struct stat file_stat;
  if (stat(logfile.c_str(), &file_stat) == 0) {
    long size = file_stat.st_size;
    long seekpos = size < FLAGS_web_log_bytes ? 0L : size - FLAGS_web_log_bytes;
    ifstream log(logfile.c_str(), ios::in);
    // Note if the file rolls between stat and seek, this could fail (and we could wind up
    // reading the whole file). But because the file is likely to be small, this is
    // unlikely to be an issue in practice.
    log.seekg(seekpos);
    document->AddMember("num_bytes", FLAGS_web_log_bytes, document->GetAllocator());
    stringstream ss;
    ss << log.rdbuf();
    Value log_json(ss.str().c_str(), document->GetAllocator());
    document->AddMember("log", log_json, document->GetAllocator());
  } else {
    Value error(Substitute("Couldn't open INFO log file: $0", logfile).c_str(),
        document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
  }
}

// Registered to handle "/varz", and produces json containing an array of flag metadata
// objects:
//
// "title": "Command-line Flags",
// "flags": [
//     {
//       "name": "catalog_service_port",
//       "type": "int32",
//       "description": "port where the CatalogService is running",
//       "default": "26000",
//       "current": "26000"
//     },
// .. etc
void FlagsHandler(const Webserver::WebRequest& req, Document* document) {
  vector<CommandLineFlagInfo> flag_info;
  GetAllFlags(&flag_info, true);
  Value flag_arr(kArrayType);
  for (const CommandLineFlagInfo& flag: flag_info) {
    string flag_value = CheckFlagAndRedact(flag, kudu::EscapeMode::NONE);

    Value flag_val(kObjectType);
    Value name(flag.name.c_str(), document->GetAllocator());
    flag_val.AddMember("name", name, document->GetAllocator());

    Value type(flag.type.c_str(), document->GetAllocator());
    flag_val.AddMember("type", type, document->GetAllocator());

    Value description(flag.description.c_str(), document->GetAllocator());
    flag_val.AddMember("description", description, document->GetAllocator());

    Value default_value(flag.default_value.c_str(), document->GetAllocator());
    flag_val.AddMember("default", default_value, document->GetAllocator());

    Value current_value(flag_value.c_str(), document->GetAllocator());
    flag_val.AddMember("current", current_value, document->GetAllocator());

    flag_val.AddMember("experimental", flag.hidden, document->GetAllocator());

    if (!flag.is_default) {
      flag_val.AddMember("value_changed", 1, document->GetAllocator());
    }
    flag_arr.PushBack(flag_val, document->GetAllocator());
  }
  Value title("Command-line Flags", document->GetAllocator());
  document->AddMember("title", title, document->GetAllocator());
  document->AddMember("flags", flag_arr, document->GetAllocator());
}

// Profile documentation handlers.
// Will have list of all the profile counters and the definition of Significance added
// in document.
// Counters have the following properties:
//   name, significance, description, unit
void ProfileDocsHandler(const Webserver::WebRequest& req, Document* document) {
  vector<const ProfileEntryPrototype*> prototypes;
  ProfileEntryPrototypeRegistry::get()->GetPrototypes(&prototypes);

  Value profile_docs(kArrayType);
  for (auto p : prototypes) {
    Value p_val(kObjectType);
    Value name(p->name(), document->GetAllocator());
    p_val.AddMember("name", name, document->GetAllocator());
    Value significance(p->significance(), document->GetAllocator());
    p_val.AddMember("significance", significance, document->GetAllocator());
    Value description(p->desc(), document->GetAllocator());
    p_val.AddMember("description", description, document->GetAllocator());
    auto unit_it = _TUnit_VALUES_TO_NAMES.find(p->unit());
    const char* unit_str = unit_it != _TUnit_VALUES_TO_NAMES.end() ? unit_it->second : "";
    Value unit(unit_str, document->GetAllocator());
    p_val.AddMember("unit", unit, document->GetAllocator());
    profile_docs.PushBack(p_val, document->GetAllocator());
  }

  Value significance_def(kArrayType);
  for (auto significance : ProfileEntryPrototype::ALLSIGNIFICANCE) {
    Value significance_obj(kObjectType);
    Value significance_val(ProfileEntryPrototype::SignificanceString(significance),
        document->GetAllocator());
    Value significance_description(
        ProfileEntryPrototype::SignificanceDescription(significance),
        document->GetAllocator());
    significance_obj.AddMember("name", significance_val,document->GetAllocator());
    significance_obj.AddMember("description",
        significance_description,document->GetAllocator());
    significance_def.PushBack(significance_obj, document->GetAllocator());
  }

  document->AddMember("significance_docs", significance_def, document->GetAllocator());
  document->AddMember("profile_docs", profile_docs, document->GetAllocator());
}

void JmxHandler(const Webserver::WebRequest& req, Document* document) {
  document->AddMember(rapidjson::StringRef(Webserver::ENABLE_PLAIN_JSON_KEY), true,
      document->GetAllocator());
  TGetJMXJsonResponse result;
  Status status = JniUtil::GetJMXJson(&result);
  if (!status.ok()) {
    Value error(status.GetDetail().c_str(), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    VLOG(1) << "Error fetching JMX metrics: " << status.GetDetail();
    return;
  }
  // Parse the JSON string returned from fe. We do an additional round of
  // parsing to populate the JSON structure in the 'document' for our template
  // rendering to work correctly. Otherwise the whole JSON content is considered
  // as a single string mapped to another key.
  Document doc(&document->GetAllocator());
  doc.Parse<kParseDefaultFlags>(result.jmx_json.c_str());
  if (doc.HasParseError()) {
    Value error(GetParseError_En(doc.GetParseError()), document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
    VLOG(1) << "Error fetching JMX metrics: " << doc.GetParseError();
    return;
  }
  // Populate the members in the document. Due to MOVE semantic of RapidJSON,
  // the ownership will be transferred to the target document.
  for (Value::MemberIterator it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
    document->AddMember(it->name, it->value, document->GetAllocator());
  }
}

// Helper function that creates a Value for a given build flag name, value and adds it to
// an array of build_flags
void AddBuildFlag(const std::string& flag_name, const std::string& flag_value,
    Document* document, Value* build_flags) {
  Value build_type(kObjectType);
  Value build_type_name(flag_name.c_str(), document->GetAllocator());
  build_type.AddMember("flag_name", build_type_name, document->GetAllocator());
  Value build_type_value(flag_value.c_str(), document->GetAllocator());
  build_type.AddMember("flag_value", build_type_value, document->GetAllocator());
  build_flags->PushBack(build_type, document->GetAllocator());
}

namespace impala {

// Add active or standby status for catalog or statestore.
void AddActiveStatus(Document* document, MetricGroup* metric_group) {
  DaemonEnv* d_env = DaemonEnv::GetInstance();
  bool is_statestore = false;
  bool is_catalog = false;
  if (d_env != nullptr) {
    if (d_env->name() == "statestore") {
      is_statestore = true;
    } else if (d_env->name() == "catalog") {
      is_catalog = true;
    } else {
      // Only deal with the status of catalog or statestore.
      return;
    }
    document->AddMember("is_statestore", is_statestore, document->GetAllocator());
    document->AddMember("is_catalog", is_catalog, document->GetAllocator());
  }

  if (metric_group != nullptr) {
    if (is_catalog) {
      BooleanProperty* metric = metric_group->FindMetricForTesting<BooleanProperty>(
          "catalog-server.active-status");
      if (metric != nullptr) {
        if (metric->GetValue()) {
          document->AddMember(
              "catalogd_active_status", "Active", document->GetAllocator());
        } else {
          document->AddMember(
              "catalogd_active_status", "Standby", document->GetAllocator());
        }
      }
    }
    if (is_statestore) {
      BooleanProperty* metric =
          metric_group->FindMetricForTesting<BooleanProperty>("statestore.active-status");
      if (metric != nullptr) {
        if (metric->GetValue()) {
          document->AddMember(
              "statestore_active_status", "Active", document->GetAllocator());
        } else {
          document->AddMember(
              "statestore_active_status", "Standby", document->GetAllocator());
        }
      }
    }
  }
}

void RootHandler(
    const Webserver::WebRequest& req, Document* document, MetricGroup* metric_group) {
  Value version(GetVersionString().c_str(), document->GetAllocator());
  document->AddMember("version", version, document->GetAllocator());

#ifdef NDEBUG
  const char* is_ndebug = "true";
#else
  const char* is_ndebug = "false";
#endif

  Value build_flags(kArrayType);
  AddBuildFlag("is_ndebug", is_ndebug, document, &build_flags);
  string cmake_build_type(GetCMakeBuildType());
  replace(cmake_build_type.begin(), cmake_build_type.end(), '-', '_');
  AddBuildFlag("cmake_build_type", cmake_build_type, document, &build_flags);
  AddBuildFlag("library_link_type", GetLibraryLinkType(), document, &build_flags);
  document->AddMember("build_flags", build_flags, document->GetAllocator());

  Value cpu_info(CpuInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("cpu_info", cpu_info, document->GetAllocator());
  Value mem_info(MemInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("mem_info", mem_info, document->GetAllocator());
  Value disk_info(DiskInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("disk_info", disk_info, document->GetAllocator());
  Value os_info(OsInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("os_info", os_info, document->GetAllocator());
  Value process_state_info(
      ProcessStateInfo().DebugString().c_str(), document->GetAllocator());
  document->AddMember("process_state_info", process_state_info, document->GetAllocator());
  Value cgroup_info(CGroupUtil::DebugString().c_str(), document->GetAllocator());
  document->AddMember("cgroup_info", cgroup_info, document->GetAllocator());

  Value effective_locale(std::locale("").name().c_str(), document->GetAllocator());
  document->AddMember("effective_locale", effective_locale, document->GetAllocator());
  Value glibc_version(gnu_get_libc_version(), document->GetAllocator());
  document->AddMember("glibc_version", glibc_version, document->GetAllocator());

  if (CommonMetrics::PROCESS_START_TIME != nullptr) {
    Value process_start_time(
        CommonMetrics::PROCESS_START_TIME->GetValue().c_str(), document->GetAllocator());
    document->AddMember(
        "process_start_time", process_start_time, document->GetAllocator());
  }

  AddActiveStatus(document, metric_group);

  ExecEnv* env = ExecEnv::GetInstance();
  if (env == nullptr || env->impala_server() == nullptr) return;
  ImpalaServer* impala_server = env->impala_server();
  document->AddMember("impala_server_mode", true, document->GetAllocator());
  document->AddMember("is_coordinator", impala_server->IsCoordinator(),
      document->GetAllocator());
  document->AddMember("use_local_catalog", FLAGS_use_local_catalog,
      document->GetAllocator());
  document->AddMember("is_executor", impala_server->IsExecutor(),
      document->GetAllocator());
  bool is_quiescing = impala_server->IsShuttingDown();
  document->AddMember("is_quiescing", is_quiescing, document->GetAllocator());
  if (is_quiescing) {
    Value shutdown_status(
        impala_server->ShutdownStatusToString(impala_server->GetShutdownStatus()).c_str(),
        document->GetAllocator());
    document->AddMember("shutdown_status", shutdown_status, document->GetAllocator());
  }
}

void AddDefaultUrlCallbacks(Webserver* webserver, MetricGroup* metric_group,
    MemTracker* process_mem_tracker) {
  if (!FLAGS_logtostderr) {
    webserver->RegisterUrlCallback("/logs", "logs.tmpl", LogsHandler, true);
  }
  webserver->RegisterUrlCallback("/varz", "flags.tmpl", FlagsHandler, true);
  webserver->RegisterUrlCallback(
      "/profile_docs", "profile_docs.tmpl", ProfileDocsHandler, true);
  if (JniUtil::is_jvm_inited()) {
    // JmxHandler outputs a plain JSON string and does not require a template to
    // render. However RawUrlCallback only supports PLAIN content type.
    // (TODO): Switch to RawUrlCallback when it supports JSON content-type.
    webserver->RegisterUrlCallback("/jmx", "raw_text.tmpl", JmxHandler, true);
  }
  AddMemUsageCallbacks(webserver, process_mem_tracker, metric_group);

#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  // Remote (on-demand) profiling is disabled if the process is already being profiled.
  if (!FLAGS_enable_process_lifetime_heap_profiling) {
    AddPprofUrlCallbacks(webserver);
  }
#endif

  auto root_handler = [metric_group](const Webserver::WebRequest& req, Document* doc) {
    RootHandler(req, doc, metric_group);
  };
  webserver->RegisterUrlCallback("/", "root.tmpl", root_handler, true);
}

}
