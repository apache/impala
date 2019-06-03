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

#include <sstream>
#include <fstream>
#include <sys/stat.h>
#include <boost/algorithm/string.hpp>
#include <boost/bind.hpp>
#include <gperftools/malloc_extension.h>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
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
#include "util/pprof-path-handlers.h"
#include "util/pretty-printer.h"
#include "util/process-state-info.h"

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
    Value flag_val(kObjectType);
    Value name(flag.name.c_str(), document->GetAllocator());
    flag_val.AddMember("name", name, document->GetAllocator());

    Value type(flag.type.c_str(), document->GetAllocator());
    flag_val.AddMember("type", type, document->GetAllocator());

    Value description(flag.description.c_str(), document->GetAllocator());
    flag_val.AddMember("description", description, document->GetAllocator());

    Value default_value(flag.default_value.c_str(), document->GetAllocator());
    flag_val.AddMember("default", default_value, document->GetAllocator());

    Value current_value(flag.current_value.c_str(), document->GetAllocator());
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

// Registered to handle "/memz"
void MemUsageHandler(MemTracker* mem_tracker, MetricGroup* metric_group,
    const Webserver::WebRequest& req, Document* document) {
  DCHECK(mem_tracker != NULL);
  Value mem_limit(PrettyPrinter::Print(mem_tracker->limit(), TUnit::BYTES).c_str(),
      document->GetAllocator());
  document->AddMember("mem_limit", mem_limit, document->GetAllocator());
  Value consumption(
      PrettyPrinter::Print(mem_tracker->consumption(), TUnit::BYTES).c_str(),
      document->GetAllocator());
  document->AddMember("consumption", consumption, document->GetAllocator());

  stringstream ss;
#if defined(ADDRESS_SANITIZER) || defined(THREAD_SANITIZER)
  ss << "Memory tracking is not available with address or thread sanitizer builds.";
#else
  char buf[2048];
  MallocExtension::instance()->GetStats(buf, 2048);
  ss << string(buf);
#endif

  Value overview(ss.str().c_str(), document->GetAllocator());
  document->AddMember("overview", overview, document->GetAllocator());

  // Dump all mem trackers.
  Value detailed(mem_tracker->LogUsage(MemTracker::UNLIMITED_DEPTH).c_str(),
      document->GetAllocator());
  document->AddMember("detailed", detailed, document->GetAllocator());

  Value systeminfo(MemInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("systeminfo", systeminfo, document->GetAllocator());

  if (metric_group != nullptr) {
    MetricGroup* aggregate_group = metric_group->FindChildGroup("memory");
    if (aggregate_group != nullptr) {
      Value json_metrics(kObjectType);
      aggregate_group->ToJson(false, document, &json_metrics);
      document->AddMember(
          "aggregate_metrics", json_metrics["metrics"], document->GetAllocator());
    }
    MetricGroup* jvm_group = metric_group->FindChildGroup("jvm");
    if (jvm_group != nullptr) {
      Value jvm(kObjectType);
      jvm_group->ToJson(false, document, &jvm);
      Value heap(kArrayType);
      Value non_heap(kArrayType);
      Value total(kArrayType);
      for (SizeType i = 0; i < jvm["metrics"].Size(); ++i) {
        if (strstr(jvm["metrics"][i]["name"].GetString(), "total") != nullptr) {
          total.PushBack(jvm["metrics"][i], document->GetAllocator());
        } else if (strstr(jvm["metrics"][i]["name"].GetString(), "non-heap") != nullptr) {
          non_heap.PushBack(jvm["metrics"][i], document->GetAllocator());
        } else if (strstr(jvm["metrics"][i]["name"].GetString(), "heap") != nullptr) {
          heap.PushBack(jvm["metrics"][i], document->GetAllocator());
        }
      }
      document->AddMember("jvm_total", total, document->GetAllocator());
      document->AddMember("jvm_heap", heap, document->GetAllocator());
      document->AddMember("jvm_non_heap", non_heap, document->GetAllocator());
    }
    MetricGroup* buffer_pool_group = metric_group->FindChildGroup("buffer-pool");
    if (buffer_pool_group != nullptr) {
      Value json_metrics(kObjectType);
      buffer_pool_group->ToJson(false, document, &json_metrics);
      document->AddMember(
          "buffer_pool", json_metrics["metrics"], document->GetAllocator());
    }
  }
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

void RootHandler(const Webserver::WebRequest& req, Document* document) {
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

  if (CommonMetrics::PROCESS_START_TIME != nullptr) {
    Value process_start_time(
        CommonMetrics::PROCESS_START_TIME->GetValue().c_str(), document->GetAllocator());
    document->AddMember(
        "process_start_time", process_start_time, document->GetAllocator());
  }

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

void AddDefaultUrlCallbacks(Webserver* webserver, MemTracker* process_mem_tracker,
    MetricGroup* metric_group) {
  webserver->RegisterUrlCallback("/logs", "logs.tmpl", LogsHandler, true);
  webserver->RegisterUrlCallback("/varz", "flags.tmpl", FlagsHandler, true);
  if (JniUtil::is_jvm_inited()) {
    // JmxHandler outputs a plain JSON string and does not require a template to
    // render. However RawUrlCallback only supports PLAIN content type.
    // (TODO): Switch to RawUrlCallback when it supports JSON content-type.
    webserver->RegisterUrlCallback("/jmx", "raw_text.tmpl", JmxHandler, true);
  }
  if (process_mem_tracker != NULL) {
    auto callback = [process_mem_tracker, metric_group]
        (const Webserver::WebRequest& req, Document* doc) {
      MemUsageHandler(process_mem_tracker, metric_group, req, doc);
    };
    webserver->RegisterUrlCallback("/memz", "memz.tmpl", callback, true);
  }

#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  // Remote (on-demand) profiling is disabled if the process is already being profiled.
  if (!FLAGS_enable_process_lifetime_heap_profiling) {
    AddPprofUrlCallbacks(webserver);
  }
#endif

  auto root_handler =
    [](const Webserver::WebRequest& req, Document* doc) {
      RootHandler(req, doc);
    };
  webserver->RegisterUrlCallback("/", "root.tmpl", root_handler, true);
}

}
