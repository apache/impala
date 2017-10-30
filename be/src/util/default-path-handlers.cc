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
#include "runtime/mem-tracker.h"
#include "runtime/exec-env.h"
#include "service/impala-server.h"
#include "util/common-metrics.h"
#include "util/debug-util.h"
#include "util/mem-info.h"
#include "util/pprof-path-handlers.h"
#include "util/mem-info.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/process-state-info.h"

#include "common/names.h"

using namespace google;
using namespace impala;
using namespace rapidjson;
using namespace strings;

DECLARE_bool(enable_process_lifetime_heap_profiling);
DEFINE_int64(web_log_bytes, 1024 * 1024,
    "The maximum number of bytes to display on the debug webserver's log page");

// Writes the last FLAGS_web_log_bytes of the INFO logfile to a webpage
// Note to get best performance, set GLOG_logbuflevel=-1 to prevent log buffering
void LogsHandler(const Webserver::ArgumentMap& args, Document* document) {
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

// Registered to handle "/flags", and produces json containing an array of flag metadata
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
void FlagsHandler(const Webserver::ArgumentMap& args, Document* document) {
  vector<CommandLineFlagInfo> flag_info;
  GetAllFlags(&flag_info);
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
    const Webserver::ArgumentMap& args, Document* document) {
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

namespace impala {

void RootHandler(const Webserver::ArgumentMap& args, Document* document) {
  Value version(GetVersionString().c_str(), document->GetAllocator());
  document->AddMember("version", version, document->GetAllocator());
  Value cpu_info(CpuInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("cpu_info", cpu_info, document->GetAllocator());
  Value mem_info(MemInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("mem_info", mem_info, document->GetAllocator());
  Value disk_info(DiskInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("disk_info", disk_info, document->GetAllocator());
  Value os_info(OsInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("os_info", os_info, document->GetAllocator());
  Value process_state_info(ProcessStateInfo().DebugString().c_str(),
    document->GetAllocator());
  document->AddMember("process_state_info", process_state_info,
    document->GetAllocator());

  if (CommonMetrics::PROCESS_START_TIME != nullptr) {
    Value process_start_time(CommonMetrics::PROCESS_START_TIME->value().c_str(),
      document->GetAllocator());
    document->AddMember("process_start_time", process_start_time,
      document->GetAllocator());
  }

  ExecEnv* env = ExecEnv::GetInstance();
  if (env == nullptr || env->impala_server() == nullptr) return;
  document->AddMember("impala_server_mode", true, document->GetAllocator());
  document->AddMember("is_coordinator", env->impala_server()->IsCoordinator(),
      document->GetAllocator());
  document->AddMember("is_executor", env->impala_server()->IsExecutor(),
      document->GetAllocator());
}

void AddDefaultUrlCallbacks(
    Webserver* webserver, MemTracker* process_mem_tracker, MetricGroup* metric_group) {
  webserver->RegisterUrlCallback("/logs", "logs.tmpl", LogsHandler);
  webserver->RegisterUrlCallback("/varz", "flags.tmpl", FlagsHandler);
  if (process_mem_tracker != NULL) {
    auto callback = [process_mem_tracker, metric_group]
        (const Webserver::ArgumentMap& args, Document* doc) {
      MemUsageHandler(process_mem_tracker, metric_group, args, doc);
    };
    webserver->RegisterUrlCallback("/memz", "memz.tmpl", callback);
  }

#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  // Remote (on-demand) profiling is disabled if the process is already being profiled.
  if (!FLAGS_enable_process_lifetime_heap_profiling) {
    AddPprofUrlCallbacks(webserver);
  }
#endif

  auto root_handler =
    [](const Webserver::ArgumentMap& args, Document* doc) {
      RootHandler(args, doc);
    };
  webserver->RegisterUrlCallback("/", "root.tmpl", root_handler);
}

}
