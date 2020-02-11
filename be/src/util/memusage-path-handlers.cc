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

#include "util/memusage-path-handlers.h"

#include <gperftools/malloc_extension.h>

#include "runtime/mem-tracker.h"
#include "util/common-metrics.h"
#include "util/mem-info.h"
#include "util/metrics.h"
#include "util/pretty-printer.h"

#include "common/names.h"

using namespace impala;
using namespace rapidjson;

/// A MemTracker tracks memory consumption and limits which will be displayed. This method
/// adds the mem_limit and the current memory consumption to the rapidjson document. Also,
/// it dumps all the additional mem trackers to the rapidjson document.
void AddMemTracker(MemTracker* mem_tracker,
    const Webserver::WebRequest& req, Document* document) {
  DCHECK(mem_tracker != NULL);
  Value mem_limit(PrettyPrinter::Print(mem_tracker->limit(), TUnit::BYTES).c_str(),
      document->GetAllocator());
  document->AddMember("mem_limit", mem_limit, document->GetAllocator());
  Value consumption(
      PrettyPrinter::Print(mem_tracker->consumption(), TUnit::BYTES).c_str(),
      document->GetAllocator());
  document->AddMember("consumption", consumption, document->GetAllocator());

  // Dump all mem trackers.
  Value detailed(mem_tracker->LogUsage(MemTracker::UNLIMITED_DEPTH).c_str(),
      document->GetAllocator());
  document->AddMember("detailed", detailed, document->GetAllocator());
}

/// Adds the following malloc data structures to the rapidjson document.
/// - Total inuse memory by application.
/// - Free memory(thread, central and page heap),
/// - Freelist of central cache, each class.
/// - Page heap freelist.
void AddTCmallocOverview(const Webserver::WebRequest& req, Document* document) {
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
}

/// Adds the system memory configuration to the rapidjson document.
void AddSystemInfo(const Webserver::WebRequest& req, Document* document) {
  Value systeminfo(MemInfo::DebugString().c_str(), document->GetAllocator());
  document->AddMember("systeminfo", systeminfo, document->GetAllocator());
}

/// A MetricGroup is a container for a set of metric. This method extracts and adds the
/// initialized JVM, buffer-pool and aggregated memory metrics to the rapidjson document
/// from the given MetricGroup.
void AddMetricGroup(MetricGroup* metric_group,
    const Webserver::WebRequest& req, Document* document) {
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

void impala::AddMemUsageCallbacks(Webserver* webserver, MemTracker* mem_tracker,
    MetricGroup* metric_group) {
  if (mem_tracker != nullptr) {
    auto callback = [mem_tracker, metric_group]
        (const Webserver::WebRequest& req, Document* doc) {
      AddMemTracker(mem_tracker, req, doc);
      AddTCmallocOverview(req, doc);
      AddSystemInfo(req, doc);
      AddMetricGroup(metric_group, req, doc);
    };
    webserver->RegisterUrlCallback("/memz", "memz.tmpl", callback, true);
  } else {
    auto callback = [metric_group]
        (const Webserver::WebRequest& req, Document* doc) {
      AddTCmallocOverview(req, doc);
      AddSystemInfo(req, doc);
      AddMetricGroup(metric_group, req, doc);
    };
    webserver->RegisterUrlCallback("/memz", "memz.tmpl", callback, true);
  }
}
