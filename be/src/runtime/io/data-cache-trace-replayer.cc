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

#include <boost/filesystem.hpp>
#include <fstream>
#include <gflags/gflags.h>
#include <iostream>
#include <memory>
#include <rapidjson/document.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>
#include <string>

#include "common/init.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "runtime/io/data-cache-trace.h"

#include "common/names.h"

// This provides a command-line utility for replaying data cache trace files.
// It is simply a wrapper around the DataCacheTrace::TraceReplayer to take
// command line arguments. Beyond the explicitly provided commandline flags
// below, it supports the other command line flags that influence cache behavior.
// For example, the data_cache_eviction_policy can be specified, along with policy
// specific flags such as lirs_unprotected_percentage or similar flags.
//
// Usage example:
// Suppose there is a trace of a workload using a single-directory 100GB data cache with
// LRU eviction policy. (i.e. data_cache_eviction_policy=LRU and
// data_cache="/cache_path:100GB"). The replayer can run the same workload against
// different cache configurations to see how things change.
//
// To run against a larger cache still using LRU:
// data-cache-trace-replayer --trace_directory /path/to/trace/directory
//     --data_cache="/cache_path:150GB" --data_cache_eviction_policy=LRU
//
// To run against the same cache size, but with LIRS cache eviction policy:
// data-cache-trace-replayer --trace_directory /path/to/trace/directory
//     --data_cache="/cache_path:100GB" --data_cache_eviction_policy=LIRS
//
// The replayer produces two different types of cache hit statistics. The first is
// the cache hit statistics from the original trace (i.e. the original 100GB cache
// using LRU). This is a fixed property of a given set of trace files, and it will
// always be the same regardless of the replay configuration. The second is the cache
// hit statistics from the replay using the replay configuration (e.g. a replay using
// a smaller cache may show fewer hits, etc).
//
// These commands put output in the glog INFO file. For JSON output, see the
// output_file option.

// One of either trace_file or trace_directory must be specified
DEFINE_string(trace_file, "", "Single trace file to replay");
DEFINE_string(trace_directory, "", "Directory of trace files to replay. Files will be "
    "replayed sequentially in sorted order.");

// The replay may use any configuration supported by the DataCache.
DEFINE_string(data_cache_configuration, "", "Cache configuration string in the same "
    "format as the 'data_cache' Impala startup parameter. Specifically, it takes "
    "a list of directories, separated by ',', followed by a ':' and a capacity quota "
    "per directory. For example '/data/0,/data/1:1TB' means the cache may use up to 2TB, "
    "with 1TB max in each /data/0 and /data/1.");

// If specified, the cache hit statistics are written to a JSON file with the provided
// filename. If not specified, output goes to the INFO log.
DEFINE_string(output_file, "", "File to write with JSON output containing hits/misses");

using namespace impala;
using namespace impala::io;
using namespace impala::io::trace;
using namespace rapidjson;
using strings::Substitute;

// Convert the CacheHitStatistics 'stats' to a JSON struct using the allocator from
// 'document'.
Value CacheHitStatisticsToJson(Document* document, const CacheHitStatistics& stats) {
  Value json_value(kObjectType);
  json_value.AddMember("hits", Value(stats.hits), document->GetAllocator());
  json_value.AddMember("partial_hits", Value(stats.partial_hits),
      document->GetAllocator());
  json_value.AddMember("hit_bytes", Value(stats.hit_bytes), document->GetAllocator());
  json_value.AddMember("misses", Value(stats.misses), document->GetAllocator());
  json_value.AddMember("miss_bytes", Value(stats.miss_bytes), document->GetAllocator());
  json_value.AddMember("stores", Value(stats.stores), document->GetAllocator());
  json_value.AddMember("failed_stores", Value(stats.failed_stores),
      document->GetAllocator());
  return json_value;
}

// Output CacheHitStatistics to the INFO glog.
void DumpStatisticsToLog(const CacheHitStatistics& stats) {
  LOG(INFO) << "Hits: " << std::to_string(stats.hits)
            << " hit bytes: " << std::to_string(stats.hit_bytes)
            << " partial hits: " << std::to_string(stats.partial_hits);
  LOG(INFO) << "Misses: " << std::to_string(stats.misses)
            << " miss bytes: " << std::to_string(stats.miss_bytes);
  LOG(INFO) << "Stores: " << std::to_string(stats.stores)
            << " failed stores: " << std::to_string(stats.failed_stores);
}

// Write a JSON structure with both the original trace cache hit statistics and
// the replay cache hit statistics.
void DumpStatisticsToJSON(const CacheHitStatistics& trace_stats,
    const CacheHitStatistics& replay_stats, std::string filename) {
  Document document;
  document.SetObject();

  // Add trace stats
  Value trace_stats_json = CacheHitStatisticsToJson(&document, trace_stats);
  document.AddMember("original_trace_stats", trace_stats_json, document.GetAllocator());
  Value replay_stats_json = CacheHitStatisticsToJson(&document, replay_stats);
  document.AddMember("replay_stats", replay_stats_json, document.GetAllocator());

  ofstream ofs(filename);
  OStreamWrapper osw(ofs);
  Writer<OStreamWrapper> writer(osw);
  document.Accept(writer);
}

Status ValidateFlags() {
  // data_cache_configuration is required
  if (FLAGS_data_cache_configuration.size() == 0) {
    return Status("data_cache_configuration must be specified.");
  }
  // trace_file and trace_directory are mutually exclusive
  if (FLAGS_trace_file.size() > 0 && FLAGS_trace_directory.size() > 0) {
    return Status(Substitute("Cannot specify both trace_file and trace_directory"
        "trace_file: $0 trace_directory: $1", FLAGS_trace_file, FLAGS_trace_directory));
  }
  // Requires one of trace_file or trace_directory
  if (FLAGS_trace_file.size() == 0 && FLAGS_trace_directory.size() == 0) {
    return Status("Either trace_file or trace_directory must be specified");
  }

  if (FLAGS_trace_file.size() > 0) {
    // Verify the file exists
    boost::filesystem::path trace_file(FLAGS_trace_file);
    if (!boost::filesystem::exists(trace_file)) {
      return Status(Substitute("Invalid trace_file $0. File does not exist.",
          FLAGS_trace_file));
    }
  } else if (FLAGS_trace_directory.size() > 0) {
    // Verify this is a directory that exists
    boost::filesystem::path dir(FLAGS_trace_directory);
    if (!boost::filesystem::exists(dir)) {
      return Status(Substitute("Invalid trace_directory $0. Directory doesn't exist",
          FLAGS_trace_directory));
    }
    if (!boost::filesystem::is_directory(dir)) {
      return Status(Substitute("Invalid trace_directory $0. Not a directory.",
          FLAGS_trace_directory));
    }
  }
  return Status::OK();
}

int main(int argc, char **argv) {
  InitCommonRuntime(argc, argv, false);

  Status status = ValidateFlags();
  if (!status.ok()) CLEAN_EXIT_WITH_ERROR(status.GetDetail());

  LOG(INFO) << "Initialize cache with configuration: " << FLAGS_data_cache_configuration;
  TraceReplayer replayer(FLAGS_data_cache_configuration);
  status = replayer.Init();
  if (!status.ok()) CLEAN_EXIT_WITH_ERROR(status.GetDetail());

  if (FLAGS_trace_file.size() != 0) {
    LOG(INFO) << "Replaying file: " << FLAGS_trace_file;
    status = replayer.ReplayFile(FLAGS_trace_file);
  } else if (FLAGS_trace_directory.size() != 0) {
    LOG(INFO) << "Replaying directory: " << FLAGS_trace_directory;
    status = replayer.ReplayDirectory(FLAGS_trace_directory);
  }
  if (!status.ok()) CLEAN_EXIT_WITH_ERROR(status.GetDetail());

  CacheHitStatistics original_trace_stats = replayer.GetOriginalTraceStatistics();
  CacheHitStatistics replay_stats = replayer.GetReplayStatistics();

  if (FLAGS_output_file.size() != 0) {
    DumpStatisticsToJSON(original_trace_stats, replay_stats, FLAGS_output_file);
  } else {
    LOG(INFO) << "Cache hit statistics from the original trace:";
    DumpStatisticsToLog(original_trace_stats);
    LOG(INFO) << "Cache hit statistics from the replay:";
    DumpStatisticsToLog(replay_stats);
  }
  return 0;
}
