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

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "kudu/util/slice.h"

namespace kudu {
  class AsyncLogger;
}

namespace impala {
namespace io {

class DataCache;

// When working on data cache tuning or optimization, the actual cache eviction behavior
// is only a function of the metadata operations (which file locations are accessed in
// what order) and do not require actual data. Since the data for caching workloads is
// often large and sensitive, the data cache trace provides a way to improve cache
// policies without access to the original data/workloads. It also reduces the cost of
// testing the cache, since the metadata operations are significantly faster than the
// data operations.
//
// The DataCache is instrumented to trace interesting events (e.g. hits misses, stores,
// etc). This file implements the Tracer that writes the events out to disk and a
// corresponding TraceReplayer which can read the events and replay them against a
// cache with a particular configuration.
namespace trace {

class SimpleLoggerWrapper;

enum class EventType {
  HIT,
  MISS,
  STORE,
  STORE_FAILED_BUSY,
  STORE_FAILED
};

// Functions to convert EventType to a string representation and back
EventType StringToEventType(std::string s);
std::string EventTypeToString(EventType s);

struct TraceEvent {
  TraceEvent(EventType type, double timestamp, std::string filename, int64_t mtime,
             int64_t offset, int64_t entry_length, int64_t lookup_length)
   : type(type), timestamp(timestamp), filename(filename), mtime(mtime),
     offset(offset), entry_length(entry_length), lookup_length(lookup_length) {}

  TraceEvent() = default;

  // Type of cache event: HIT, MISS, etc.
  EventType type;

  // Floating point timestamp of the event in seconds (see WallTime_Now())
  double timestamp;

  // These fields are the standard pieces of a cache key: filename, mtime, offset
  // The one distinction is that the filename can be anonymized by being hashed.
  // See Tracer::AnonymizeFilename()
  std::string filename;
  int64_t mtime;
  int64_t offset;

  // The 'entry_length' is the size of the cache entry. For a HIT, this may be
  // larger or smaller than the lookup length. For a MISS, this is always -1.
  // For STORE*, this is the size of the cache entry being added.
  int64_t entry_length;
  // The lookup length is the size requested by the cache user. For a HIT/MISS,
  // this is the total size that the user requested. For STORE*, this is always -1.
  int64_t lookup_length;

  // Implement the == operator to make test logic cleaner
  bool operator==(const TraceEvent& rhs) const {
    return type == rhs.type && timestamp == rhs.timestamp &&
        filename == rhs.filename && mtime == rhs.mtime && offset == rhs.offset &&
        entry_length == rhs.entry_length && lookup_length == rhs.lookup_length;
  }
};

// Parse the json input and fill in a TraceEvent representing the data
Status JsonToTraceEvent(std::string json, TraceEvent* event);

// Construct a JSON entry from the provided TraceEvent
std::string TraceEventToJson(const TraceEvent& event);

/// The file name prefix pattern used for the access trace.
extern const string TRACE_FILE_PREFIX;

class Tracer {
 public:
  /// The Tracer records data cache trace events to a representation on disk.
  /// 'log_dir' is the directory that the Tracer uses for output. It is using
  /// the SimpleLogger under the covers, so it obeys the 'max_entries_per_file'
  /// and 'max_trace_files' to limit the total number of entries on disk.
  /// If 'anonymize_trace' is true, the filenames in the trace are anonymized
  /// by hashing them (see AnonymizeFilename()).
  explicit Tracer(std::string log_dir, uint64_t max_entries_per_file,
      int32_t max_trace_files, bool anonymize_trace);

  ~Tracer();

  // Initialize the tracer
  Status Init();

  // Force the logger to flush its buffers
  void Flush();

  // Record an entry into the log
  void Trace(EventType type, double timestamp, kudu::Slice filename,
      int64_t mtime, int64_t offset, int64_t lookup_len, int64_t entry_len);

 private:
  // The underlying logger that we wrap with the AsyncLogger wrapper
  // 'logger_'. NOTE: AsyncLogger consumes a raw pointer which must
  // outlive the AsyncLogger instance, so it's important that these
  // are declared in this order (logger_ must destruct before
  // underlying_logger_).
  std::unique_ptr<SimpleLoggerWrapper> underlying_logger_;
  // The async wrapper around underlying_logger_ (see above).
  std::unique_ptr<kudu::AsyncLogger> logger_;

  // Whether to anonymize the trace by hashing the filenames
  bool anonymize_trace_;

  // This anonymizes the filename by hashing it and encoding that in a Base64 string.
  // This returns a 22 character string.
  std::string AnonymizeFilename(kudu::Slice filename);
};

// Information about the cache outcomes for a particular workload
struct CacheHitStatistics {
  uint64_t hits = 0;
  uint64_t partial_hits = 0;
  uint64_t hit_bytes = 0;
  uint64_t misses = 0;
  uint64_t miss_bytes = 0;
  uint64_t stores = 0;
  uint64_t failed_stores = 0;
};

// The access trace has a TraceEvent JSON entry per line in the file. This is a helper
// class to iterate over the TraceEvents from a single file.
class TraceFileIterator {
 public:
  TraceFileIterator(std::string filename)
    : filename_(filename) {}

  // Initialize the iterator
  Status Init();

  // Get the next TraceEvent. If not at the end of the file, 'event' is filled with
  // the next TraceEvent. If at the end of the file, 'done' is set to true and
  // 'event' is untouched. Returns status in the event of an error.
  Status GetNextEvent(TraceEvent* event, bool* done);

 private:
  // File to iterate over
  std::string filename_;
  // Set to true in Init()
  bool initialized_ = false;
  // Input stream reading the file
  std::ifstream stream_;
};

class TraceReplayer {
 public:
  // The TraceReplayer creates a DataCache with the provided 'trace_configuration'
  // in the special trace replay mode. This allows replaying the trace without any
  // underlying data or filesystem operations. The 'trace_configuration' matches the
  // configuration format from the DataCache constructor. See data-cache.h.
  TraceReplayer(std::string trace_configuration);

  ~TraceReplayer();

  // Initialize the replayer
  Status Init();

  // Replay a single trace file
  Status ReplayFile(std::string filename);

  // Replay a directory of trace files as generated by the Tracer above.
  Status ReplayDirectory(std::string directory);

  // Get the hit statistics that are recorded in the original trace.
  CacheHitStatistics GetOriginalTraceStatistics() const {
    DCHECK(initialized_);
    return original_trace_stats_;
  }

  // Get the hit statistics for the replay of the trace.
  CacheHitStatistics GetReplayStatistics() const {
    DCHECK(initialized_);
    return replay_stats_;
  }

 private:

  // Update the original trace statistics given this TraceEvent
  void UpdateTraceStats(const TraceEvent& entry);

  // Replay an individual TraceEvent against the current cache.
  void ReplayEntry(const TraceEvent& entry);

  // Configuration of the cache used for the replay
  std::string trace_configuration_;

  // Set to true in Init().
  bool initialized_ = false;

  // DataCache used for the trace replay
  std::unique_ptr<DataCache> data_cache_;

  // The hit statistics that are recorded in the original trace. This is
  // purely a summation of the contents in the trace (i.e. hits, misses, etc).
  CacheHitStatistics original_trace_stats_;

  // The hit statistics for the replay of the trace. The outcomes of the
  // trace replay are independent from the original trace and are dependent
  // on the cache configuration. Even with equivalent cache configurations, the
  // outcome can differ from the original trace due to concurrency.
  CacheHitStatistics replay_stats_;
};
} // namespace trace
} // namespace io
} // namespace impala
