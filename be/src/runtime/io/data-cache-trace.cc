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

#include "runtime/io/data-cache-trace.h"

#include <boost/filesystem.hpp>
#include <fstream>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/writer.h>
#include <string>

#include "gutil/hash/city.h"
#include "gutil/strings/escaping.h"
#include "kudu/util/async_logger.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/path_util.h"
#include "runtime/io/data-cache.h"
#include "util/filesystem-util.h"
#include "util/simple-logger.h"

#include "common/names.h"

using kudu::Slice;
using kudu::JoinPathSegments;
using strings::Substitute;

namespace impala {
namespace io {
namespace trace {

const string TRACE_FILE_PREFIX = "impala_cache_trace-";

EventType StringToEventType(std::string s) {
  if (s == "H") return EventType::HIT;
  if (s == "M") return EventType::MISS;
  if (s == "S") return EventType::STORE;
  if (s == "B") return EventType::STORE_FAILED_BUSY;
  if (s == "F") return EventType::STORE_FAILED;
  CHECK(false) << "Unknown EventType string: " << s;
  return EventType::HIT;
}

std::string EventTypeToString(EventType s) {
  switch (s) {
  case EventType::HIT:
    return "H";
  case EventType::MISS:
    return "M";
  case EventType::STORE:
    return "S";
  case EventType::STORE_FAILED_BUSY:
    return "B";
  case EventType::STORE_FAILED:
    return "F";
  default:
    CHECK(false) << "Unknown EventType";
    return "Invalid";
  }
}

Status JsonToTraceEvent(std::string json, TraceEvent* event) {
  rapidjson::Document d;
  d.Parse<0>(json.c_str());
  // Check for any parse failure
  if (d.HasParseError()) {
    return Status(Substitute("Failed to parse TraceEvent JSON. Error: $0 JSON: $1",
        rapidjson::GetParseError_En(d.GetParseError()), json));
  }
  // Verify that this is an object and the required fields are present
  if (!d.IsObject() || !d.HasMember("ts") || !d.HasMember("s") || !d.HasMember("f") ||
      !d.HasMember("m") || !d.HasMember("o")) {
    return Status(Substitute("Invalid TraceEvent JSON: $0", json));
  }
  event->timestamp = d["ts"].GetDouble();
  event->type = StringToEventType(d["s"].GetString());
  event->filename = d["f"].GetString();
  event->mtime = d["m"].GetInt64();
  event->offset = d["o"].GetInt64();
  event->entry_length = d.HasMember("eLen") ? d["eLen"].GetInt64() : -1;
  event->lookup_length = d.HasMember("lLen") ? d["lLen"].GetInt64() : -1;
  return Status::OK();
}

std::string TraceEventToJson(const TraceEvent& event) {
    ostringstream buf;
  kudu::JsonWriter jw(&buf, kudu::JsonWriter::COMPACT);

  jw.StartObject();
  jw.String("ts");
  jw.Double(event.timestamp);
  jw.String("s");
  jw.String(EventTypeToString(event.type));
  jw.String("f");
  jw.String(event.filename);
  jw.String("m");
  jw.Int64(event.mtime);
  jw.String("o");
  jw.Int64(event.offset);

  if (event.lookup_length != -1) {
    jw.String("lLen");
    jw.Int64(event.lookup_length);
  }
  if (event.entry_length != -1) {
    jw.String("eLen");
    jw.Int64(event.entry_length);
  }
  jw.EndObject();

  return buf.str();
}

Status TraceFileIterator::Init() {
  boost::filesystem::path file_path(filename_);
  if (!boost::filesystem::exists(file_path)) {
    return Status(Substitute("Trace file does not exist: $0", filename_));
  }
  stream_.open(filename_);
  initialized_ = true;
  return Status::OK();
}

Status TraceFileIterator::GetNextEvent(TraceEvent* event, bool* done) {
  string line;
  DCHECK(initialized_);
  DCHECK(event != nullptr);
  DCHECK(done != nullptr);
  if (getline(stream_, line)) {
    RETURN_IF_ERROR(JsonToTraceEvent(line, event));
    *done = false;
  } else {
    *done = true;
  }
  return Status::OK();
}

// Simple implementation of a glog Logger that writes to a file, used for
// cache access tracing.
//
// This doesn't fully implement the Logger interface -- only the bare minimum
// to be usable with kudu::AsyncLogger.
class SimpleLoggerWrapper : public google::base::Logger {
 public:
  explicit SimpleLoggerWrapper(string log_dir, string log_file_name_prefix,
      uint64_t max_entries_per_file, int32_t max_log_files)
    : logger_(log_dir, log_file_name_prefix, max_entries_per_file, max_log_files) {}

  virtual ~SimpleLoggerWrapper() {
    Flush();
  }

  Status Open() {
    return logger_.Init();
  }

  void Write(bool force_flush,
             time_t timestamp,
             const char* message,
             int message_len) override {
    string message_str(message, message_len);
    Status status = logger_.AppendEntry(message_str);
    if (!status.ok()) {
      LOG_EVERY_N(WARNING, 1000) << "Could not write to data cache access trace ("
          << google::COUNTER << " attempts failed): " << status.GetDetail();
    }
    if (force_flush) Flush();
  }

  // Flush any buffered messages.
  // NOTE: declared 'final' to allow safe calls from the destructor.
  void Flush() override final {
    Status status = logger_.Flush();
    if (!status.ok()) {
      LOG_EVERY_N(WARNING, 1000) << "Could not write to data cache access trace ("
          << google::COUNTER << " attempts failed): " << status.GetDetail();
    }
  }

  uint32 LogSize() override {
    LOG(FATAL) << "Unimplemented";
    return 0;
  }

 private:
  SimpleLogger logger_;
};

Tracer::Tracer(string log_dir, uint64_t max_entries_per_file, int32_t max_log_files,
    bool anonymize_trace)
  : underlying_logger_(new SimpleLoggerWrapper(log_dir, TRACE_FILE_PREFIX,
        max_entries_per_file, max_log_files)),
    anonymize_trace_(anonymize_trace) {}

Tracer::~Tracer() {
  if (logger_) logger_->Stop();
}

Status Tracer::Init() {
  RETURN_IF_ERROR(underlying_logger_->Open());
  logger_.reset(new kudu::AsyncLogger(underlying_logger_.get(), 8 * 1024 * 1024));
  logger_->Start();
  return Status::OK();
}

void Tracer::Flush() {
  logger_->Flush();
}

void Tracer::Trace(EventType type, double timestamp, Slice filename, int64_t mtime,
    int64_t offset, int64_t lookup_len, int64_t entry_len) {

  // Sanity checks for lookup_len / entry_len
  if (type == EventType::HIT) {
    DCHECK(lookup_len > 0 && entry_len > 0);
  } else if (type == EventType::MISS) {
    DCHECK(lookup_len > 0 && entry_len == -1);
  } else if (type == EventType::STORE || type == EventType::STORE_FAILED ||
      type == EventType::STORE_FAILED_BUSY) {
    DCHECK(lookup_len == -1 && entry_len > 0);
  } else {
    DCHECK(false) << "Unrecognized EventType";
  }

  string filename_str =
      anonymize_trace_ ? AnonymizeFilename(filename) : filename.ToString();
  TraceEvent event(type, timestamp, filename_str, mtime, offset, entry_len,
      lookup_len);
  string json = TraceEventToJson(event);
  logger_->Write(/*force_flush="*/false, /*timestamp=*/0, json.data(), json.size());
}

string Tracer::AnonymizeFilename(Slice filename) {
  uint128 hash = util_hash::CityHash128(reinterpret_cast<const char*>(filename.data()),
      filename.size());
  // A 128-bit (16-byte) hash results in a 22-byte base64-encoded string. We opt to
  // generate the string without the typical two characters of padding.
  const int ESCAPED_LEN = 22;
  DCHECK_EQ(ESCAPED_LEN, CalculateBase64EscapedLen(sizeof(hash), /* padding */ false));
  string b64_out;
  Base64Escape(reinterpret_cast<const unsigned char*>(&hash), sizeof(hash),
      &b64_out, /* padding */ false);
  DCHECK_EQ(b64_out.size(), ESCAPED_LEN);
  return b64_out;
}

TraceReplayer::TraceReplayer(string trace_configuration)
  : trace_configuration_(trace_configuration) {}

TraceReplayer::~TraceReplayer() {}

Status TraceReplayer::Init() {
  data_cache_.reset(new DataCache(trace_configuration_, /* num_async_write_threads */ 0,
      /* trace_replay */ true));
  RETURN_IF_ERROR(data_cache_->Init());
  initialized_ = true;
  return Status::OK();
}

Status TraceReplayer::ReplayFile(string filename) {
  DCHECK(initialized_);
  TraceFileIterator file_iter(filename);
  RETURN_IF_ERROR(file_iter.Init());
  while (true) {
    bool done = false;
    TraceEvent trace_event;
    RETURN_IF_ERROR(file_iter.GetNextEvent(&trace_event, &done));
    if (done) break;
    ReplayEntry(trace_event);
  }
  return Status::OK();
}

Status TraceReplayer::ReplayDirectory(string directory) {
  DCHECK(initialized_);
  vector<string> trace_files;
  RETURN_IF_ERROR(SimpleLogger::GetLogFiles(directory, TRACE_FILE_PREFIX, &trace_files));
  for (const string& trace_file : trace_files) {
    RETURN_IF_ERROR(ReplayFile(trace_file));
  }
  return Status::OK();
}

void TraceReplayer::UpdateTraceStats(const TraceEvent& entry) {
  switch (entry.type) {
  case EventType::STORE:
    ++original_trace_stats_.stores;
    break;
  case EventType::STORE_FAILED:
  case EventType::STORE_FAILED_BUSY:
    ++original_trace_stats_.failed_stores;
    break;
  case EventType::HIT:
    if (entry.entry_length >= entry.lookup_length) {
      ++original_trace_stats_.hits;
      original_trace_stats_.hit_bytes += entry.lookup_length;
    } else {
      ++original_trace_stats_.partial_hits;
      original_trace_stats_.hit_bytes += entry.entry_length;
      original_trace_stats_.miss_bytes += entry.lookup_length - entry.entry_length;
    }
    break;
  case EventType::MISS:
    ++original_trace_stats_.misses;
    original_trace_stats_.miss_bytes += entry.lookup_length;
    break;
  default:
    CHECK(false) << "Invalid TraceEvent";
  }
}

void TraceReplayer::ReplayEntry(const TraceEvent& entry) {
  // First, update hit/miss counts as reported by the trace itself.
  UpdateTraceStats(entry);

  // Second, do the actual replay against the current cache (which may have different
  // settings from the original cache). This mirrors the behavior of DiskIoMgr.
  // Replay only needs hits and misses.
  if (entry.type != EventType::HIT && entry.type != EventType::MISS) {
    return;
  }
  DCHECK_GT(entry.lookup_length, 0);
  // Try to read the whole chunk from the cache. If it does a partial read,
  // the rest is a miss, but it will try to store the complete read into the cache.
  int64_t bytes_read = data_cache_->Lookup(entry.filename, entry.mtime, entry.offset,
      entry.lookup_length, /* buffer */ nullptr);
  DCHECK_LE(bytes_read, entry.lookup_length);
  if (bytes_read == 0) {
    // Complete miss, and we try to store the whole chunk into the cache
    ++replay_stats_.misses;
    replay_stats_.miss_bytes += entry.lookup_length;
    bool success = data_cache_->Store(entry.filename, entry.mtime, entry.offset,
        /* buffer */ nullptr, entry.lookup_length);
    if (success) {
      ++replay_stats_.stores;
    } else {
      ++replay_stats_.failed_stores;
    }
  } else if (bytes_read == entry.lookup_length) {
    // Total hit, nothing to store to the cache
    ++replay_stats_.hits;
    replay_stats_.hit_bytes += bytes_read;
  } else {
    // Partial hit, store complete entry to the cache
    ++replay_stats_.partial_hits;
    replay_stats_.hit_bytes += bytes_read;
    replay_stats_.miss_bytes += entry.lookup_length - bytes_read;
    bool success = data_cache_->Store(entry.filename, entry.mtime, entry.offset,
        /* buffer */ nullptr, entry.lookup_length);
    if (success) {
      ++replay_stats_.stores;
    } else {
      ++replay_stats_.failed_stores;
    }
  }
}
} // namespace trace
} // namespace io
} // namespace impala
