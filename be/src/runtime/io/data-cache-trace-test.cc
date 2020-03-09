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
#include <vector>

#include "common/status.h"
#include "kudu/util/slice.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {
namespace io {
namespace trace {

using boost::filesystem::path;
using kudu::Slice;
using strings::Substitute;

// Basic defaults for number of log files and entries per log file. These do not
// impact the correctness of tests.
static const int MAX_LOG_FILES = 10;
static const size_t MAX_ENTRIES_PER_FILE = 100;

class DataCacheTraceTest : public testing::Test {
protected:
  DataCacheTraceTest() : tmp_dir_("/tmp" / boost::filesystem::unique_path()) {}

  path tmp_dir() const { return tmp_dir_; }

  void SetUp() override {
    boost::filesystem::create_directories(tmp_dir());
  }

  void TearDown() override {
    boost::filesystem::remove_all(tmp_dir());
  }

  // Helper function to create a tracer without thinking about the number of files
  // or entries per file.
  unique_ptr<Tracer> CreateSimpleTracer(path directory, bool anonymize_trace = false) {
    return std::make_unique<Tracer>(directory.string(), MAX_ENTRIES_PER_FILE,
        MAX_LOG_FILES, anonymize_trace);
  }

  // Since most tests don't need to modify most fields, this provides a basic TraceEvent
  // to use as a template.
  TraceEvent GetTemplateTraceEvent() {
    TraceEvent event;
    event.type = EventType::HIT;
    event.timestamp = 1;
    event.filename = "fname.txt";
    event.mtime = 1;
    event.offset = 0;
    // Entry length and offset length should be filled in by caller
    event.entry_length = -1;
    event.lookup_length = -1;

    return event;
  }

  // Helper function to trace from a TraceEvent, allowing simpler test code
  void TraceFromTraceEvent(Tracer* tracer, const TraceEvent& event) {
    tracer->Trace(event.type, event.timestamp, Slice(event.filename), event.mtime,
        event.offset, event.lookup_length, event.entry_length);
  }

  // This constructs valid trace events of all types. It uses unique numbers for each
  // numeric field unless there is a required value (i.e. -1 for some event types).
  std::vector<TraceEvent> GetSampleTraceEvents() {
    std::vector<TraceEvent> events;
    events.emplace_back(EventType::HIT, 1, "hit_filename.txt", 2, 3, 4, 5);
    events.emplace_back(EventType::MISS, 6, "miss_filename.txt", 7, 8,
        /* entry_length */-1, 9);
    events.emplace_back(EventType::STORE, 10, "store_filename.txt", 11, 12, 13,
        /* lookup_length */-1);
    events.emplace_back(EventType::STORE_FAILED, 14, "store_failed.txt", 15, 16, 17,
        /* lookup_length */ -1);
    events.emplace_back(EventType::STORE_FAILED_BUSY, 18, "store_failed_busy.txt",
        19, 20, 21, /* lookup_length */ -1);
    return events;
  }

private:
  // Temporary directory for the test run
  path tmp_dir_;
};

TEST_F(DataCacheTraceTest, FromJSONFailures) {
  TraceEvent event;
  Status status;

  // Empty JSON (missing required fields)
  status = JsonToTraceEvent("{}", &event);
  EXPECT_FALSE(status.ok());

  // Invalid JSON
  status = JsonToTraceEvent("{ x = 1234", &event);
  EXPECT_FALSE(status.ok());

  // Gibberish
  status = JsonToTraceEvent("\\896437 a189047623fgdjkh", &event);
  EXPECT_FALSE(status.ok());
}

TEST_F(DataCacheTraceTest, ToFromJSON) {
  // Create an array of TraceEvents, go to JSON and back, verify the same
  vector<TraceEvent> sample_events = GetSampleTraceEvents();
  for (const TraceEvent& event : sample_events) {
    string json = TraceEventToJson(event);
    TraceEvent fromjson_event;
    EXPECT_OK(JsonToTraceEvent(json, &fromjson_event));
    EXPECT_TRUE(event == fromjson_event);
  }
}

TEST_F(DataCacheTraceTest, TraceReplayBasic1) {
  // Trace repeated hits to the same file, then replay them and verify appropriate
  // cache hits.
  path basictest_path = tmp_dir() / "basic1";
  unique_ptr<Tracer> tracer = CreateSimpleTracer(basictest_path);
  ASSERT_OK(tracer->Init());
  TraceEvent event = GetTemplateTraceEvent();

  // Five hits to the same location (provided by the template)
  event.type = EventType::HIT;
  event.lookup_length = 1024;
  event.entry_length = 1024;
  for (int i = 0; i < 5; ++i) {
    TraceFromTraceEvent(tracer.get(), event);
  }
  tracer->Flush();

  TraceReplayer replayer("/tmp:50MB");
  EXPECT_OK(replayer.Init());
  EXPECT_OK(replayer.ReplayDirectory(basictest_path.string()));

  // Every event was a HIT in the original trace
  CacheHitStatistics original_trace_stats = replayer.GetOriginalTraceStatistics();
  EXPECT_EQ(original_trace_stats.hits, 5);
  EXPECT_EQ(original_trace_stats.hit_bytes, 5120);
  EXPECT_EQ(original_trace_stats.partial_hits, 0);
  EXPECT_EQ(original_trace_stats.misses, 0);
  EXPECT_EQ(original_trace_stats.miss_bytes, 0);
  EXPECT_EQ(original_trace_stats.stores, 0);
  EXPECT_EQ(original_trace_stats.failed_stores, 0);

  // The replay stats are different from the original trace stats. The first access to
  // the location is a HIT in the original trace, but the replay doesn't have an entry
  // for the location yet. So, the replay has a MISS and then a STORE for the first
  // access to the location. The rest of the entries are the same.
  CacheHitStatistics replay_stats = replayer.GetReplayStatistics();
  EXPECT_EQ(replay_stats.hits, 4);
  EXPECT_EQ(replay_stats.hit_bytes, 4096);
  EXPECT_EQ(replay_stats.partial_hits, 0);
  EXPECT_EQ(replay_stats.misses, 1);
  EXPECT_EQ(replay_stats.miss_bytes, 1024);
  EXPECT_EQ(replay_stats.stores, 1);
  EXPECT_EQ(replay_stats.failed_stores, 0);
}

TEST_F(DataCacheTraceTest, TraceReplayBasic2) {
  // Trace and replay trace events that reference unique files (and thus won't be hits)
  path basictest_path = tmp_dir() / "basic2";
  unique_ptr<Tracer> tracer = CreateSimpleTracer(basictest_path);
  ASSERT_OK(tracer->Init());
  TraceEvent event = GetTemplateTraceEvent();

  // Five misses to different files
  event.type = EventType::MISS;
  event.lookup_length = 1024;
  event.entry_length = -1;
  for (int i = 0; i < 5; ++i) {
    event.filename = Substitute("file$0.txt", i);
    TraceFromTraceEvent(tracer.get(), event);
  }
  tracer->Flush();

  TraceReplayer replayer("/tmp:50MB");
  EXPECT_OK(replayer.Init());
  EXPECT_OK(replayer.ReplayDirectory(basictest_path.string()));

  CacheHitStatistics original_trace_stats = replayer.GetOriginalTraceStatistics();
  EXPECT_EQ(original_trace_stats.hits, 0);
  EXPECT_EQ(original_trace_stats.hit_bytes, 0);
  EXPECT_EQ(original_trace_stats.partial_hits, 0);
  EXPECT_EQ(original_trace_stats.misses, 5);
  EXPECT_EQ(original_trace_stats.miss_bytes, 5120);
  EXPECT_EQ(original_trace_stats.stores, 0);
  EXPECT_EQ(original_trace_stats.failed_stores, 0);

  // When the replayer sees a cache miss, it stores the entry to the cache, so the
  // replay trace stats will have an additional 5 STOREs compared to the original
  // trace stats.
  CacheHitStatistics replay_stats = replayer.GetReplayStatistics();
  EXPECT_EQ(replay_stats.hits, 0);
  EXPECT_EQ(replay_stats.hit_bytes, 0);
  EXPECT_EQ(replay_stats.partial_hits, 0);
  EXPECT_EQ(replay_stats.misses, 5);
  EXPECT_EQ(replay_stats.miss_bytes, 5120);
  EXPECT_EQ(replay_stats.stores, 5);
  EXPECT_EQ(replay_stats.failed_stores, 0);
}

TEST_F(DataCacheTraceTest, TraceReplayIgnoredEvents) {
  // The trace replayer only replays HIT/MISS events. Verify that other events are
  // ignored.
  path ignoredevents_path = tmp_dir() / "ignoredevents";
  unique_ptr<Tracer> tracer = CreateSimpleTracer(ignoredevents_path);
  ASSERT_OK(tracer->Init());
  TraceEvent event = GetTemplateTraceEvent();

  // Normal store
  event.type = EventType::STORE;
  event.lookup_length = -1;
  event.entry_length = 1024;
  TraceFromTraceEvent(tracer.get(), event);

  // Failed store (non-busy)
  event.type = EventType::STORE_FAILED;
  event.lookup_length = -1;
  event.entry_length = 1024;
  TraceFromTraceEvent(tracer.get(), event);

  // Failed stored (busy)
  event.type = EventType::STORE_FAILED_BUSY;
  event.lookup_length = -1;
  event.entry_length = 1024;
  TraceFromTraceEvent(tracer.get(), event);
  tracer->Flush();

  TraceReplayer replayer("/tmp:50MB");
  EXPECT_OK(replayer.Init());
  EXPECT_OK(replayer.ReplayDirectory(ignoredevents_path.string()));
  CacheHitStatistics original_trace_stats = replayer.GetOriginalTraceStatistics();
  EXPECT_EQ(original_trace_stats.hits, 0);
  EXPECT_EQ(original_trace_stats.hit_bytes, 0);
  EXPECT_EQ(original_trace_stats.partial_hits, 0);
  EXPECT_EQ(original_trace_stats.misses, 0);
  EXPECT_EQ(original_trace_stats.miss_bytes, 0);
  EXPECT_EQ(original_trace_stats.stores, 1);
  EXPECT_EQ(original_trace_stats.failed_stores, 2);

  // The replayer only cares about HIT and MISS events, so the replay trace stats
  // show nothing.
  CacheHitStatistics replay_stats = replayer.GetReplayStatistics();
  EXPECT_EQ(replay_stats.hits, 0);
  EXPECT_EQ(replay_stats.hit_bytes, 0);
  EXPECT_EQ(replay_stats.partial_hits, 0);
  EXPECT_EQ(replay_stats.misses, 0);
  EXPECT_EQ(replay_stats.miss_bytes, 0);
  EXPECT_EQ(replay_stats.stores, 0);
  EXPECT_EQ(replay_stats.failed_stores, 0);
}

TEST_F(DataCacheTraceTest, TraceReplayPartialHits) {
  // The trace replayer needs to replicate the data cache's behaviors for partial
  // hits.
  path partialhits_path = tmp_dir() / "partialhits";
  unique_ptr<Tracer> tracer = CreateSimpleTracer(partialhits_path);
  ASSERT_OK(tracer->Init());
  TraceEvent event = GetTemplateTraceEvent();

  // Initial miss loads small entry (512 bytes)
  event.type = EventType::MISS;
  event.lookup_length = 512;
  event.entry_length = -1;
  TraceFromTraceEvent(tracer.get(), event);

  // Store this small entry (skipped by replayer, but mimics what is happening)
  event.type = EventType::STORE;
  event.lookup_length = -1;
  event.entry_length = 512;
  TraceFromTraceEvent(tracer.get(), event);

  // Trying to read a larger element at the same location is a partial hit
  event.type = EventType::HIT;
  event.lookup_length = 1024;
  event.entry_length = 512;
  TraceFromTraceEvent(tracer.get(), event);

  // Store the larger element (skipped by replayer, but mimics what is happening)
  event.type = EventType::STORE;
  event.lookup_length = -1;
  event.entry_length = 1024;
  TraceFromTraceEvent(tracer.get(), event);

  // Trying to read the shorter length again is a hit
  event.type = EventType::HIT;
  event.lookup_length = 512;
  event.entry_length = 1024;
  TraceFromTraceEvent(tracer.get(), event);

  tracer->Flush();

  TraceReplayer replayer("/tmp:50MB");
  EXPECT_OK(replayer.Init());
  EXPECT_OK(replayer.ReplayDirectory(partialhits_path.string()));
  CacheHitStatistics original_trace_stats = replayer.GetOriginalTraceStatistics();
  EXPECT_EQ(original_trace_stats.hits, 1);
  EXPECT_EQ(original_trace_stats.hit_bytes, 1024);
  EXPECT_EQ(original_trace_stats.partial_hits, 1);
  EXPECT_EQ(original_trace_stats.misses, 1);
  EXPECT_EQ(original_trace_stats.miss_bytes, 1024);
  EXPECT_EQ(original_trace_stats.stores, 2);
  EXPECT_EQ(original_trace_stats.failed_stores, 0);

  // The events above were created in a way that the original trace is identical to
  // what the replay will do. So, the stats are identical.
  CacheHitStatistics replay_stats = replayer.GetReplayStatistics();
  EXPECT_EQ(replay_stats.hits, 1);
  EXPECT_EQ(replay_stats.hit_bytes, 1024);
  EXPECT_EQ(replay_stats.partial_hits, 1);
  EXPECT_EQ(replay_stats.misses, 1);
  EXPECT_EQ(replay_stats.miss_bytes, 1024);
  EXPECT_EQ(replay_stats.stores, 2);
  EXPECT_EQ(replay_stats.failed_stores, 0);
}
} // namespace trace
} // namespace io
} // namespace impala
