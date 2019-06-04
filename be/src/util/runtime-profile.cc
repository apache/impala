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

#include "util/runtime-profile-counters.h"

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <utility>

#include <boost/bind.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread.hpp>

#include "common/object-pool.h"
#include "gutil/strings/strip.h"
#include "kudu/util/logging.h"
#include "rpc/thrift-util.h"
#include "runtime/mem-tracker.h"
#include "util/coding-util.h"
#include "util/compress.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/periodic-counter-updater.h"
#include "util/pretty-printer.h"
#include "util/redactor.h"
#include "util/scope-exit-trigger.h"
#include "util/ubsan.h"

#include "common/names.h"

DECLARE_int32(status_report_interval_ms);
DECLARE_int32(periodic_counter_update_period_ms);

namespace impala {

// Thread counters name
static const string THREAD_TOTAL_TIME = "TotalWallClockTime";
static const string THREAD_USER_TIME = "UserTime";
static const string THREAD_SYS_TIME = "SysTime";
static const string THREAD_VOLUNTARY_CONTEXT_SWITCHES = "VoluntaryContextSwitches";
static const string THREAD_INVOLUNTARY_CONTEXT_SWITCHES = "InvoluntaryContextSwitches";

// The root counter name for all top level counters.
static const string ROOT_COUNTER = "";

const string RuntimeProfile::TOTAL_TIME_COUNTER_NAME = "TotalTime";
const string RuntimeProfile::LOCAL_TIME_COUNTER_NAME = "LocalTime";
const string RuntimeProfile::INACTIVE_TIME_COUNTER_NAME = "InactiveTotalTime";

RuntimeProfile* RuntimeProfile::Create(ObjectPool* pool, const string& name,
    bool is_averaged_profile) {
  return pool->Add(new RuntimeProfile(pool, name, is_averaged_profile));
}

RuntimeProfile::RuntimeProfile(
    ObjectPool* pool, const string& name, bool is_averaged_profile)
  : pool_(pool),
    name_(name),
    is_averaged_profile_(is_averaged_profile),
    counter_total_time_(TUnit::TIME_NS),
    inactive_timer_(TUnit::TIME_NS),
    local_time_percent_(0),
    local_time_ns_(0) {
  Counter* total_time_counter;
  Counter* inactive_timer;
  if (!is_averaged_profile) {
    total_time_counter = &counter_total_time_;
    inactive_timer = &inactive_timer_;
  } else {
    total_time_counter = pool->Add(new AveragedCounter(TUnit::TIME_NS));
    inactive_timer = pool->Add(new AveragedCounter(TUnit::TIME_NS));
  }
  counter_map_[TOTAL_TIME_COUNTER_NAME] = total_time_counter;
  counter_map_[INACTIVE_TIME_COUNTER_NAME] = inactive_timer;
}

RuntimeProfile::~RuntimeProfile() {
  DCHECK(!has_active_periodic_counters_);
}

void RuntimeProfile::StopPeriodicCounters() {
  lock_guard<SpinLock> l(counter_map_lock_);
  if (!has_active_periodic_counters_) return;
  for (Counter* sampling_counter : sampling_counters_) {
    PeriodicCounterUpdater::StopSamplingCounter(sampling_counter);
  }
  for (Counter* rate_counter : rate_counters_) {
    PeriodicCounterUpdater::StopRateCounter(rate_counter);
  }
  for (vector<Counter*>* counters : bucketing_counters_) {
    PeriodicCounterUpdater::StopBucketingCounters(counters);
  }
  for (auto& time_series_counter_entry : time_series_counter_map_) {
    PeriodicCounterUpdater::StopTimeSeriesCounter(time_series_counter_entry.second);
  }
  has_active_periodic_counters_ = false;
}

RuntimeProfile* RuntimeProfile::CreateFromThrift(ObjectPool* pool,
    const TRuntimeProfileTree& profiles) {
  if (profiles.nodes.size() == 0) return NULL;
  int idx = 0;
  RuntimeProfile* profile = RuntimeProfile::CreateFromThrift(pool, profiles.nodes, &idx);
  profile->SetTExecSummary(profiles.exec_summary);
  return profile;
}

RuntimeProfile* RuntimeProfile::CreateFromThrift(ObjectPool* pool,
    const vector<TRuntimeProfileNode>& nodes, int* idx) {
  DCHECK_LT(*idx, nodes.size());

  const TRuntimeProfileNode& node = nodes[*idx];
  RuntimeProfile* profile = Create(pool, node.name);
  profile->metadata_ = node.node_metadata;
  for (int i = 0; i < node.counters.size(); ++i) {
    const TCounter& counter = node.counters[i];
    profile->counter_map_[counter.name] =
        pool->Add(new Counter(counter.unit, counter.value));
  }

  if (node.__isset.event_sequences) {
    for (const TEventSequence& sequence: node.event_sequences) {
      profile->event_sequence_map_[sequence.name] =
          pool->Add(new EventSequence(sequence.timestamps, sequence.labels));
    }
  }

  if (node.__isset.time_series_counters) {
    for (const TTimeSeriesCounter& val: node.time_series_counters) {
      // Capture all incoming time series counters with the same type since re-sampling
      // will have happened on the sender side.
      profile->time_series_counter_map_[val.name] = pool->Add(
          new ChunkedTimeSeriesCounter(val.name, val.unit, val.period_ms, val.values));
    }
  }

  if (node.__isset.summary_stats_counters) {
    for (const TSummaryStatsCounter& val: node.summary_stats_counters) {
      profile->summary_stats_map_[val.name] =
          pool->Add(new SummaryStatsCounter(
              val.unit, val.total_num_values, val.min_value, val.max_value, val.sum));
    }
  }

  profile->child_counter_map_ = node.child_counters_map;
  profile->info_strings_ = node.info_strings;
  profile->info_strings_display_order_ = node.info_strings_display_order;

  ++*idx;
  for (int i = 0; i < node.num_children; ++i) {
    profile->AddChild(RuntimeProfile::CreateFromThrift(pool, nodes, idx));
  }
  return profile;
}

void RuntimeProfile::UpdateAverage(RuntimeProfile* other) {
  DCHECK(other != NULL);
  DCHECK(is_averaged_profile_);

  // Merge this level
  {
    CounterMap::iterator dst_iter;
    CounterMap::const_iterator src_iter;
    lock_guard<SpinLock> l(counter_map_lock_);
    lock_guard<SpinLock> m(other->counter_map_lock_);
    for (src_iter = other->counter_map_.begin();
         src_iter != other->counter_map_.end(); ++src_iter) {

      // Ignore this counter for averages.
      if (src_iter->first == INACTIVE_TIME_COUNTER_NAME) continue;

      dst_iter = counter_map_.find(src_iter->first);
      AveragedCounter* avg_counter;

      // Get the counter with the same name in dst_iter (this->counter_map_)
      // Create one if it doesn't exist.
      if (dst_iter == counter_map_.end()) {
        avg_counter = pool_->Add(new AveragedCounter(src_iter->second->unit()));
        counter_map_[src_iter->first] = avg_counter;
      } else {
        DCHECK(dst_iter->second->unit() == src_iter->second->unit());
        avg_counter = static_cast<AveragedCounter*>(dst_iter->second);
      }
      avg_counter->UpdateCounter(src_iter->second);
    }

    // TODO: Can we unlock the counter_map_lock_ here?
    ChildCounterMap::const_iterator child_counter_src_itr;
    for (child_counter_src_itr = other->child_counter_map_.begin();
         child_counter_src_itr != other->child_counter_map_.end();
         ++child_counter_src_itr) {
      set<string>* child_counters = FindOrInsert(&child_counter_map_,
          child_counter_src_itr->first, set<string>());
      child_counters->insert(child_counter_src_itr->second.begin(),
          child_counter_src_itr->second.end());
    }
  }

  {
    lock_guard<SpinLock> l(children_lock_);
    lock_guard<SpinLock> m(other->children_lock_);
    // Recursively merge children with matching names.
    // Track the current position in the vector so we preserve the order of children
    // if children are added after the first Update()/UpdateAverage() call (IMPALA-6694).
    // E.g. if the first update sends [B, D] and the second update sends [A, B, C, D],
    // then this code makes sure that children_ is [A, B, C, D] afterwards.
    ChildVector::iterator insert_pos = children_.begin();
    for (int i = 0; i < other->children_.size(); ++i) {
      RuntimeProfile* other_child = other->children_[i].first;
      ChildMap::iterator j = child_map_.find(other_child->name_);
      RuntimeProfile* child = NULL;
      if (j != child_map_.end()) {
        child = j->second;
        // Search forward until the insert position is either at the end of the vector
        // or after this child. This preserves the order if the relative order of
        // children in all updates is consistent.
        bool found_child = false;
        while (insert_pos != children_.end() && !found_child) {
          found_child = insert_pos->first == child;
          ++insert_pos;
        }
      } else {
        child = Create(pool_, other_child->name_, true);
        child->metadata_ = other_child->metadata_;
        bool indent_other_child = other->children_[i].second;
        child_map_[child->name_] = child;
        insert_pos = children_.insert(insert_pos, make_pair(child, indent_other_child));
        ++insert_pos;
      }
      child->UpdateAverage(other_child);
    }
  }

  ComputeTimeInProfile();
}

void RuntimeProfile::Update(const TRuntimeProfileTree& thrift_profile) {
  int idx = 0;
  Update(thrift_profile.nodes, &idx);
  DCHECK_EQ(idx, thrift_profile.nodes.size());
}

void RuntimeProfile::Update(const vector<TRuntimeProfileNode>& nodes, int* idx) {
  if (UNLIKELY(nodes.size()) == 0) return;
  DCHECK_LT(*idx, nodes.size());
  const TRuntimeProfileNode& node = nodes[*idx];
  {
    // Update this level.
    map<string, Counter*>::iterator dst_iter;
    lock_guard<SpinLock> l(counter_map_lock_);
    for (int i = 0; i < node.counters.size(); ++i) {
      const TCounter& tcounter = node.counters[i];
      CounterMap::iterator j = counter_map_.find(tcounter.name);
      if (j == counter_map_.end()) {
        counter_map_[tcounter.name] =
          pool_->Add(new Counter(tcounter.unit, tcounter.value));
      } else {
        if (j->second->unit() != tcounter.unit) {
          LOG(ERROR) << "Cannot update counters with the same name ("
                     << j->first << ") but different units.";
        } else {
          j->second->Set(tcounter.value);
        }
      }
    }

    ChildCounterMap::const_iterator child_counter_src_itr;
    for (child_counter_src_itr = node.child_counters_map.begin();
         child_counter_src_itr != node.child_counters_map.end();
         ++child_counter_src_itr) {
      set<string>* child_counters = FindOrInsert(&child_counter_map_,
          child_counter_src_itr->first, set<string>());
      child_counters->insert(child_counter_src_itr->second.begin(),
          child_counter_src_itr->second.end());
    }
  }

  {
    const InfoStrings& info_strings = node.info_strings;
    lock_guard<SpinLock> l(info_strings_lock_);
    for (const string& key: node.info_strings_display_order) {
      // Look for existing info strings and update in place. If there
      // are new strings, add them to the end of the display order.
      // TODO: Is nodes.info_strings always a superset of
      // info_strings_? If so, can just copy the display order.
      InfoStrings::const_iterator it = info_strings.find(key);
      DCHECK(it != info_strings.end());
      InfoStrings::iterator existing = info_strings_.find(key);
      if (existing == info_strings_.end()) {
        info_strings_.emplace(key, it->second);
        info_strings_display_order_.push_back(key);
      } else {
        info_strings_[key] = it->second;
      }
    }
  }

  {
    lock_guard<SpinLock> l(counter_map_lock_);
    for (int i = 0; i < node.time_series_counters.size(); ++i) {
      const TTimeSeriesCounter& c = node.time_series_counters[i];
      TimeSeriesCounterMap::iterator it = time_series_counter_map_.find(c.name);
      if (it == time_series_counter_map_.end()) {
        // Capture all incoming time series counters with the same type since re-sampling
        // will have happened on the sender side.
        time_series_counter_map_[c.name] = pool_->Add(
            new ChunkedTimeSeriesCounter(c.name, c.unit, c.period_ms, c.values));
      } else {
        int64_t start_idx = c.__isset.start_index ? c.start_index : 0;
        it->second->SetSamples(c.period_ms, c.values, start_idx);
      }
    }
  }

  {
    lock_guard<SpinLock> l(event_sequence_lock_);
    for (int i = 0; i < node.event_sequences.size(); ++i) {
      const TEventSequence& seq = node.event_sequences[i];
      EventSequenceMap::iterator it = event_sequence_map_.find(seq.name);
      if (it == event_sequence_map_.end()) {
        event_sequence_map_[seq.name] =
            pool_->Add(new EventSequence(seq.timestamps, seq.labels));
      } else {
        it->second->AddNewerEvents(seq.timestamps, seq.labels);
      }
    }
  }

  {
    lock_guard<SpinLock> l(summary_stats_map_lock_);
    for (int i = 0; i < node.summary_stats_counters.size(); ++i) {
      const TSummaryStatsCounter& c = node.summary_stats_counters[i];
      SummaryStatsCounterMap::iterator it = summary_stats_map_.find(c.name);
      if (it == summary_stats_map_.end()) {
        summary_stats_map_[c.name] =
            pool_->Add(new SummaryStatsCounter(
                c.unit, c.total_num_values, c.min_value, c.max_value, c.sum));
      } else {
        it->second->SetStats(c);
      }
    }
  }

  ++*idx;
  {
    lock_guard<SpinLock> l(children_lock_);
    // Track the current position in the vector so we preserve the order of children
    // if children are added after the first Update()/UpdateAverage() call (IMPALA-6694).
    // E.g. if the first update sends [B, D] and the second update sends [A, B, C, D],
    // then this code makes sure that children_ is [A, B, C, D] afterwards.
    ChildVector::iterator insert_pos = children_.begin();
    // Update children with matching names; create new ones if they don't match.
    for (int i = 0; i < node.num_children; ++i) {
      const TRuntimeProfileNode& tchild = nodes[*idx];
      ChildMap::iterator j = child_map_.find(tchild.name);
      RuntimeProfile* child = NULL;
      if (j != child_map_.end()) {
        child = j->second;
        // Search forward until the insert position is either at the end of the vector
        // or after this child. This preserves the order if the relative order of
        // children in all updates is consistent.
        bool found_child = false;
        while (insert_pos != children_.end() && !found_child) {
          found_child = insert_pos->first == child;
          ++insert_pos;
        }
      } else {
        child = Create(pool_, tchild.name);
        child->metadata_ = tchild.node_metadata;
        child_map_[tchild.name] = child;
        insert_pos = children_.insert(insert_pos, make_pair(child, tchild.indent));
        ++insert_pos;
      }
      child->Update(nodes, idx);
    }
  }
}

void RuntimeProfile::Divide(int n) {
  DCHECK_GT(n, 0);
  map<string, Counter*>::iterator iter;
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    for (iter = counter_map_.begin(); iter != counter_map_.end(); ++iter) {
      if (iter->second->unit() == TUnit::DOUBLE_VALUE) {
        iter->second->Set(iter->second->double_value() / n);
      } else {
        iter->second->value_.Store(iter->second->value() / n);
      }
    }
  }
  {
    lock_guard<SpinLock> l(children_lock_);
    for (ChildMap::iterator i = child_map_.begin(); i != child_map_.end(); ++i) {
      i->second->Divide(n);
    }
  }
}

void RuntimeProfile::ComputeTimeInProfile() {
  ComputeTimeInProfile(total_time_counter()->value());
}

void RuntimeProfile::ComputeTimeInProfile(int64_t total) {
  // Recurse on children. After this, childrens' total time is up to date.
  {
    lock_guard<SpinLock> l(children_lock_);
    for (int i = 0; i < children_.size(); ++i) {
      children_[i].first->ComputeTimeInProfile();
    }
  }

  // Get total time from children
  int64_t children_total_time = 0;
  {
    lock_guard<SpinLock> l(children_lock_);
    for (int i = 0; i < children_.size(); ++i) {
      children_total_time += children_[i].first->total_time();
    }
  }
  // IMPALA-5200: Take the max, because the parent includes all of the time from the
  // children, whether or not its total time counter has been updated recently enough
  // to see this.
  total_time_ns_ = max(children_total_time, total_time_counter()->value());

  // If a local time counter exists, use its value as local time. Otherwise, derive the
  // local time from total time and the child time.
  bool has_local_time_counter = false;
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    CounterMap::const_iterator itr = counter_map_.find(LOCAL_TIME_COUNTER_NAME);
    if (itr != counter_map_.end()) {
      local_time_ns_ = itr->second->value();
      has_local_time_counter = true;
    }
  }

  if (!has_local_time_counter) {
    local_time_ns_ = total_time_ns_ - children_total_time;
    if (!is_averaged_profile_) {
      local_time_ns_ -= inactive_timer()->value();
    }
  }
  // Counters have some margin, set to 0 if it was negative.
  local_time_ns_ = ::max<int64_t>(0, local_time_ns_);
  local_time_percent_ =
      static_cast<double>(local_time_ns_) / total_time_ns_;
  local_time_percent_ = ::min(1.0, local_time_percent_) * 100;
}

void RuntimeProfile::AddChild(RuntimeProfile* child, bool indent, RuntimeProfile* loc) {
  lock_guard<SpinLock> l(children_lock_);
  ChildVector::iterator insert_pos;
  if (loc == NULL) {
    insert_pos = children_.end();
  } else {
    bool found = false;
    for (ChildVector::iterator it = children_.begin(); it != children_.end(); ++it) {
      if (it->first == loc) {
        insert_pos = it + 1;
        found = true;
        break;
      }
    }
    DCHECK(found) << "Invalid loc";
  }
  AddChildLocked(child, indent, insert_pos);
}

void RuntimeProfile::AddChildLocked(
    RuntimeProfile* child, bool indent, ChildVector::iterator insert_pos) {
  children_lock_.DCheckLocked();
  DCHECK(child != NULL);
  if (child_map_.count(child->name_) > 0) {
    // This child has already been added, so do nothing.
    // Otherwise, the map and vector will be out of sync.
    return;
  }
  child_map_[child->name_] = child;
  children_.insert(insert_pos, make_pair(child, indent));
}

void RuntimeProfile::PrependChild(RuntimeProfile* child, bool indent) {
  lock_guard<SpinLock> l(children_lock_);
  AddChildLocked(child, indent, children_.begin());
}

RuntimeProfile* RuntimeProfile::CreateChild(const string& name, bool indent,
    bool prepend) {
  lock_guard<SpinLock> l(children_lock_);
  DCHECK(child_map_.find(name) == child_map_.end());
  RuntimeProfile* child = Create(pool_, name);
  AddChildLocked(child, indent, prepend ? children_.begin() : children_.end());
  return child;
}

void RuntimeProfile::GetChildren(vector<RuntimeProfile*>* children) {
  children->clear();
  lock_guard<SpinLock> l(children_lock_);
  for (const auto& entry : children_) children->push_back(entry.first);
}

void RuntimeProfile::GetAllChildren(vector<RuntimeProfile*>* children) {
  lock_guard<SpinLock> l(children_lock_);
  for (ChildMap::iterator i = child_map_.begin(); i != child_map_.end(); ++i) {
    children->push_back(i->second);
    i->second->GetAllChildren(children);
  }
}

void RuntimeProfile::SortChildrenByTotalTime() {
  lock_guard<SpinLock> l(children_lock_);
  // Create a snapshot of total time values so that they don't change while we're
  // sorting. Sort the <total_time, index> pairs, then reshuffle children_.
  vector<pair<int64_t, int64_t>> total_times;
  for (int i = 0; i < children_.size(); ++i) {
    total_times.emplace_back(children_[i].first->total_time_counter()->value(), i);
  }
  // Order by descending total time.
  sort(total_times.begin(), total_times.end(),
      [](const pair<int64_t, int64_t>& p1, const pair<int64_t, int64_t>& p2) {
        return p1.first > p2.first;
      });
  ChildVector new_children;
  for (const auto& p : total_times) new_children.emplace_back(children_[p.second]);
  children_ = move(new_children);
}

void RuntimeProfile::AddInfoString(const string& key, const string& value) {
  return AddInfoStringInternal(key, value, false);
}

void RuntimeProfile::AddInfoStringRedacted(const string& key, const string& value) {
  return AddInfoStringInternal(key, value, false, true);
}

void RuntimeProfile::AppendInfoString(const string& key, const string& value) {
  return AddInfoStringInternal(key, value, true);
}

void RuntimeProfile::AddInfoStringInternal(const string& key, string value,
    bool append, bool redact) {

  if (redact) Redact(&value);

  StripTrailingWhitespace(&value);

  lock_guard<SpinLock> l(info_strings_lock_);
  InfoStrings::iterator it = info_strings_.find(key);
  if (it == info_strings_.end()) {
    info_strings_.emplace(key, std::move(value));
    info_strings_display_order_.push_back(key);
  } else {
    if (append) {
      it->second += ", " + std::move(value);
    } else {
      it->second = std::move(value);
    }
  }
}

void RuntimeProfile::UpdateInfoString(const string& key, string value) {
  lock_guard<SpinLock> l(info_strings_lock_);
  InfoStrings::iterator it = info_strings_.find(key);
  if (it != info_strings_.end()) it->second = std::move(value);
}

const string* RuntimeProfile::GetInfoString(const string& key) const {
  lock_guard<SpinLock> l(info_strings_lock_);
  InfoStrings::const_iterator it = info_strings_.find(key);
  if (it == info_strings_.end()) return NULL;
  return &it->second;
}

void RuntimeProfile::AddCodegenMsg(
    bool codegen_enabled, const string& extra_info, const string& extra_label) {
  string str = codegen_enabled ? "Codegen Enabled" : "Codegen Disabled";
  if (!extra_info.empty()) str = str + ": " + extra_info;
  if (!extra_label.empty()) str = extra_label + " " + str;
  AppendExecOption(str);
}

#define ADD_COUNTER_IMPL(NAME, T)                                                \
  RuntimeProfile::T* RuntimeProfile::NAME(                                       \
      const string& name, TUnit::type unit, const string& parent_counter_name) { \
    lock_guard<SpinLock> l(counter_map_lock_);                                   \
    bool dummy;                                                                  \
    return NAME##Locked(name, unit, parent_counter_name, &dummy);                \
  }                                                                              \
  RuntimeProfile::T* RuntimeProfile::NAME##Locked( const string& name,           \
      TUnit::type unit, const string& parent_counter_name, bool* created) {      \
    counter_map_lock_.DCheckLocked();                                            \
    DCHECK_EQ(is_averaged_profile_, false);                                      \
    if (counter_map_.find(name) != counter_map_.end()) {                         \
      *created = false;                                                          \
      return reinterpret_cast<T*>(counter_map_[name]);                           \
    }                                                                            \
    DCHECK(parent_counter_name == ROOT_COUNTER                                   \
        || counter_map_.find(parent_counter_name) != counter_map_.end());        \
    T* counter = pool_->Add(new T(unit));                                        \
    counter_map_[name] = counter;                                                \
    set<string>* child_counters =                                                \
        FindOrInsert(&child_counter_map_, parent_counter_name, set<string>());   \
    child_counters->insert(name);                                                \
    *created = true;                                                             \
    return counter;                                                              \
  }

ADD_COUNTER_IMPL(AddCounter, Counter);
ADD_COUNTER_IMPL(AddHighWaterMarkCounter, HighWaterMarkCounter);
ADD_COUNTER_IMPL(AddConcurrentTimerCounter, ConcurrentTimerCounter);

RuntimeProfile::DerivedCounter* RuntimeProfile::AddDerivedCounter(
    const string& name, TUnit::type unit,
    const SampleFunction& counter_fn, const string& parent_counter_name) {
  DCHECK_EQ(is_averaged_profile_, false);
  lock_guard<SpinLock> l(counter_map_lock_);
  if (counter_map_.find(name) != counter_map_.end()) return NULL;
  DerivedCounter* counter = pool_->Add(new DerivedCounter(unit, counter_fn));
  counter_map_[name] = counter;
  set<string>* child_counters =
      FindOrInsert(&child_counter_map_, parent_counter_name, set<string>());
  child_counters->insert(name);
  return counter;
}

RuntimeProfile::ThreadCounters* RuntimeProfile::AddThreadCounters(
    const string& prefix) {
  ThreadCounters* counter = pool_->Add(new ThreadCounters());
  counter->total_time_ = AddCounter(prefix + THREAD_TOTAL_TIME, TUnit::TIME_NS);
  counter->user_time_ = AddCounter(prefix + THREAD_USER_TIME, TUnit::TIME_NS,
      prefix + THREAD_TOTAL_TIME);
  counter->sys_time_ = AddCounter(prefix + THREAD_SYS_TIME, TUnit::TIME_NS,
      prefix + THREAD_TOTAL_TIME);
  counter->voluntary_context_switches_ =
      AddCounter(prefix + THREAD_VOLUNTARY_CONTEXT_SWITCHES, TUnit::UNIT);
  counter->involuntary_context_switches_ =
      AddCounter(prefix + THREAD_INVOLUNTARY_CONTEXT_SWITCHES, TUnit::UNIT);
  return counter;
}

void RuntimeProfile::AddLocalTimeCounter(const SampleFunction& counter_fn) {
  DerivedCounter* local_time_counter = pool_->Add(
      new DerivedCounter(TUnit::TIME_NS, counter_fn));
  lock_guard<SpinLock> l(counter_map_lock_);
  DCHECK(counter_map_.find(LOCAL_TIME_COUNTER_NAME) == counter_map_.end())
      << "LocalTimeCounter already exists in the map.";
  counter_map_[LOCAL_TIME_COUNTER_NAME] = local_time_counter;
}

RuntimeProfile::Counter* RuntimeProfile::GetCounter(const string& name) {
  lock_guard<SpinLock> l(counter_map_lock_);
  if (counter_map_.find(name) != counter_map_.end()) {
    return counter_map_[name];
  }
  return NULL;
}

RuntimeProfile::SummaryStatsCounter* RuntimeProfile::GetSummaryStatsCounter(
    const string& name) {
  lock_guard<SpinLock> l(summary_stats_map_lock_);
  if (summary_stats_map_.find(name) != summary_stats_map_.end()) {
    return summary_stats_map_[name];
  }
  return nullptr;
}

void RuntimeProfile::GetCounters(const string& name, vector<Counter*>* counters) {
  Counter* c = GetCounter(name);
  if (c != NULL) counters->push_back(c);

  lock_guard<SpinLock> l(children_lock_);
  for (int i = 0; i < children_.size(); ++i) {
    children_[i].first->GetCounters(name, counters);
  }
}

RuntimeProfile::EventSequence* RuntimeProfile::GetEventSequence(const string& name) const
{
  lock_guard<SpinLock> l(event_sequence_lock_);
  EventSequenceMap::const_iterator it = event_sequence_map_.find(name);
  if (it == event_sequence_map_.end()) return NULL;
  return it->second;
}

// Print the profile:
//  1. Profile Name
//  2. Info Strings
//  3. Counters
//  4. Children
void RuntimeProfile::PrettyPrint(ostream* s, const string& prefix) const {
  ostream& stream = *s;

  // Create copy of counter_map_ and child_counter_map_ so we don't need to hold lock
  // while we call value() on the counters (some of those might be DerivedCounters).
  CounterMap counter_map;
  ChildCounterMap child_counter_map;
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    counter_map = counter_map_;
    child_counter_map = child_counter_map_;
  }

  map<string, Counter*>::const_iterator total_time =
      counter_map.find(TOTAL_TIME_COUNTER_NAME);
  DCHECK(total_time != counter_map.end());

  stream.flags(ios::fixed);
  stream << prefix << name_ << ":";
  if (total_time->second->value() != 0) {
    stream << "(Total: "
           << PrettyPrinter::Print(total_time->second->value(),
               total_time->second->unit())
           << ", non-child: "
           << PrettyPrinter::Print(local_time_ns_, TUnit::TIME_NS)
           << ", % non-child: "
           << setprecision(2) << local_time_percent_
           << "%)";
  }
  stream << endl;

  {
    lock_guard<SpinLock> l(info_strings_lock_);
    for (const string& key: info_strings_display_order_) {
      stream << prefix << "  " << key << ": " << info_strings_.find(key)->second << endl;
    }
  }

  {
    // Print all the event timers as the following:
    // <EventKey> Timeline: 2s719ms
    //     - Event 1: 6.522us (6.522us)
    //     - Event 2: 2s288ms (2s288ms)
    //     - Event 3: 2s410ms (121.138ms)
    // The times in parentheses are the time elapsed since the last event.
    vector<EventSequence::Event> events;
    lock_guard<SpinLock> l(event_sequence_lock_);
    for (const EventSequenceMap::value_type& event_sequence: event_sequence_map_) {
      // If the stopwatch has never been started (e.g. because this sequence came from
      // Thrift), look for the last element to tell us the total runtime. For
      // currently-updating sequences, it's better to use the stopwatch value because that
      // updates continuously.
      int64_t last = event_sequence.second->ElapsedTime();
      event_sequence.second->GetEvents(&events);
      if (last == 0 && events.size() > 0) last = events.back().second;
      stream << prefix << "  " << event_sequence.first << ": "
             << PrettyPrinter::Print(last, TUnit::TIME_NS)
             << endl;

      int64_t prev = 0L;
      event_sequence.second->GetEvents(&events);
      for (const EventSequence::Event& event: events) {
        stream << prefix << "     - " << event.first << ": "
               << PrettyPrinter::Print(event.second, TUnit::TIME_NS) << " ("
               << PrettyPrinter::Print(event.second - prev, TUnit::TIME_NS) << ")"
               << endl;
        prev = event.second;
      }
    }
  }

  {
    // Print all time series counters as following:
    //    - <Name> (<period>): <val1>, <val2>, <etc>
    lock_guard<SpinLock> l(counter_map_lock_);
    for (const TimeSeriesCounterMap::value_type& v: time_series_counter_map_) {
      const TimeSeriesCounter* counter = v.second;
      lock_guard<SpinLock> l(counter->lock_);
      int num, period;
      const int64_t* samples = counter->GetSamplesLocked(&num, &period);
      if (num > 0) {
        // Clamp number of printed values at 64, the maximum number of values in the
        // SamplingTimeSeriesCounter.
        int step = 1 + (num - 1) / 64;
        period *= step;
        stream << prefix << "   - " << v.first << " ("
               << PrettyPrinter::Print(period * 1000000L, TUnit::TIME_NS) << "): ";
        for (int i = 0; i < num; i += step) {
          stream << PrettyPrinter::Print(samples[i], counter->unit());
          if (i + step < num) stream << ", ";
        }
        if (step > 1) {
          stream << " (Showing " << ((num + 1) / step) << " of " << num << " values from "
            "Thrift Profile)";
        }
        stream << endl;
      }
    }
  }

  {
    lock_guard<SpinLock> l(summary_stats_map_lock_);
    // Print all SummaryStatsCounters as following:
    // <Name>: (Avg: <value> ; Min: <min_value> ; Max: <max_value> ;
    // Number of samples: <total>)
    for (const SummaryStatsCounterMap::value_type& v: summary_stats_map_) {
      if (v.second->TotalNumValues() == 0) {
        // No point printing all the stats if number of samples is zero.
        stream << prefix << "   - " << v.first << ": "
               << PrettyPrinter::Print(v.second->value(), v.second->unit(), true)
               << " (Number of samples: " << v.second->TotalNumValues() << ")" << endl;
      } else {
        stream << prefix << "   - " << v.first << ": (Avg: "
               << PrettyPrinter::Print(v.second->value(), v.second->unit(), true)
               << " ; Min: "
               << PrettyPrinter::Print(v.second->MinValue(), v.second->unit(), true)
               << " ; Max: "
               << PrettyPrinter::Print(v.second->MaxValue(), v.second->unit(), true)
               << " ; Number of samples: " << v.second->TotalNumValues() << ")" << endl;
      }
    }
  }
  RuntimeProfile::PrintChildCounters(
      prefix, ROOT_COUNTER, counter_map, child_counter_map, s);

  // Create copy of children_ so we don't need to hold lock while we call
  // PrettyPrint() on the children.
  ChildVector children;
  {
    lock_guard<SpinLock> l(children_lock_);
    children = children_;
  }
  for (int i = 0; i < children.size(); ++i) {
    RuntimeProfile* profile = children[i].first;
    bool indent = children[i].second;
    profile->PrettyPrint(s, prefix + (indent ? "  " : ""));
  }
}

Status RuntimeProfile::SerializeToArchiveString(string* out) const {
  stringstream ss;
  RETURN_IF_ERROR(SerializeToArchiveString(&ss));
  *out = ss.str();
  return Status::OK();
}

Status RuntimeProfile::SerializeToArchiveString(stringstream* out) const {
  Status status;
  TRuntimeProfileTree thrift_object;
  const_cast<RuntimeProfile*>(this)->ToThrift(&thrift_object);
  ThriftSerializer serializer(true);
  vector<uint8_t> serialized_buffer;
  RETURN_IF_ERROR(serializer.SerializeToVector(&thrift_object, &serialized_buffer));

  // Compress the serialized thrift string.  This uses string keys and is very
  // easy to compress.
  scoped_ptr<Codec> compressor;
  Codec::CodecInfo codec_info(THdfsCompression::DEFAULT);
  RETURN_IF_ERROR(Codec::CreateCompressor(NULL, false, codec_info, &compressor));
  const auto close_compressor =
      MakeScopeExitTrigger([&compressor]() { compressor->Close(); });

  vector<uint8_t> compressed_buffer;
  int64_t max_compressed_size = compressor->MaxOutputLen(serialized_buffer.size());
  DCHECK_GT(max_compressed_size, 0);
  compressed_buffer.resize(max_compressed_size);
  int64_t result_len = compressed_buffer.size();
  uint8_t* compressed_buffer_ptr = compressed_buffer.data();
  RETURN_IF_ERROR(compressor->ProcessBlock(true, serialized_buffer.size(),
      serialized_buffer.data(), &result_len, &compressed_buffer_ptr));
  compressed_buffer.resize(result_len);

  Base64Encode(compressed_buffer, out);
  return Status::OK();;
}

Status RuntimeProfile::DeserializeFromArchiveString(
    const std::string& archive_str, TRuntimeProfileTree* out) {
  int64_t decoded_max;
  if (!Base64DecodeBufLen(archive_str.c_str(), archive_str.size(), &decoded_max)) {
    return Status("Error in DeserializeFromArchiveString: Base64DecodeBufLen failed.");
  }

  vector<uint8_t> decoded_buffer;
  decoded_buffer.resize(decoded_max);
  int64_t decoded_len;
  if (!Base64Decode(archive_str.c_str(), archive_str.size(), decoded_max,
          reinterpret_cast<char*>(decoded_buffer.data()), &decoded_len)) {
    return Status("Error in DeserializeFromArchiveString: Base64Decode failed.");
  }
  decoded_buffer.resize(decoded_len);

  scoped_ptr<Codec> decompressor;
  MemTracker mem_tracker;
  MemPool mem_pool(&mem_tracker);
  const auto close_mem_tracker = MakeScopeExitTrigger([&mem_pool, &mem_tracker]() {
    mem_pool.FreeAll();
    mem_tracker.Close();
  });
  RETURN_IF_ERROR(Codec::CreateDecompressor(
      &mem_pool, false, THdfsCompression::DEFAULT, &decompressor));
  const auto close_decompressor =
      MakeScopeExitTrigger([&decompressor]() { decompressor->Close(); });

  int64_t result_len;
  uint8_t* decompressed_buffer;
  RETURN_IF_ERROR(decompressor->ProcessBlock(
      false, decoded_len, decoded_buffer.data(), &result_len, &decompressed_buffer));

  uint32_t deserialized_len = static_cast<uint32_t>(result_len);
  RETURN_IF_ERROR(
      DeserializeThriftMsg(decompressed_buffer, &deserialized_len, true, out));
  return Status::OK();
}

void RuntimeProfile::SetTExecSummary(const TExecSummary& summary) {
  lock_guard<SpinLock> l(t_exec_summary_lock_);
  t_exec_summary_ = summary;
}

void RuntimeProfile::ToThrift(TRuntimeProfileTree* tree) const {
  tree->nodes.clear();
  ToThrift(&tree->nodes);
  ExecSummaryToThrift(tree);
}

void RuntimeProfile::ToThrift(vector<TRuntimeProfileNode>* nodes) const {
  int index = nodes->size();
  nodes->push_back(TRuntimeProfileNode());
  TRuntimeProfileNode& node = (*nodes)[index];
  node.name = name_;
  // Set the required metadata field to the plan node ID for compatibility with any tools
  // that rely on the plan node id being set there.
  node.metadata = metadata_.__isset.plan_node_id ? metadata_.plan_node_id : -1;
  // Thrift requires exactly one field of a union to be set so we only mark node_metadata
  // as set if that is the case.
  if (metadata_.__isset.plan_node_id || metadata_.__isset.data_sink_id) {
    node.__set_node_metadata(metadata_);
  }
  node.indent = true;

  CounterMap counter_map;
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    counter_map = counter_map_;
    node.child_counters_map = child_counter_map_;
  }
  for (map<string, Counter*>::const_iterator iter = counter_map.begin();
       iter != counter_map.end(); ++iter) {
    TCounter counter;
    counter.name = iter->first;
    counter.value = iter->second->value();
    counter.unit = iter->second->unit();
    node.counters.push_back(counter);
  }

  {
    lock_guard<SpinLock> l(info_strings_lock_);
    node.info_strings = info_strings_;
    node.info_strings_display_order = info_strings_display_order_;
  }

  {
    vector<EventSequence::Event> events;
    lock_guard<SpinLock> l(event_sequence_lock_);
    if (event_sequence_map_.size() != 0) {
      node.__set_event_sequences(vector<TEventSequence>());
      node.event_sequences.resize(event_sequence_map_.size());
      int idx = 0;
      for (const EventSequenceMap::value_type& val: event_sequence_map_) {
        TEventSequence* seq = &node.event_sequences[idx++];
        seq->name = val.first;
        val.second->GetEvents(&events);
        for (const EventSequence::Event& ev: events) {
          seq->labels.push_back(ev.first);
          seq->timestamps.push_back(ev.second);
        }
      }
    }
  }

  {
    lock_guard<SpinLock> l(counter_map_lock_);
    if (time_series_counter_map_.size() != 0) {
      node.__set_time_series_counters(
          vector<TTimeSeriesCounter>(time_series_counter_map_.size()));
      int idx = 0;
      for (const TimeSeriesCounterMap::value_type& val: time_series_counter_map_) {
        val.second->ToThrift(&node.time_series_counters[idx++]);
      }
    }
  }

  {
    lock_guard<SpinLock> l(summary_stats_map_lock_);
    if (summary_stats_map_.size() != 0) {
      node.__set_summary_stats_counters(
          vector<TSummaryStatsCounter>(summary_stats_map_.size()));
      int idx = 0;
      for (const SummaryStatsCounterMap::value_type& val: summary_stats_map_) {
        val.second->ToThrift(&node.summary_stats_counters[idx++], val.first);
      }
    }
  }

  ChildVector children;
  {
    lock_guard<SpinLock> l(children_lock_);
    children = children_;
    node.num_children = children_.size();
  }
  for (int i = 0; i < children.size(); ++i) {
    int child_idx = nodes->size();
    children[i].first->ToThrift(nodes);
    // fix up indentation flag
    (*nodes)[child_idx].indent = children[i].second;
  }
}

void RuntimeProfile::ExecSummaryToThrift(TRuntimeProfileTree* tree) const {
  GetExecSummary(&tree->exec_summary);
  tree->__isset.exec_summary = true;
}

void RuntimeProfile::GetExecSummary(TExecSummary* t_exec_summary) const {
  lock_guard<SpinLock> l(t_exec_summary_lock_);
  *t_exec_summary = t_exec_summary_;
}

void RuntimeProfile::SetPlanNodeId(int node_id) {
  DCHECK(!metadata_.__isset.data_sink_id) << "Don't set conflicting metadata";
  metadata_.__set_plan_node_id(node_id);
}

void RuntimeProfile::SetDataSinkId(int sink_id) {
  DCHECK(!metadata_.__isset.plan_node_id) << "Don't set conflicting metadata";
  metadata_.__set_data_sink_id(sink_id);
}

int64_t RuntimeProfile::UnitsPerSecond(
    const RuntimeProfile::Counter* total_counter, const RuntimeProfile::Counter* timer) {
  DCHECK(total_counter->unit() == TUnit::BYTES || total_counter->unit() == TUnit::UNIT);
  DCHECK(timer->unit() == TUnit::TIME_NS);

  if (timer->value() == 0) return 0;
  double secs = static_cast<double>(timer->value()) / 1000.0 / 1000.0 / 1000.0;
  return total_counter->value() / secs;
}

int64_t RuntimeProfile::CounterSum(const vector<Counter*>* counters) {
  int64_t value = 0;
  for (int i = 0; i < counters->size(); ++i) {
    value += (*counters)[i]->value();
  }
  return value;
}

RuntimeProfile::Counter* RuntimeProfile::AddRateCounter(
    const string& name, Counter* src_counter) {
  TUnit::type dst_unit;
  switch (src_counter->unit()) {
    case TUnit::BYTES:
      dst_unit = TUnit::BYTES_PER_SECOND;
      break;
    case TUnit::UNIT:
      dst_unit = TUnit::UNIT_PER_SECOND;
      break;
    default:
      DCHECK(false) << "Unsupported src counter unit: " << src_counter->unit();
      return NULL;
  }
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    bool created;
    Counter* dst_counter = AddCounterLocked(name, dst_unit, "", &created);
    if (!created) return dst_counter;
    rate_counters_.push_back(dst_counter);
    PeriodicCounterUpdater::RegisterPeriodicCounter(src_counter, NULL, dst_counter,
        PeriodicCounterUpdater::RATE_COUNTER);
    has_active_periodic_counters_ = true;
    return dst_counter;
  }
}

RuntimeProfile::Counter* RuntimeProfile::AddRateCounter(
    const string& name, SampleFunction fn, TUnit::type dst_unit) {
  lock_guard<SpinLock> l(counter_map_lock_);
  bool created;
  Counter* dst_counter = AddCounterLocked(name, dst_unit, "", &created);
  if (!created) return dst_counter;
  rate_counters_.push_back(dst_counter);
  PeriodicCounterUpdater::RegisterPeriodicCounter(NULL, fn, dst_counter,
      PeriodicCounterUpdater::RATE_COUNTER);
  has_active_periodic_counters_ = true;
  return dst_counter;
}

RuntimeProfile::Counter* RuntimeProfile::AddSamplingCounter(
    const string& name, Counter* src_counter) {
  DCHECK(src_counter->unit() == TUnit::UNIT);
  lock_guard<SpinLock> l(counter_map_lock_);
  bool created;
  Counter* dst_counter = AddCounterLocked(name, TUnit::DOUBLE_VALUE, "", &created);
  if (!created) return dst_counter;
  sampling_counters_.push_back(dst_counter);
  PeriodicCounterUpdater::RegisterPeriodicCounter(src_counter, NULL, dst_counter,
      PeriodicCounterUpdater::SAMPLING_COUNTER);
  has_active_periodic_counters_ = true;
  return dst_counter;
}

RuntimeProfile::Counter* RuntimeProfile::AddSamplingCounter(
    const string& name, SampleFunction sample_fn) {
  lock_guard<SpinLock> l(counter_map_lock_);
  bool created;
  Counter* dst_counter = AddCounterLocked(name, TUnit::DOUBLE_VALUE, "", &created);
  if (!created) return dst_counter;
  sampling_counters_.push_back(dst_counter);
  PeriodicCounterUpdater::RegisterPeriodicCounter(NULL, sample_fn, dst_counter,
      PeriodicCounterUpdater::SAMPLING_COUNTER);
  has_active_periodic_counters_ = true;
  return dst_counter;
}

vector<RuntimeProfile::Counter*>* RuntimeProfile::AddBucketingCounters(
    Counter* src_counter, int num_buckets) {
  lock_guard<SpinLock> l(counter_map_lock_);
  vector<RuntimeProfile::Counter*>* buckets = pool_->Add(new vector<Counter*>);
  for (int i = 0; i < num_buckets; ++i) {
      buckets->push_back(
          pool_->Add(new RuntimeProfile::Counter(TUnit::DOUBLE_VALUE, 0)));
  }
  bucketing_counters_.insert(buckets);
  has_active_periodic_counters_ = true;
  PeriodicCounterUpdater::RegisterBucketingCounters(src_counter, buckets);
  return buckets;
}

RuntimeProfile::EventSequence* RuntimeProfile::AddEventSequence(const string& name) {
  lock_guard<SpinLock> l(event_sequence_lock_);
  EventSequenceMap::iterator timer_it = event_sequence_map_.find(name);
  if (timer_it != event_sequence_map_.end()) return timer_it->second;

  EventSequence* timer = pool_->Add(new EventSequence());
  event_sequence_map_[name] = timer;
  return timer;
}

RuntimeProfile::EventSequence* RuntimeProfile::AddEventSequence(const string& name,
    const TEventSequence& from) {
  lock_guard<SpinLock> l(event_sequence_lock_);
  EventSequenceMap::iterator timer_it = event_sequence_map_.find(name);
  if (timer_it != event_sequence_map_.end()) return timer_it->second;

  EventSequence* timer = pool_->Add(new EventSequence(from.timestamps, from.labels));
  event_sequence_map_[name] = timer;
  return timer;
}

void RuntimeProfile::PrintChildCounters(const string& prefix,
    const string& counter_name, const CounterMap& counter_map,
    const ChildCounterMap& child_counter_map, ostream* s) {
  ostream& stream = *s;
  ChildCounterMap::const_iterator itr = child_counter_map.find(counter_name);
  if (itr != child_counter_map.end()) {
    const set<string>& child_counters = itr->second;
    for (const string& child_counter: child_counters) {
      CounterMap::const_iterator iter = counter_map.find(child_counter);
      if (iter == counter_map.end()) continue;
      stream << prefix << "   - " << iter->first << ": "
             << PrettyPrinter::Print(iter->second->value(), iter->second->unit(), true)
             << endl;
      RuntimeProfile::PrintChildCounters(prefix + "  ", child_counter, counter_map,
          child_counter_map, s);
    }
  }
}

RuntimeProfile::SummaryStatsCounter* RuntimeProfile::AddSummaryStatsCounter(
    const string& name, TUnit::type unit, const std::string& parent_counter_name) {
  DCHECK_EQ(is_averaged_profile_, false);
  lock_guard<SpinLock> l(summary_stats_map_lock_);
  if (summary_stats_map_.find(name) != summary_stats_map_.end()) {
    return summary_stats_map_[name];
  }
  SummaryStatsCounter* counter = pool_->Add(new SummaryStatsCounter(unit));
  summary_stats_map_[name] = counter;
  return counter;
}

RuntimeProfile::TimeSeriesCounter* RuntimeProfile::AddSamplingTimeSeriesCounter(
    const string& name, TUnit::type unit, SampleFunction fn) {
  DCHECK(fn != nullptr);
  lock_guard<SpinLock> l(counter_map_lock_);
  TimeSeriesCounterMap::iterator it = time_series_counter_map_.find(name);
  if (it != time_series_counter_map_.end()) return it->second;
  TimeSeriesCounter* counter = pool_->Add(new SamplingTimeSeriesCounter(name, unit, fn));
  time_series_counter_map_[name] = counter;
  PeriodicCounterUpdater::RegisterTimeSeriesCounter(counter);
  has_active_periodic_counters_ = true;
  return counter;
}

RuntimeProfile::TimeSeriesCounter* RuntimeProfile::AddSamplingTimeSeriesCounter(
    const string& name, Counter* src_counter) {
  DCHECK(src_counter != NULL);
  return AddSamplingTimeSeriesCounter(name, src_counter->unit(),
      bind(&Counter::value, src_counter));
}

void RuntimeProfile::TimeSeriesCounter::AddSample(int ms_elapsed) {
  lock_guard<SpinLock> l(lock_);
  int64_t sample = sample_fn_();
  AddSampleLocked(sample, ms_elapsed);
}

const int64_t* RuntimeProfile::TimeSeriesCounter::GetSamplesLockedForSend(
    int* num_samples, int* period) {
  return GetSamplesLocked(num_samples, period);
}

void RuntimeProfile::TimeSeriesCounter::SetSamples(
      int period, const std::vector<int64_t>& samples, int64_t start_idx) {
  DCHECK(false);
}

void RuntimeProfile::SamplingTimeSeriesCounter::AddSampleLocked(
    int64_t sample, int ms_elapsed){
  samples_.AddSample(sample, ms_elapsed);
}

const int64_t* RuntimeProfile::SamplingTimeSeriesCounter::GetSamplesLocked(
    int* num_samples, int* period) const {
  return samples_.GetSamples(num_samples, period);
}

RuntimeProfile::ChunkedTimeSeriesCounter::ChunkedTimeSeriesCounter(
    const string& name, TUnit::type unit, SampleFunction fn)
  : TimeSeriesCounter(name, unit, fn)
  , period_(FLAGS_periodic_counter_update_period_ms)
  , max_size_(10 * FLAGS_status_report_interval_ms / period_) {}

void RuntimeProfile::ChunkedTimeSeriesCounter::Clear() {
  lock_guard<SpinLock> l(lock_);
  previous_sample_count_ += last_get_count_;
  values_.erase(values_.begin(), values_.begin() + last_get_count_);
  last_get_count_ = 0;
}

void RuntimeProfile::ChunkedTimeSeriesCounter::AddSampleLocked(
    int64_t sample, int ms_elapsed) {
  // We chose inefficiently erasing elements from a vector over using a std::deque because
  // this should only happen very infrequently and we rely on contiguous storage in
  // GetSamplesLocked*().
  if (max_size_ > 0 && values_.size() == max_size_) {
    KLOG_EVERY_N_SECS(WARNING, 60) << "ChunkedTimeSeriesCounter reached maximum size";
    values_.erase(values_.begin(), values_.begin() + 1);
  }
  DCHECK_LT(values_.size(), max_size_);
  values_.push_back(sample);
}

const int64_t* RuntimeProfile::ChunkedTimeSeriesCounter::GetSamplesLocked(
    int* num_samples, int* period) const {
  DCHECK(num_samples != nullptr);
  DCHECK(period != nullptr);
  *num_samples = values_.size();
  *period = period_;
  return values_.data();
}

const int64_t* RuntimeProfile::ChunkedTimeSeriesCounter::GetSamplesLockedForSend(
    int* num_samples, int* period) {
  last_get_count_ = values_.size();
  return GetSamplesLocked(num_samples, period);
}

void RuntimeProfile::ChunkedTimeSeriesCounter::SetSamples(
    int period, const std::vector<int64_t>& samples, int64_t start_idx) {
  lock_guard<SpinLock> l(lock_);
  if (start_idx == 0) {
    // This could be coming from a SamplingTimeSeriesCounter or another
    // ChunkedTimeSeriesCounter.
    period_ = period;
    values_ = samples;
    return;
  }
  // Only ChunkedTimeSeriesCounter will set start_idx > 0.
  DCHECK_GT(start_idx, 0);
  DCHECK_EQ(period_, period);
  if (values_.size() < start_idx) {
    // Fill up with 0.
    values_.resize(start_idx);
  }
  DCHECK_GE(values_.size(), start_idx);
  // Skip values we already received.
  auto start_it = samples.begin() + values_.size() - start_idx;
  values_.insert(values_.end(), start_it, samples.end());
}

RuntimeProfile::TimeSeriesCounter* RuntimeProfile::AddChunkedTimeSeriesCounter(
    const string& name, TUnit::type unit, SampleFunction fn) {
  DCHECK(fn != nullptr);
  lock_guard<SpinLock> l(counter_map_lock_);
  TimeSeriesCounterMap::iterator it = time_series_counter_map_.find(name);
  if (it != time_series_counter_map_.end()) return it->second;
  TimeSeriesCounter* counter = pool_->Add(new ChunkedTimeSeriesCounter(name, unit, fn));
  time_series_counter_map_[name] = counter;
  PeriodicCounterUpdater::RegisterTimeSeriesCounter(counter);
  has_active_periodic_counters_ = true;
  return counter;
}

void RuntimeProfile::ClearChunkedTimeSeriesCounters() {
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    for (auto& it : time_series_counter_map_) it.second->Clear();
  }
  {
    lock_guard<SpinLock> l(children_lock_);
    for (int i = 0; i < children_.size(); ++i) {
      children_[i].first->ClearChunkedTimeSeriesCounters();
    }
  }
}

void RuntimeProfile::TimeSeriesCounter::ToThrift(TTimeSeriesCounter* counter) {
  lock_guard<SpinLock> l(lock_);
  int num, period;
  const int64_t* samples = GetSamplesLockedForSend(&num, &period);
  counter->values.resize(num);
  Ubsan::MemCpy(counter->values.data(), samples, num * sizeof(int64_t));

  counter->name = name_;
  counter->unit = unit_;
  counter->period_ms = period;
  counter->__set_start_index(previous_sample_count_);
}

void RuntimeProfile::EventSequence::ToThrift(TEventSequence* seq) {
  lock_guard<SpinLock> l(lock_);
  /// It's possible that concurrent events can be logged out of sequence so we sort the
  /// events before serializing them.
  SortEvents();
  for (const EventSequence::Event& ev: events_) {
    seq->labels.push_back(ev.first);
    seq->timestamps.push_back(ev.second);
  }
}

void RuntimeProfile::SummaryStatsCounter::ToThrift(TSummaryStatsCounter* counter,
    const std::string& name) {
  lock_guard<SpinLock> l(lock_);
  counter->name = name;
  counter->unit = unit_;
  counter->sum = sum_;
  counter->total_num_values = total_num_values_;
  counter->min_value = min_;
  counter->max_value = max_;
}

void RuntimeProfile::SummaryStatsCounter::UpdateCounter(int64_t new_value) {
  lock_guard<SpinLock> l(lock_);

  ++total_num_values_;
  sum_ += new_value;
  value_.Store(sum_ / total_num_values_);

  if (new_value < min_) min_ = new_value;
  if (new_value > max_) max_ = new_value;
}

void RuntimeProfile::SummaryStatsCounter::SetStats(const TSummaryStatsCounter& counter) {
  // We drop this input if it looks malformed.
  if (counter.total_num_values < 0) return;
  lock_guard<SpinLock> l(lock_);
  unit_ = counter.unit;
  sum_ = counter.sum;
  total_num_values_ = counter.total_num_values;
  min_ = counter.min_value;
  max_ = counter.max_value;

  value_.Store(total_num_values_ == 0 ? 0 : sum_ / total_num_values_);
}

int64_t RuntimeProfile::SummaryStatsCounter::MinValue() {
  lock_guard<SpinLock> l(lock_);
  return min_;
}

int64_t RuntimeProfile::SummaryStatsCounter::MaxValue() {
  lock_guard<SpinLock> l(lock_);
  return max_;
}

int32_t RuntimeProfile::SummaryStatsCounter::TotalNumValues() {
  lock_guard<SpinLock> l(lock_);
  return total_num_values_;
}

}
