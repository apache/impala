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
#include <mutex>
#include <numeric>
#include <type_traits>
#include <utility>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/bind.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include "common/object-pool.h"
#include "gutil/strings/strip.h"
#include "kudu/util/logging.h"
#include "rpc/thrift-util.h"
#include "runtime/mem-pool.h"
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
#include "gutil/strings/strcat.h"

#include "common/names.h"

DECLARE_int32(status_report_interval_ms);
DECLARE_int32(periodic_counter_update_period_ms);
DECLARE_int32(periodic_system_counter_update_period_ms);

// This must be set on the coordinator to enable the alternative profile representation.
// It should not be set on executors - the setting is sent to the executors by
// TQueryCtx.gen_aggregated_profile.
DEFINE_bool_hidden(gen_experimental_profile, false,
    "(Experimental) when set on coordinator, generate a new aggregated runtime profile "
    "layout. Format is subject to change.");

using boost::algorithm::to_lower_copy;
using namespace rapidjson;

namespace impala {

// Thread counters name
static const string THREAD_TOTAL_TIME = "TotalWallClockTime";
static const string THREAD_USER_TIME = "UserTime";
static const string THREAD_SYS_TIME = "SysTime";
static const string THREAD_VOLUNTARY_CONTEXT_SWITCHES = "VoluntaryContextSwitches";
static const string THREAD_INVOLUNTARY_CONTEXT_SWITCHES = "InvoluntaryContextSwitches";

// The root counter name for all top level counters.
static const string ROOT_COUNTER = "";

const string RuntimeProfileBase::TOTAL_TIME_COUNTER_NAME = "TotalTime";
const string RuntimeProfileBase::LOCAL_TIME_COUNTER_NAME = "LocalTime";
const string RuntimeProfileBase::INACTIVE_TIME_COUNTER_NAME = "InactiveTotalTime";

const string RuntimeProfileBase::SKEW_SUMMARY = "skew(s) found at";
const string RuntimeProfileBase::SKEW_DETAILS = "Skew details";

const string RuntimeProfile::FRAGMENT_INSTANCE_LIFECYCLE_TIMINGS =
    "Fragment Instance Lifecycle Timings";
const string RuntimeProfile::BUFFER_POOL = "Buffer pool";
const string RuntimeProfile::DEQUEUE = "Dequeue";
const string RuntimeProfile::ENQUEUE = "Enqueue";
const string RuntimeProfile::HASH_TABLE = "Hash Table";
const string RuntimeProfile::PREFIX_FILTER = "Filter ";
const string RuntimeProfile::PREFIX_GROUPING_AGGREGATOR = "GroupingAggregator ";

constexpr ProfileEntryPrototype::Significance ProfileEntryPrototype::ALLSIGNIFICANCE[];

/// Helper to interpret the bit pattern of 'val' as T, which can either be an int64_t or
/// a double.
template <typename T>
static T BitcastFromInt64(int64_t val) {
  static_assert(std::is_same<T, int64_t>::value || std::is_same<T, double>::value,
      "Only double and int64_t are supported");
  T res;
  memcpy(&res, &val, sizeof(int64_t));
  return res;
}

/// Helper to store the bit pattern of 'val' as an int64_t, T can either be an int64_t or
/// a double.
template <typename T>
static int64_t BitcastToInt64(T val) {
  static_assert(std::is_same<T, int64_t>::value || std::is_same<T, double>::value,
      "Only double and int64_t are supported");
  int64_t res;
  memcpy(&res, &val, sizeof(int64_t));
  return res;
}

static int32_t GetProfileVersion(const TRuntimeProfileTree& tree) {
  // Assume version 1 if not set, because profile_version was only added in v2.
  return tree.__isset.profile_version ? tree.profile_version : 1;
}

static bool UntimedProfileNode(const string& name) {
  return name.compare(RuntimeProfile::FRAGMENT_INSTANCE_LIFECYCLE_TIMINGS) == 0
      || name.compare(RuntimeProfile::BUFFER_POOL) == 0
      || name.compare(RuntimeProfile::DEQUEUE) == 0
      || name.compare(RuntimeProfile::ENQUEUE) == 0
      || name.compare(RuntimeProfile::HASH_TABLE) == 0
      || name.rfind(RuntimeProfile::PREFIX_FILTER, 0) == 0
      || name.rfind(RuntimeProfile::PREFIX_GROUPING_AGGREGATOR, 0) == 0;
}

static RuntimeProfile::Verbosity DefaultVerbosity() {
  return FLAGS_gen_experimental_profile ? RuntimeProfile::Verbosity::DEFAULT :
                                          RuntimeProfile::Verbosity::LEGACY;
}

void ProfileEntryPrototypeRegistry::AddPrototype(const ProfileEntryPrototype* prototype) {
  lock_guard<SpinLock> l(lock_);
  DCHECK(prototypes_.find(prototype->name()) == prototypes_.end()) <<
      "Found duplicate prototype name: " << prototype->name();
  prototypes_.emplace(prototype->name(), prototype);
}

void ProfileEntryPrototypeRegistry::GetPrototypes(
    vector<const ProfileEntryPrototype*>* out) {
  lock_guard<SpinLock> l(lock_);
  out->reserve(prototypes_.size());
  for (auto p : prototypes_) out->push_back(p.second);
}

ProfileEntryPrototype::ProfileEntryPrototype(const char* name, Significance significance,
    const char* desc, TUnit::type unit) :
    name_(name), significance_(significance), desc_(desc),
    unit_(unit) {
  ProfileEntryPrototypeRegistry::get()->AddPrototype(this);
}

const char* ProfileEntryPrototype::SignificanceString(
    ProfileEntryPrototype::Significance significance) {
  switch (significance) {
    case Significance::STABLE_HIGH:
      return "STABLE & HIGH";
    case Significance::STABLE_LOW:
      return "STABLE & LOW";
    case Significance::UNSTABLE:
      return "UNSTABLE";
    case Significance::DEBUG:
      return "DEBUG";
    default:
      DCHECK(false);
      return "";
  }
}

const char* ProfileEntryPrototype::SignificanceDescription(
    ProfileEntryPrototype::Significance significance) {
  switch (significance) {
    case Significance::STABLE_HIGH:
      return "High level and stable counters - always useful for measuring query "
             "performance and status. Counters that everyone is interested. should "
             "rarely change and if it does we will make some effort to notify users.";
    case Significance::STABLE_LOW:
      return "Low level and stable counters - interesting counters to monitor and "
             "analyze by machine. It will probably be interesting under some "
             "circumstances for users.";
    case Significance::UNSTABLE:
      return "Unstable but useful - useful to understand query performance, but subject"
             " to change, particularly if the implementation changes. E.g. "
             "RowBatchQueuePutWaitTime, MaterializeTupleTimer";
    case Significance::DEBUG:
      return "Debugging counters - generally not useful to users of Impala, the main"
             " use case is low-level debugging. Can be hidden to reduce noise for "
             "most consumers of profiles.";
    default:
      DCHECK(false);
      return "";
  }
}

RuntimeProfileBase::RuntimeProfileBase(ObjectPool* pool, const string& name,
    Counter* total_time_counter, Counter* inactive_timer, bool add_default_counters)
  : pool_(pool),
    name_(name),
    total_time_counter_(total_time_counter),
    inactive_timer_(inactive_timer) {
  DCHECK(total_time_counter != nullptr);
  DCHECK(inactive_timer != nullptr);
  set<string>& root_counters = child_counter_map_[ROOT_COUNTER];
  if (add_default_counters) {
    counter_map_[TOTAL_TIME_COUNTER_NAME] = total_time_counter;
    root_counters.emplace(TOTAL_TIME_COUNTER_NAME);
    counter_map_[INACTIVE_TIME_COUNTER_NAME] = inactive_timer;
    root_counters.emplace(INACTIVE_TIME_COUNTER_NAME);
  }
}

RuntimeProfileBase::~RuntimeProfileBase() {}

template <typename CreateFn>
RuntimeProfileBase* RuntimeProfileBase::AddOrCreateChild(const string& name,
    ChildVector::iterator* insert_pos, CreateFn create_fn, bool indent) {
  RuntimeProfileBase* child = nullptr;
  ChildMap::iterator it = child_map_.find(name);
  if (it != child_map_.end()) {
    child = it->second;
    DCHECK(child != nullptr);
    // Search forward until the insert position is either at the end of the vector
    // or after this child. This preserves the order if the relative order of
    // children in all updates is consistent. If the order is inconsistent we
    // may not find the child because it is somewhere before '*insert_pos' in the
    // vector.
    bool found_child = false;
    while (*insert_pos != children_.end() && !found_child) {
      found_child = (*insert_pos)->first == child;
      ++*insert_pos;
    }
  } else {
    child = create_fn();
    child_map_[child->name_] = child;
    *insert_pos = children_.insert(*insert_pos, make_pair(child, indent));
    ++*insert_pos;
  }
  return child;
}

void RuntimeProfileBase::UpdateChildCountersLocked(const unique_lock<SpinLock>& lock,
      const ChildCounterMap& src) {
  DCHECK(lock.mutex() == &counter_map_lock_ && lock.owns_lock());
  // Merge all new parent->child edges into the map. This assumes that all 'src' maps
  // provided are consistent, i.e. one child does not have multiple parents.
  ChildCounterMap::const_iterator child_counter_src_itr;
  for (const auto& entry : src) {
    set<string>* child_counters =
        FindOrInsert(&child_counter_map_, entry.first, set<string>());
    child_counters->insert(entry.second.begin(), entry.second.end());
  }
}

RuntimeProfile* RuntimeProfile::Create(
    ObjectPool* pool, const string& name, bool add_default_counters) {
  return pool->Add(new RuntimeProfile(pool, name, add_default_counters));
}

RuntimeProfile::RuntimeProfile(
    ObjectPool* pool, const string& name, bool add_default_counters)
  : RuntimeProfileBase(pool, name, &builtin_counter_total_time_, &builtin_inactive_timer_,
      add_default_counters) {}

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
  RuntimeProfileBase* profile =
      RuntimeProfileBase::CreateFromThriftHelper(pool, profiles.nodes, &idx);
  // The root must always be a RuntimeProfile, not an AggregatedProfile.
  RuntimeProfile* root = dynamic_cast<RuntimeProfile*>(profile);
  DCHECK(root != nullptr);
  root->SetTExecSummary(profiles.exec_summary);
  // Some values like local time are not serialized to Thrift and need to be
  // recomputed.
  root->ComputeTimeInProfile();
  return root;
}

RuntimeProfileBase* RuntimeProfileBase::CreateFromThriftHelper(
    ObjectPool* pool, const vector<TRuntimeProfileNode>& nodes, int* idx) {
  DCHECK_LT(*idx, nodes.size());

  const TRuntimeProfileNode& node = nodes[*idx];
  RuntimeProfileBase* profile;
  if (node.__isset.aggregated) {
    DCHECK(node.aggregated.__isset.num_instances);
    profile = AggregatedRuntimeProfile::Create(pool, node.name,
        node.aggregated.num_instances, node.aggregated.__isset.input_profiles);
  } else {
    profile = RuntimeProfile::Create(pool, node.name);
  }
  profile->metadata_ = node.node_metadata;
  profile->InitFromThrift(node, pool);
  profile->child_counter_map_ = node.child_counters_map;

  // TODO: IMPALA-9846: move to RuntimeProfile::InitFromThrift() once 'info_strings_' is
  // moved.
  profile->info_strings_ = node.info_strings;
  profile->info_strings_display_order_ = node.info_strings_display_order;

  ++*idx;
  {
    lock_guard<SpinLock> l(profile->children_lock_);
    for (int i = 0; i < node.num_children; ++i) {
      bool indent = nodes[*idx].indent;
      profile->AddChildLocked(
          RuntimeProfileBase::CreateFromThriftHelper(pool, nodes, idx),
          indent, profile->children_.end());
    }
  }
  return profile;
}

void RuntimeProfile::InitFromThrift(const TRuntimeProfileNode& node, ObjectPool* pool) {
  // Only read 'counters' for non-aggregated profiles. Aggregated profiles will populate
  // 'counter_map_' from the aggregated counters in thrift.
  for (int i = 0; i < node.counters.size(); ++i) {
    const TCounter& counter = node.counters[i];
    counter_map_[counter.name] = pool->Add(new Counter(counter.unit, counter.value));
  }

  if (node.__isset.event_sequences) {
    for (const TEventSequence& sequence: node.event_sequences) {
      event_sequence_map_[sequence.name] =
          pool->Add(new EventSequence(sequence.timestamps, sequence.labels));
    }
  }

  if (node.__isset.time_series_counters) {
    for (const TTimeSeriesCounter& val: node.time_series_counters) {
      // Capture all incoming time series counters with the same type since re-sampling
      // will have happened on the sender side.
      time_series_counter_map_[val.name] = pool->Add(
          new ChunkedTimeSeriesCounter(val.name, val.unit, val.period_ms, val.values));
    }
  }

  if (node.__isset.summary_stats_counters) {
    for (const TSummaryStatsCounter& val: node.summary_stats_counters) {
      summary_stats_map_[val.name] = pool->Add(new SummaryStatsCounter(
          val.unit, val.total_num_values, val.min_value, val.max_value, val.sum));
    }
  }
}

void AggregatedRuntimeProfile::UpdateAggregatedFromInstance(
    RuntimeProfile* other, int idx, bool add_default_counters) {
  {
    lock_guard<SpinLock> l(input_profile_name_lock_);
    DCHECK(!input_profile_names_.empty())
        << "Update() can only be called on root of averaged profile tree";
    input_profile_names_[idx] = other->name();
  }

  UpdateAggregatedFromInstanceRecursive(other, idx, add_default_counters);

  // Recursively compute times on the whole tree.
  ComputeTimeInProfile();
}

void AggregatedRuntimeProfile::UpdateAggregatedFromInstanceRecursive(
    RuntimeProfile* other, int idx, bool add_default_counters) {
  DCHECK(other != NULL);
  DCHECK_GE(idx, 0);
  DCHECK_LT(idx, num_input_profiles_);

  // Merge all counters and other items in the profile at this level.
  UpdateCountersFromInstance(other, idx);
  UpdateInfoStringsFromInstance(other, idx);
  UpdateSummaryStatsFromInstance(other, idx);
  UpdateEventSequencesFromInstance(other, idx);

  {
    lock_guard<SpinLock> l(children_lock_);
    lock_guard<SpinLock> m(other->children_lock_);
    // Recursively merge children with matching names.
    // Track the current position in the vector so we preserve the order of children
    // if children are added after the first Update() call (IMPALA-6694).
    // E.g. if the first update sends [B, D] and the second update sends [A, B, C, D],
    // then this code makes sure that children_ is [A, B, C, D] afterwards.
    ChildVector::iterator insert_pos = children_.begin();
    for (int i = 0; i < other->children_.size(); ++i) {
      RuntimeProfile* other_child =
          dynamic_cast<RuntimeProfile*>(other->children_[i].first);
      DCHECK(other_child != nullptr)
          << other->children_[i].first->name() << " must be a RuntimeProfile";
      bool indent_other_child = other->children_[i].second;
      bool is_timed = add_default_counters && !UntimedProfileNode(other_child->name());
      AggregatedRuntimeProfile* child =
          dynamic_cast<AggregatedRuntimeProfile*>(AddOrCreateChild(
              other_child->name(), &insert_pos,
              [this, other_child, is_timed]() {
                AggregatedRuntimeProfile* child2 = Create(pool_, other_child->name(),
                    num_input_profiles_, /*is_root=*/false, is_timed);
                child2->metadata_ = other_child->metadata();
                return child2;
              },
              indent_other_child));
      DCHECK(child != nullptr);
      child->UpdateAggregatedFromInstanceRecursive(other_child, idx, is_timed);
    }
  }
}

void AggregatedRuntimeProfile::UpdateCountersFromInstance(
    RuntimeProfile* other, int idx) {
  CounterMap::iterator dst_iter;
  CounterMap::const_iterator src_iter;
  unique_lock<SpinLock> l(counter_map_lock_);
  unique_lock<SpinLock> m(other->counter_map_lock_);
  for (src_iter = other->counter_map_.begin();
       src_iter != other->counter_map_.end(); ++src_iter) {
    dst_iter = counter_map_.find(src_iter->first);
    AveragedCounter* avg_counter;

    // Get the counter with the same name in dst_iter (this->counter_map_)
    // Create one if it doesn't exist.
    if (dst_iter == counter_map_.end()) {
      avg_counter = pool_->Add(
          new AveragedCounter(src_iter->second->unit(), num_input_profiles_));
      counter_map_[src_iter->first] = avg_counter;
    } else {
      DCHECK(dst_iter->second->unit() == src_iter->second->unit());
      avg_counter = static_cast<AveragedCounter*>(dst_iter->second);
    }
    avg_counter->UpdateCounter(src_iter->second, idx);
  }

  for (const auto& src_entry : other->time_series_counter_map_) {
    RuntimeProfile::TimeSeriesCounter* src = src_entry.second;
    TAggTimeSeriesCounter& dst = time_series_counter_map_[src_entry.first];
    if (dst.values.empty()) {
      dst.name = src_entry.first;
      dst.unit = src->unit();
      dst.period_ms.resize(num_input_profiles_);
      dst.values.resize(num_input_profiles_);
      dst.start_index.resize(num_input_profiles_);
    } else {
      DCHECK_EQ(dst.unit, src->unit());
    }
    // Use a temporary thrift counter so we can reuse the thrift conversion code.
    TTimeSeriesCounter tcounter;
    src->ToThrift(&tcounter);
    dst.period_ms[idx] = tcounter.period_ms;
    dst.values[idx] = tcounter.values;
    dst.start_index[idx] = tcounter.start_index;
  }

  UpdateChildCountersLocked(l, other->child_counter_map_);
}

void AggregatedRuntimeProfile::UpdateInfoStringsFromInstance(
    RuntimeProfile* other, int idx) {
  lock_guard<SpinLock> l(agg_info_strings_lock_);
  lock_guard<SpinLock> m(other->info_strings_lock_);
  for (const auto& entry : other->info_strings_) {
    vector<string>& values = agg_info_strings_[entry.first];
    if (values.empty()) values.resize(num_input_profiles_);
    if (values[idx] != entry.second) values[idx] = entry.second;
  }
}

void AggregatedRuntimeProfile::UpdateSummaryStatsFromInstance(
    RuntimeProfile* other, int idx) {
  lock_guard<SpinLock> l(summary_stats_map_lock_);
  lock_guard<SpinLock> m(other->summary_stats_map_lock_);
  for (const RuntimeProfile::SummaryStatsCounterMap::value_type& src_entry :
      other->summary_stats_map_) {
    DCHECK_GT(num_input_profiles_, 0);
    AggSummaryStatsCounterMap::mapped_type& agg_entry =
        summary_stats_map_[src_entry.first];
    vector<SummaryStatsCounter*>& agg_instance_counters = agg_entry.second;
    if (agg_instance_counters.empty()) {
      agg_instance_counters.resize(num_input_profiles_);
      agg_entry.first = src_entry.second->unit();
    } else {
      DCHECK_EQ(agg_entry.first, src_entry.second->unit()) << "Unit must be consistent";
    }

    // Get the counter with the same name.  Create one if it doesn't exist.
    if (agg_instance_counters[idx] == nullptr) {
      agg_instance_counters[idx] =
          pool_->Add(new SummaryStatsCounter(src_entry.second->unit()));
    }
    // Overwrite the previous value with the new value.
    agg_instance_counters[idx]->SetStats(*src_entry.second);
  }
}

void AggregatedRuntimeProfile::UpdateEventSequencesFromInstance(
    RuntimeProfile* other, int idx) {
  lock_guard<SpinLock> l(event_sequence_lock_);
  lock_guard<SpinLock> m(other->event_sequence_lock_);
  for (const auto& src_entry : other->event_sequence_map_) {
    DCHECK_GT(num_input_profiles_, 0);
    AggEventSequence& seq = event_sequence_map_[src_entry.first];
    if (seq.label_idxs.empty()) {
      seq.label_idxs.resize(num_input_profiles_);
      seq.timestamps.resize(num_input_profiles_);
    } else {
      DCHECK_EQ(num_input_profiles_, seq.label_idxs.size());
      DCHECK_EQ(num_input_profiles_, seq.timestamps.size());
    }

    std::vector<int32_t>& label_idxs = seq.label_idxs[idx];
    std::vector<int64_t>& timestamps = seq.timestamps[idx];
    label_idxs.clear();
    timestamps.clear();
    RuntimeProfile::EventSequence::EventList events;
    src_entry.second->GetEvents(&events);
    for (const RuntimeProfile::EventSequence::Event& event : events) {
      auto it = seq.labels.find(event.first);
      int32_t label_idx;
      if (it == seq.labels.end()) {
        label_idx = seq.labels.size();
        seq.labels.emplace(event.first, label_idx);
      } else {
        label_idx = it->second;
      }
      label_idxs.push_back(label_idx);
      timestamps.push_back(event.second);
    }
  }
}

// TODO: do we need any special handling of the computed timers? Like local/total time.
void AggregatedRuntimeProfile::UpdateAggregatedFromInstances(
    const TRuntimeProfileTree& src, int start_idx, bool add_default_counters) {
  DCHECK(FLAGS_gen_experimental_profile)
    << "Aggregated profiles should only be used on the coordinator when enabled by flag";
  if (UNLIKELY(src.nodes.size()) == 0) return;
  const TRuntimeProfileNode& root = src.nodes[0];
  DCHECK(root.__isset.aggregated);
  const TAggregatedRuntimeProfileNode& agg_root = root.aggregated;
  {
    lock_guard<SpinLock> l(input_profile_name_lock_);
    DCHECK(!input_profile_names_.empty())
        << "Update() can only be called on root of averaged profile tree";
    DCHECK_EQ(agg_root.input_profiles.size(), agg_root.num_instances);
    for (int i = 0; i < agg_root.num_instances; ++i) {
      if (LIKELY(!agg_root.input_profiles[i].empty())) {
        input_profile_names_[start_idx + i] = agg_root.input_profiles[i];
      }
    }
  }

  int node_idx = 0;
  UpdateAggregatedFromInstancesRecursive(src, &node_idx, start_idx, add_default_counters);

  // Recursively compute times on the whole tree.
  ComputeTimeInProfile();
}

void AggregatedRuntimeProfile::UpdateAggregatedFromInstancesRecursive(
    const TRuntimeProfileTree& src, int* node_idx, int start_idx,
    bool add_default_counters) {
  DCHECK_LT(*node_idx, src.nodes.size());
  const TRuntimeProfileNode& node = src.nodes[*node_idx];
  DCHECK(node.__isset.aggregated);

  UpdateFromInstances(node, start_idx, pool_);

  ++*node_idx;
  {
    lock_guard<SpinLock> l(children_lock_);
    ChildVector::iterator insert_pos = children_.begin();
    // Update children with matching names; create new ones if they don't match.
    for (int i = 0; i < node.num_children; ++i) {
      const TRuntimeProfileNode& tchild = src.nodes[*node_idx];
      bool is_timed = add_default_counters && !UntimedProfileNode(tchild.name);
      AggregatedRuntimeProfile* child =
          dynamic_cast<AggregatedRuntimeProfile*>(AddOrCreateChild(
              tchild.name, &insert_pos,
              [this, tchild, is_timed]() {
                AggregatedRuntimeProfile* child2 = Create(
                    pool_, tchild.name, num_input_profiles_, /*is_root=*/false, is_timed);
                child2->metadata_ = tchild.node_metadata;
                return child2;
              },
              tchild.indent));
      DCHECK(child != nullptr);
      child->UpdateAggregatedFromInstancesRecursive(src, node_idx, start_idx, is_timed);
    }
  }
}

void AggregatedRuntimeProfile::UpdateFromInstances(
    const TRuntimeProfileNode& node, int start_idx, ObjectPool* pool) {
  UpdateCountersFromInstances(node, start_idx, pool);
  UpdateInfoStringsFromInstances(node, start_idx, pool);
  UpdateSummaryStatsFromInstances(node, start_idx, pool);
  UpdateEventSequencesFromInstances(node, start_idx, pool);
}

void AggregatedRuntimeProfile::UpdateCountersFromInstances(
    const TRuntimeProfileNode& node, int start_idx, ObjectPool* pool) {
  unique_lock<SpinLock> l(counter_map_lock_);
  DCHECK(node.__isset.aggregated);
  const TAggregatedRuntimeProfileNode& agg_node = node.aggregated;
  for (const TAggCounter& tcounter : agg_node.counters) {
    auto dst_iter = counter_map_.find(tcounter.name);
    // Get the counter with the same name in dst_iter (this->counter_map_)
    // Create one if it doesn't exist.
    AveragedCounter* avg_counter;
    if (dst_iter == counter_map_.end()) {
      avg_counter = pool->Add(new AveragedCounter(tcounter.unit, num_input_profiles_));
      counter_map_[tcounter.name] = avg_counter;
    } else {
      DCHECK(dst_iter->second->unit() == tcounter.unit);
      avg_counter = static_cast<AveragedCounter*>(dst_iter->second);
    }
    avg_counter->Update(start_idx, tcounter.has_value, tcounter.values);
  }

  for (const TAggTimeSeriesCounter& tcounter : agg_node.time_series_counters) {
    TAggTimeSeriesCounter& dst = time_series_counter_map_[tcounter.name];
    if (dst.values.empty()) {
      dst.name = tcounter.name;
      dst.unit = tcounter.unit;
      dst.period_ms.resize(num_input_profiles_);
      dst.values.resize(num_input_profiles_);
      dst.start_index.resize(num_input_profiles_);
    } else {
      DCHECK_EQ(dst.unit, tcounter.unit);
    }
    DCHECK_LE(start_idx + tcounter.values.size(), num_input_profiles_);
    DCHECK_EQ(tcounter.values.size(), tcounter.period_ms.size());
    DCHECK_EQ(tcounter.values.size(), tcounter.start_index.size()) << tcounter.name;
    for (int i = 0; i < tcounter.values.size(); ++i) {
      int idx = start_idx + i;
      if (UNLIKELY(tcounter.values[i].empty() && !dst.values[idx].empty())) continue;
      dst.period_ms[idx] = tcounter.period_ms[i];
      dst.values[idx] = tcounter.values[i];
      dst.start_index[idx] = tcounter.start_index[i];
    }
  }

  UpdateChildCountersLocked(l, node.child_counters_map);
}

void AggregatedRuntimeProfile::UpdateInfoStringsFromInstances(
    const TRuntimeProfileNode& node, int start_idx, ObjectPool* pool) {
  lock_guard<SpinLock> l(agg_info_strings_lock_);
  DCHECK(node.__isset.aggregated);
  const TAggregatedRuntimeProfileNode& agg_node = node.aggregated;
  for (const auto& entry : agg_node.info_strings) {
    vector<string>& values = agg_info_strings_[entry.first];
    if (values.empty()) values.resize(num_input_profiles_);
    for (const auto& distinct_val : entry.second) {
      const string& val = distinct_val.first;
      for (int offset : distinct_val.second) {
        if (values[start_idx + offset] != val) values[start_idx + offset] = val;
      }
    }
  }
}

void AggregatedRuntimeProfile::UpdateSummaryStatsFromInstances(
    const TRuntimeProfileNode& node, int start_idx, ObjectPool* pool) {
  lock_guard<SpinLock> l(summary_stats_map_lock_);
  DCHECK(node.__isset.aggregated);
  const TAggregatedRuntimeProfileNode& agg_node = node.aggregated;
  for (const TAggSummaryStatsCounter& tcounter : agg_node.summary_stats_counters) {
    DCHECK_GT(num_input_profiles_, 0);
    DCHECK_LE(start_idx + tcounter.has_value.size(), num_input_profiles_);
    auto& entry = summary_stats_map_[tcounter.name];
    vector<SummaryStatsCounter*>& instance_counters = entry.second;
    if (instance_counters.empty()) {
      instance_counters.resize(num_input_profiles_);
      entry.first = tcounter.unit;
    } else {
      DCHECK_EQ(entry.first, tcounter.unit) << "Unit must be consistent";
    }

    TSummaryStatsCounter tmp_counter;
    tmp_counter.name = tcounter.name;
    tmp_counter.unit = tcounter.unit;
    for (int i = 0; i < tcounter.has_value.size(); ++i) {
      if (!tcounter.has_value[i]) continue;
      int idx = start_idx + i;
      // Get the counter with the same name.  Create one if it doesn't exist.
      if (instance_counters[idx] == nullptr) {
        instance_counters[idx] = pool->Add(new SummaryStatsCounter(tcounter.unit));
      }
      // Overwrite the previous value with the new value.
      tmp_counter.sum = tcounter.sum[i];
      tmp_counter.total_num_values = tcounter.total_num_values[i];
      tmp_counter.min_value = tcounter.min_value[i];
      tmp_counter.max_value = tcounter.max_value[i];
      instance_counters[idx]->SetStats(tmp_counter);
    }
  }
}

void AggregatedRuntimeProfile::UpdateEventSequencesFromInstances(
    const TRuntimeProfileNode& node, int start_idx, ObjectPool* pool) {
  lock_guard<SpinLock> l(event_sequence_lock_);
  DCHECK(node.__isset.aggregated);
  const TAggregatedRuntimeProfileNode& agg_node = node.aggregated;
  for (const TAggEventSequence& tseq : agg_node.event_sequences) {
    DCHECK_GT(num_input_profiles_, 0);
    AggEventSequence& seq = event_sequence_map_[tseq.name];
    if (seq.label_idxs.empty()) {
      seq.label_idxs.resize(num_input_profiles_);
      seq.timestamps.resize(num_input_profiles_);
    } else {
      DCHECK_EQ(num_input_profiles_, seq.label_idxs.size());
      DCHECK_EQ(num_input_profiles_, seq.timestamps.size());
    }

    DCHECK_LE(start_idx + tseq.timestamps.size(), num_input_profiles_);
    DCHECK_EQ(tseq.timestamps.size(), tseq.label_idxs.size());
    for (int i = 0; i < tseq.timestamps.size(); ++i) {
      if (UNLIKELY(tseq.timestamps[i].empty())) continue;
      int idx = start_idx + i;
      std::vector<int32_t>& label_idxs = seq.label_idxs[idx];
      std::vector<int64_t>& timestamps = seq.timestamps[idx];
      label_idxs.clear();
      timestamps.clear();
      DCHECK_EQ(tseq.timestamps[i].size(), tseq.label_idxs[i].size());
      for (int j = 0; j < tseq.timestamps[i].size(); ++j) {
        // Decode label using dictionary and encode using this profile's dictionary.
        int32_t src_label_idx = tseq.label_idxs[i][j];
        DCHECK_GE(src_label_idx, 0);
        DCHECK_LT(src_label_idx, tseq.label_dict.size());
        const string& label = tseq.label_dict[src_label_idx];
        auto it = seq.labels.find(label);
        int32_t label_idx;
        if (it == seq.labels.end()) {
          label_idx = seq.labels.size();
          seq.labels.emplace(label, label_idx);
        } else {
          label_idx = it->second;
        }
        label_idxs.push_back(label_idx);
        timestamps.push_back(tseq.timestamps[i][j]);
      }
    }
  }
}

void RuntimeProfile::Update(
    const TRuntimeProfileTree& thrift_profile, bool add_default_counters) {
  int idx = 0;
  Update(thrift_profile.nodes, &idx, add_default_counters);
  DCHECK_EQ(idx, thrift_profile.nodes.size());
  // Re-compute the total time for the entire profile tree.
  ComputeTimeInProfile();
}

void RuntimeProfile::Update(
    const vector<TRuntimeProfileNode>& nodes, int* idx, bool add_default_counters) {
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
    // if children are added after the first Update() call (IMPALA-6694).
    // E.g. if the first update sends [B, D] and the second update sends [A, B, C, D],
    // then this code makes sure that children_ is [A, B, C, D] afterwards.
    ChildVector::iterator insert_pos = children_.begin();
    // Update children with matching names; create new ones if they don't match.
    for (int i = 0; i < node.num_children; ++i) {
      const TRuntimeProfileNode& tchild = nodes[*idx];
      bool is_timed = add_default_counters && !UntimedProfileNode(tchild.name);
      RuntimeProfile* child = dynamic_cast<RuntimeProfile*>(AddOrCreateChild(
          tchild.name, &insert_pos,
          [this, tchild, is_timed]() {
            RuntimeProfile* child2 = Create(pool_, tchild.name, is_timed);
            child2->metadata_ = tchild.node_metadata;
            return child2;
          },
          tchild.indent));
      DCHECK(child != nullptr);
      child->Update(nodes, idx, is_timed);
    }
  }
}

void RuntimeProfileBase::ComputeTimeInProfile() {
  // Recurse on children. After this, childrens' total time is up to date.
  int64_t children_total_time = 0;
  {
    lock_guard<SpinLock> l(children_lock_);
    for (int i = 0; i < children_.size(); ++i) {
      children_[i].first->ComputeTimeInProfile();
      children_total_time += children_[i].first->total_time();
    }
  }
  // IMPALA-5200: Take the max, because the parent includes all of the time from the
  // children, whether or not its total time counter has been updated recently enough
  // to see this.
  int64_t total_time_ns = max(children_total_time, total_time_counter()->value());

  // If a local time counter exists, use its value as local time. Otherwise, derive the
  // local time from total time and the child time.
  int64_t local_time_ns = 0;
  bool has_local_time_counter = false;
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    CounterMap::const_iterator itr = counter_map_.find(LOCAL_TIME_COUNTER_NAME);
    if (itr != counter_map_.end()) {
      local_time_ns = itr->second->value();
      has_local_time_counter = true;
    }
  }

  if (!has_local_time_counter) {
    local_time_ns = total_time_ns - children_total_time;
    local_time_ns -= inactive_timer()->value();
  }
  // Counters have some margin, set to 0 if it was negative.
  local_time_ns = ::max<int64_t>(0, local_time_ns);
  double local_time_frac =
      min(1.0, static_cast<double>(local_time_ns) / total_time_ns);
  total_time_ns_.Store(total_time_ns);
  local_time_ns_.Store(local_time_ns);
  local_time_frac_.Store(BitcastToInt64(local_time_frac));
}

void RuntimeProfile::AddChild(
    RuntimeProfileBase* child, bool indent, RuntimeProfile* loc) {
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

void RuntimeProfileBase::AddChildLocked(
    RuntimeProfileBase* child, bool indent, ChildVector::iterator insert_pos) {
  children_lock_.DCheckLocked();
  DCHECK(child != NULL);
  if (child_map_.count(child->name()) > 0) {
    // This child has already been added, so do nothing.
    // Otherwise, the map and vector will be out of sync.
    return;
  }
  child_map_[child->name()] = child;
  children_.insert(insert_pos, make_pair(child, indent));
}

void RuntimeProfile::PrependChild(RuntimeProfileBase* child, bool indent) {
  lock_guard<SpinLock> l(children_lock_);
  AddChildLocked(child, indent, children_.begin());
}

RuntimeProfile* RuntimeProfile::CreateChild(
    const string& name, bool indent, bool prepend, bool add_default_counters) {
  lock_guard<SpinLock> l(children_lock_);
  DCHECK(child_map_.find(name) == child_map_.end());
  RuntimeProfile* child = Create(pool_, name, add_default_counters);
  AddChildLocked(child, indent, prepend ? children_.begin() : children_.end());
  return child;
}

void RuntimeProfileBase::GetChildren(vector<RuntimeProfileBase*>* children) const {
  children->clear();
  lock_guard<SpinLock> l(children_lock_);
  for (const auto& entry : children_) children->push_back(entry.first);
}

void RuntimeProfileBase::GetAllChildren(vector<RuntimeProfileBase*>* children) {
  lock_guard<SpinLock> l(children_lock_);
  for (ChildMap::iterator i = child_map_.begin(); i != child_map_.end(); ++i) {
    children->push_back(i->second);
    i->second->GetAllChildren(children);
  }
}

int RuntimeProfileBase::num_counters() const {
  std::lock_guard<SpinLock> l(counter_map_lock_);
  return counter_map_.size();
}

void RuntimeProfileBase::AddSkewInfo(RuntimeProfileBase* root, double threshold) {
  lock_guard<SpinLock> l(children_lock_);
  DCHECK(root);
  // A map of specs for profiles and average counters in profiles that can potentially
  // harbor skews. Each spec is a pair of a profile name prefix and a list of counter
  // names. Lookup of the map for a profile name prefix is O(1).
  typedef string NodeNamePrefix;
  typedef vector<string> CounterNames;
  static const unordered_map<NodeNamePrefix, CounterNames> skew_profile_specs = {
      {"KUDU_SCAN_NODE", {"RowsRead"}}, {"HDFS_SCAN_NODE", {"RowsRead"}},
      {"HASH_JOIN_NODE", {"ProbeRows", "BuildRows"}},
      {"GroupingAggregator", {"RowsReturned"}}, {"EXCHANGE_NODE", {"RowsReturned"}},
      {"SORT_NODE", {"RowsReturned"}}};
  // If this profile can potentially harbor skews, identify the corresponding counters
  // within it and verify.
  for (auto& skew_profile_it : skew_profile_specs) {
    // Test if the profile name prefix is indeed a prefix of the profile name.
    if (name().find(skew_profile_it.first) == 0) {
      for (auto& counter_name_it : skew_profile_it.second) {
        auto counter_it = counter_map_.find(counter_name_it);
        // Test if the counter name is in the counter spec.
        if (counter_it != counter_map_.end()) {
          auto average_counter =
              dynamic_cast<RuntimeProfile::AveragedCounter*>(counter_it->second);
          string details;
          // Skew detection can only be done for an average counter.
          if (average_counter && average_counter->HasSkew(threshold, &details)) {
            // Skew detected:
            // Log the profile name in the 'root' profile
            root->AddInfoStringInternal(SKEW_SUMMARY, name(), true);
            // Log the counter name and skew details in 'this' profile
            this->AddInfoStringInternal(
                SKEW_DETAILS, StrCat(counter_name_it, " ", details), true);
          }
        }
      }
    }
  }
  // Keep looking from within each child profile.
  for (auto child : children_) {
    child.first->AddSkewInfo(root, threshold);
  }
}

bool RuntimeProfileBase::ParseVerbosity(const string& str, Verbosity* out) {
  string lower = to_lower_copy(str);
  if (lower == "minimal" || lower == "0") {
    *out = Verbosity::MINIMAL;
    return true;
  } else if (lower == "legacy" || lower == "1") {
    *out = Verbosity::LEGACY;
    return true;
  } else if (lower == "default" || lower == "2") {
    *out = Verbosity::DEFAULT;
    return true;
  } else if (lower == "extended" || lower == "3") {
    *out = Verbosity::EXTENDED;
    return true;
  } else if (lower == "full" || lower == "4") {
    *out = Verbosity::FULL;
    return true;
  } else {
    return false;
  }
}

void RuntimeProfile::SortChildrenByTotalTime() {
  lock_guard<SpinLock> l(children_lock_);
  // Create a snapshot of total time values so that they don't change while we're
  // sorting. Sort the <total_time, index> pairs, then reshuffle children_.
  vector<pair<int64_t, int64_t>> total_times;
  total_times.reserve(children_.size());
  for (int i = 0; i < children_.size(); ++i) {
    total_times.emplace_back(children_[i].first->total_time_counter()->value(), i);
  }
  // Order by descending total time.
  sort(total_times.begin(), total_times.end(),
      [](const pair<int64_t, int64_t>& p1, const pair<int64_t, int64_t>& p2) {
        return p1.first > p2.first;
      });
  ChildVector new_children;
  new_children.reserve(total_times.size());
  for (const auto& p : total_times) new_children.emplace_back(children_[p.second]);
  children_ = move(new_children);
}

void RuntimeProfileBase::AddInfoString(const string& key, const string& value) {
  return AddInfoStringInternal(key, value, false);
}

void RuntimeProfile::AddInfoStringRedacted(const string& key, const string& value) {
  return AddInfoStringInternal(key, value, false, true);
}

void RuntimeProfile::AppendInfoString(const string& key, const string& value) {
  return AddInfoStringInternal(key, value, true);
}

void RuntimeProfileBase::AddInfoStringInternal(
    const string& key, string value, bool append, bool redact) {
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

RuntimeProfileBase::Counter* RuntimeProfileBase::GetCounter(const string& name) const {
  lock_guard<SpinLock> l(counter_map_lock_);
  if (auto iter = counter_map_.find(name); iter != counter_map_.cend()) {
    return iter->second;
  }
  return NULL;
}

RuntimeProfileBase::SummaryStatsCounter* RuntimeProfile::GetSummaryStatsCounter(
    const string& name) {
  lock_guard<SpinLock> l(summary_stats_map_lock_);
  if (summary_stats_map_.find(name) != summary_stats_map_.end()) {
    return summary_stats_map_[name];
  }
  return nullptr;
}

void RuntimeProfileBase::GetCounters(const string& name, vector<Counter*>* counters) {
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

void RuntimeProfile::ToJson(Document* d) const {
  return ToJson(DefaultVerbosity(), d);
}

void RuntimeProfile::ToJson(Verbosity verbosity, Document* d) const {
  // queryObj that stores all JSON format profile information
  Value queryObj(kObjectType);
  ToJsonHelper(verbosity, &queryObj, d);
  GenericStringRef<char> sectionRef(d->HasMember("internal_profile") ?
      "profile_json" : "contents");
  d->RemoveMember(sectionRef);
  d->AddMember(sectionRef, queryObj, d->GetAllocator());
}

void RuntimeProfile::JsonProfileToString(
    const rapidjson::Document& json_profile, bool pretty, ostream* string_output) {
  // Serialize to JSON without extra whitespace/formatting.
  rapidjson::StringBuffer sb;
  if (pretty) {
    PrettyWriter<StringBuffer> writer(sb);
    writer.SetIndent('\t', 1);
    writer.SetFormatOptions(kFormatSingleLineArray);
    json_profile.Accept(writer);
  } else {
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    json_profile.Accept(writer);
  }
  *string_output << sb.GetString();
}

void RuntimeProfileBase::ToJsonCounters(Verbosity verbosity, Value* parent, Document* d,
    const string& counter_name, const CounterMap& counter_map,
    const ChildCounterMap& child_counter_map) const {
  auto& allocator = d->GetAllocator();
  ChildCounterMap::const_iterator itr = child_counter_map.find(counter_name);
  if (itr != child_counter_map.end()) {
    const set<string>& child_counters = itr->second;
    for (const string& child_counter: child_counters) {
      CounterMap::const_iterator iter = counter_map.find(child_counter);
      if (iter == counter_map.end()) continue;

      Value counter(kObjectType);
      iter->second->ToJson(verbosity, *d, &counter);
      Value child_counter_json(child_counter.c_str(), child_counter.size(), allocator);
      counter.AddMember("counter_name", child_counter_json, allocator);

      Value child_counters_json(kArrayType);
      ToJsonCounters(verbosity, &child_counters_json, d, child_counter, counter_map,
          child_counter_map);
      if (!child_counters_json.Empty()){
        counter.AddMember("child_counters", child_counters_json, allocator);
      }
      parent->PushBack(counter, allocator);
    }
  }
}

void RuntimeProfileBase::ToJsonHelper(
    Verbosity verbosity, Value* parent, Document* d) const {
  Document::AllocatorType& allocator = d->GetAllocator();
  // Create copy of counter_map_ and child_counter_map_ so we don't need to hold lock
  // while we call value() on the counters (some of those might be DerivedCounters).
  CounterMap counter_map;
  ChildCounterMap child_counter_map;
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    counter_map = counter_map_;
    child_counter_map = child_counter_map_;
  }

  // 1. Name
  Value name(name_.c_str(), allocator);
  parent->AddMember("profile_name", name, allocator);

  // 2. Num_children
  parent->AddMember("num_children", children_.size(), allocator);

  // 3. Metadata
  if (metadata_.__isset.plan_node_id || metadata_.__isset.data_sink_id){
    Value node_metadata_json(kObjectType);
    if (metadata_.__isset.plan_node_id){
      node_metadata_json.AddMember("plan_node_id", metadata_.plan_node_id, allocator);
    }
    if (metadata_.__isset.data_sink_id){
      node_metadata_json.AddMember("data_sink_id", metadata_.data_sink_id, allocator);
    }
    parent->AddMember("node_metadata", node_metadata_json, allocator);
  }

  // 4. Info strings
  // TODO: IMPALA-9846: move to subclass once 'info_strings_' is also moved
  {
    lock_guard<SpinLock> l(info_strings_lock_);
    if (!info_strings_.empty()) {
      Value info_strings_json(kArrayType);
      for (const string& key : info_strings_display_order_) {
        Value info_string_json(kObjectType);
        Value key_json(key.c_str(), allocator);
        auto value_itr = info_strings_.find(key);
        DCHECK(value_itr != info_strings_.end());
        Value value_json(value_itr->second.c_str(), allocator);
        info_string_json.AddMember("key", key_json, allocator);
        info_string_json.AddMember("value", value_json, allocator);
        info_strings_json.PushBack(info_string_json, allocator);
      }
      parent->AddMember("info_strings", info_strings_json, allocator);
    }
  }

  // 5. Counters and info strings from subclasses
  ToJsonSubclass(verbosity, parent, d);

  // 6. Counters
  Value counters(kArrayType);
  ToJsonCounters(verbosity, &counters, d, ROOT_COUNTER, counter_map, child_counter_map);
  if (!counters.Empty()) {
    parent->AddMember("counters", counters, allocator);
  }

  // 7. Children Runtime Profiles
  //
  // Create copy of children_ so we don't need to hold lock while we call
  // ToJsonHelper() on the children.
  ChildVector children;
  {
    lock_guard<SpinLock> l(children_lock_);
    children = children_;
  }

  if (!children.empty()) {
    Value child_profiles(kArrayType);
    for (int i = 0; i < children.size(); ++i) {
      RuntimeProfileBase* profile = children[i].first;
      Value child_profile(kObjectType);
      profile->ToJsonHelper(verbosity, &child_profile, d);
      child_profiles.PushBack(child_profile, allocator);
    }
    parent->AddMember("child_profiles", child_profiles, allocator);
  }
}

void RuntimeProfile::ToJsonSubclass(
    Verbosity verbosity, Value* parent, Document* d) const {
  Document::AllocatorType& allocator = d->GetAllocator();
  // 1. Events
  {
    lock_guard<SpinLock> l(event_sequence_lock_);
    if (!event_sequence_map_.empty()) {
      Value event_sequences_json(kArrayType);
      for (EventSequenceMap::const_iterator it = event_sequence_map_.begin();
           it != event_sequence_map_.end(); ++it) {
        Value event_sequence_json(kObjectType);
        it->second->ToJson(verbosity, *d, &event_sequence_json);
        event_sequences_json.PushBack(event_sequence_json, allocator);
      }
      parent->AddMember("event_sequences", event_sequences_json, allocator);
    }
  }

  // 2. Time_series_counter_map
  {
    // Print all time series counters as following:
    //    - <Name> (<period>): <val1>, <val2>, <etc>
    lock_guard<SpinLock> l(counter_map_lock_);
    if (!time_series_counter_map_.empty()) {
      Value time_series_counters_json(kArrayType);
      for (const TimeSeriesCounterMap::value_type& v : time_series_counter_map_) {
        TimeSeriesCounter* counter = v.second;
        Value time_series_json(kObjectType);
        counter->ToJson(verbosity, *d, &time_series_json);
        time_series_counters_json.PushBack(time_series_json, allocator);
      }
      parent->AddMember("time_series_counters", time_series_counters_json, allocator);
    }
  }

  // 3. SummaryStatsCounter
  {
    lock_guard<SpinLock> l(summary_stats_map_lock_);
    if (!summary_stats_map_.empty()) {
      Value summary_stats_counters_json(kArrayType);
      for (const SummaryStatsCounterMap::value_type& v : summary_stats_map_) {
        Value summary_stats_counter(kObjectType);
        Value summary_name_json(v.first.c_str(), v.first.size(), allocator);
        v.second->ToJson(verbosity, *d, &summary_stats_counter);
        summary_stats_counter.AddMember("counter_name", summary_name_json, allocator);
        summary_stats_counters_json.PushBack(summary_stats_counter, allocator);
      }
      parent->AddMember("summary_stats_counters", summary_stats_counters_json, allocator);
    }
  }
}

void RuntimeProfileBase::PrettyPrint(ostream* s, const string& prefix) const {
  PrettyPrint(DefaultVerbosity(), s, prefix);
}

// Print the profile:
//  1. Profile Name
//  2. Info Strings
//  3. Counters
//  4. Children
void RuntimeProfileBase::PrettyPrint(
    Verbosity verbosity, ostream* s, const string& prefix) const {
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

  int num_input_profiles = GetNumInputProfiles();
  Counter* total_time = total_time_counter();
  DCHECK(total_time != nullptr);
  stream.flags(ios::fixed);
  stream << prefix << name_;
  if (num_input_profiles != 1) {
    stream << " [" << num_input_profiles << " instances]";
  }
  stream << ":";
  if (total_time->value() != 0) {
    stream << "(Total: " << PrettyPrinter::Print(total_time->value(), total_time->unit())
           << ", non-child: "
           << PrettyPrinter::Print(local_time_ns_.Load(), TUnit::TIME_NS)
           << ", % non-child: " << setprecision(2)
           << BitcastFromInt64<double>(local_time_frac_.Load()) * 100 << "%)";
  }
  stream << endl;

  // TODO: IMPALA-9846: move to RuntimeProfile::PrettyPrintInfoStrings() once we move
  // 'info_strings_'.
  {
    lock_guard<SpinLock> l(info_strings_lock_);
    for (const string& key: info_strings_display_order_) {
      stream << prefix << "  " << key << ": " << info_strings_.find(key)->second << endl;
    }
  }

  PrettyPrintInfoStrings(s, prefix, verbosity);
  PrettyPrintSubclassCounters(s, prefix, verbosity);
  RuntimeProfileBase::PrintChildCounters(
      prefix, verbosity, ROOT_COUNTER, counter_map, child_counter_map, s);

  // Create copy of children_ so we don't need to hold lock while we call
  // PrettyPrint() on the children.
  ChildVector children;
  {
    lock_guard<SpinLock> l(children_lock_);
    children = children_;
  }
  for (int i = 0; i < children.size(); ++i) {
    RuntimeProfileBase* profile = children[i].first;
    bool indent = children[i].second;
    profile->PrettyPrint(verbosity, s, prefix + (indent ? "  " : ""));
  }
}

void RuntimeProfile::PrettyPrintInfoStrings(
    ostream* s, const string& prefix, Verbosity verbosity) const {}

// Helper to pretty-print the data for a time series counter. 'num' is the number of
// values in 'samples'.
//
// The format is as follow:
//    - <Label> (<period>): <val1>, <val2>, <etc>
static void PrettyPrintTimeSeries(const string& label, const int64_t* samples, int num,
    int period_ms, const string& prefix, TUnit::type unit, ostream* s) {
  ostream& stream = *s;
  if (num == 0) return;
  // Clamp number of printed values at 64, the maximum number of values in the
  // SamplingTimeSeriesCounter.
  int step = 1 + (num - 1) / 64;
  period_ms *= step;
  stream << prefix << "   - " << label << " ("
         << PrettyPrinter::Print(period_ms * 1000000L, TUnit::TIME_NS) << "): ";
  for (int i = 0; i < num; i += step) {
    stream << PrettyPrinter::Print(samples[i], unit);
    if (i + step < num) stream << ", ";
  }
  if (step > 1) {
    stream << " (Showing " << ((num + 1) / step) << " of " << num << " values from "
      "Thrift Profile)";
  }
  stream << endl;
}

void RuntimeProfile::PrettyPrintTimeline(ostream* s, const string& prefix) const {
  ostream& stream = *s;
  // Print all the event timers as the following:
  // <EventKey> Timeline: 2s719ms
  //     - Event 1: 6.522us (6.522us)
  //     - Event 2: 2s288ms (2s288ms)
  //     - Event 3: 2s410ms (121.138ms)
  // The times in parentheses are the time elapsed since the last event.
  vector<EventSequence::Event> events;
  lock_guard<SpinLock> l(event_sequence_lock_);
  for (const auto& event_sequence : event_sequence_map_) {
    // If the stopwatch has never been started (e.g. because this sequence came from
    // Thrift), look for the last element to tell us the total runtime. For
    // currently-updating sequences, it's better to use the stopwatch value because that
    // updates continuously.
    int64_t last = event_sequence.second->ElapsedTime();
    event_sequence.second->GetEvents(&events);
    if (last == 0 && events.size() > 0) last = events.back().second;
    stream << prefix << event_sequence.first << ": "
           << PrettyPrinter::Print(last, TUnit::TIME_NS)
           << endl;

    int64_t prev = 0L;
    event_sequence.second->GetEvents(&events);
    for (const EventSequence::Event& event: events) {
      stream << prefix << "   - " << event.first << ": "
             << PrettyPrinter::Print(event.second, TUnit::TIME_NS) << " ("
             << PrettyPrinter::Print(event.second - prev, TUnit::TIME_NS) << ")"
             << endl;
      prev = event.second;
    }
  }
}

void RuntimeProfile::PrettyPrintSubclassCounters(
    ostream* s, const string& prefix, Verbosity verbosity) const {
  PrettyPrintTimeline(s, prefix + "  ");

  {
    // Print time series counters.
    lock_guard<SpinLock> l(counter_map_lock_);
    for (const auto& v : time_series_counter_map_) {
      const TimeSeriesCounter* counter = v.second;
      lock_guard<SpinLock> l(counter->lock_);
      int num, period;
      const int64_t* samples = counter->GetSamplesLocked(&num, &period);
      PrettyPrintTimeSeries(v.first, samples, num, period, prefix, counter->unit(), s);
    }
  }

  {
    lock_guard<SpinLock> l(summary_stats_map_lock_);
    // Print all SummaryStatsCounters as following:
    // <Name>: (Avg: <value> ; Min: <min_value> ; Max: <max_value> ;
    // Number of samples: <total>)
    for (const auto& v : summary_stats_map_) {
      v.second->PrettyPrint(prefix, v.first, verbosity, s);
    }
  }
}

Status RuntimeProfile::Compress(vector<uint8_t>* out) const {
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

  int64_t max_compressed_size = compressor->MaxOutputLen(serialized_buffer.size());
  DCHECK_GT(max_compressed_size, 0);
  out->resize(max_compressed_size);
  int64_t result_len = out->size();
  uint8_t* compressed_buffer_ptr = out->data();
  RETURN_IF_ERROR(compressor->ProcessBlock(true, serialized_buffer.size(),
      serialized_buffer.data(), &result_len, &compressed_buffer_ptr));
  out->resize(result_len);
  return Status::OK();
}

Status RuntimeProfile::DecompressToThrift(
    const vector<uint8_t>& compressed_profile, TRuntimeProfileTree* out) {
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
  RETURN_IF_ERROR(decompressor->ProcessBlock(false, compressed_profile.size(),
      compressed_profile.data(), &result_len, &decompressed_buffer));

  uint32_t deserialized_len = static_cast<uint32_t>(result_len);
  RETURN_IF_ERROR(
      DeserializeThriftMsg(decompressed_buffer, &deserialized_len, true, out));
  return Status::OK();
}

Status RuntimeProfile::DecompressToProfile(
    const vector<uint8_t>& compressed_profile, ObjectPool* pool, RuntimeProfile** out) {
  TRuntimeProfileTree thrift_profile;
  RETURN_IF_ERROR(
      RuntimeProfile::DecompressToThrift(compressed_profile, &thrift_profile));
  *out = RuntimeProfile::CreateFromThrift(pool, thrift_profile);
  return Status::OK();
}

Status RuntimeProfile::SerializeToArchiveString(string* out) const {
  stringstream ss;
  RETURN_IF_ERROR(SerializeToArchiveString(&ss));
  *out = ss.str();
  return Status::OK();
}

Status RuntimeProfile::SerializeToArchiveString(stringstream* out) const {
  vector<uint8_t> compressed_buffer;
  RETURN_IF_ERROR(Compress(&compressed_buffer));
  Base64Encode(compressed_buffer, out);
  return Status::OK();
}

Status RuntimeProfile::DeserializeFromArchiveString(
    const std::string& archive_str, TRuntimeProfileTree* out) {
  int64_t decoded_max;
  if (!Base64DecodeBufLen(archive_str.c_str(), archive_str.size(), &decoded_max)) {
    return Status("Error in DeserializeFromArchiveString: Base64DecodeBufLen failed.");
  }

  vector<uint8_t> decoded_buffer;
  decoded_buffer.resize(decoded_max);
  unsigned decoded_len;
  if (!Base64Decode(archive_str.c_str(), archive_str.size(), decoded_max,
          reinterpret_cast<char*>(decoded_buffer.data()), &decoded_len)) {
    return Status("Error in DeserializeFromArchiveString: Base64Decode failed.");
  }
  decoded_buffer.resize(decoded_len);
  return DecompressToThrift(decoded_buffer, out);
}

Status RuntimeProfile::CreateFromArchiveString(const string& archive_str,
    ObjectPool* pool, RuntimeProfile** out, int32_t* profile_version_out) {
  TRuntimeProfileTree tree;
  RETURN_IF_ERROR(DeserializeFromArchiveString(archive_str, &tree));
  *profile_version_out = GetProfileVersion(tree);
  *out = RuntimeProfile::CreateFromThrift(pool, tree);
  return Status::OK();
}

void RuntimeProfile::SetTExecSummary(const TExecSummary& summary) {
  lock_guard<SpinLock> l(t_exec_summary_lock_);
  t_exec_summary_ = summary;
}

void RuntimeProfileBase::ToThrift(TRuntimeProfileTree* tree) const {
  tree->nodes.clear();
  ToThriftHelper(&tree->nodes);
  if (FLAGS_gen_experimental_profile) tree->__set_profile_version(2);
}

void RuntimeProfile::ToThrift(TRuntimeProfileTree* tree) const {
  RuntimeProfileBase::ToThrift(tree);
  ExecSummaryToThrift(tree);
}

void RuntimeProfileBase::ToThriftHelper(vector<TRuntimeProfileNode>* nodes) const {
  // Use a two-pass approach where we first collect nodes with a pre-order traversal and
  // then serialize them. This is done to allow reserving the full vector of
  // TRuntimeProfileNodes upfront - copying the constructed nodes when resizing the vector
  // can be very expensive - see IMPALA-9378.
  vector<CollectedNode> preorder_nodes;
  CollectNodes(false, &preorder_nodes);

  nodes->reserve(preorder_nodes.size());
  for (CollectedNode& preorder_node : preorder_nodes) {
    nodes->emplace_back();
    TRuntimeProfileNode& node = nodes->back();
    preorder_node.node->ToThriftHelper(&node);
    node.indent = preorder_node.indent;
    node.num_children = preorder_node.num_children;
  }
}

void RuntimeProfileBase::ToThriftHelper(TRuntimeProfileNode* out_node) const {
  // Use a reference to reduce code churn. TODO: clean this up to use a pointer later.
  TRuntimeProfileNode& node = *out_node;
  node.name = name_;
  // Set the required metadata field to the plan node ID for compatibility with any tools
  // that rely on the plan node id being set there.
  node.metadata = metadata_.__isset.plan_node_id ? metadata_.plan_node_id : -1;
  // Thrift requires exactly one field of a union to be set so we only mark node_metadata
  // as set if that is the case.
  if (metadata_.__isset.plan_node_id || metadata_.__isset.data_sink_id) {
    node.__set_node_metadata(metadata_);
  }

  // Copy of the entries. We need to do this because calling value() on entries in
  // 'counter_map_' might invoke an arbitrary function. Use vector instead of map
  // for efficiency of creation and iteration. Only copy references to string keys.
  // std::map entries are stable.
  vector<pair<const string&, const Counter*>> counter_map_entries;
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    counter_map_entries.reserve(counter_map_.size());
    std::copy(counter_map_.begin(), counter_map_.end(),
        std::back_inserter(counter_map_entries));
    node.child_counters_map = child_counter_map_;
  }

  // TODO: IMPALA-9846: move to RuntimeProfile::ToThriftSubclass() once 'info_strings_'
  // is moved.
  {
    lock_guard<SpinLock> l(info_strings_lock_);
    out_node->info_strings = info_strings_;
    out_node->info_strings_display_order = info_strings_display_order_;
  }

  ToThriftSubclass(counter_map_entries, out_node);
}

void RuntimeProfile::ToThriftSubclass(
    vector<pair<const string&, const Counter*>>& counter_map_entries,
    TRuntimeProfileNode* out_node) const {
  out_node->counters.reserve(counter_map_entries.size());
  for (const auto& entry : counter_map_entries) {
    out_node->counters.emplace_back();
    TCounter& counter = out_node->counters.back();
    counter.name = entry.first;
    counter.value = entry.second->value();
    counter.unit = entry.second->unit();
  }

  {
    lock_guard<SpinLock> l(counter_map_lock_);
    if (time_series_counter_map_.size() != 0) {
      out_node->__isset.time_series_counters = true;
      out_node->time_series_counters.reserve(time_series_counter_map_.size());
      for (const auto& val : time_series_counter_map_) {
        out_node->time_series_counters.emplace_back();
        val.second->ToThrift(&out_node->time_series_counters.back());
      }
    }
  }

  {
    vector<EventSequence::Event> events;
    lock_guard<SpinLock> l(event_sequence_lock_);
    if (event_sequence_map_.size() != 0) {
      out_node->__isset.event_sequences = true;
      out_node->event_sequences.reserve(event_sequence_map_.size());
      for (const auto& val : event_sequence_map_) {
        out_node->event_sequences.emplace_back();
        TEventSequence& seq = out_node->event_sequences.back();
        seq.name = val.first;
        val.second->GetEvents(&events);
        seq.labels.reserve(events.size());
        seq.timestamps.reserve(events.size());
        for (const EventSequence::Event& ev: events) {
          seq.labels.push_back(move(ev.first));
          seq.timestamps.push_back(ev.second);
        }
      }
    }
  }

  {
    lock_guard<SpinLock> l(summary_stats_map_lock_);
    if (summary_stats_map_.size() != 0) {
      out_node->__isset.summary_stats_counters = true;
      out_node->summary_stats_counters.resize(summary_stats_map_.size());
      int idx = 0;
      for (const SummaryStatsCounterMap::value_type& val: summary_stats_map_) {
        val.second->ToThrift(&out_node->summary_stats_counters[idx++], val.first);
      }
    }
  }
}

void RuntimeProfileBase::CollectNodes(bool indent, vector<CollectedNode>* nodes) const {
  lock_guard<SpinLock> l(children_lock_);
  nodes->emplace_back(this, indent, children_.size());
  for (const auto& child : children_) {
    child.first->CollectNodes(child.second, nodes);
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

void RuntimeProfile::GetTimeline(string* output) const {
  DCHECK(output != nullptr);
  stringstream stream;
  PrettyPrintTimeline(&stream, "");
  *output = stream.str();
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
    const Counter* total_counter, const Counter* timer) {
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

RuntimeProfileBase::Counter* RuntimeProfile::AddRateCounter(
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
    Counter* dst_counter = AddCounterLocked(name, dst_unit, ROOT_COUNTER, &created);
    if (!created) return dst_counter;
    rate_counters_.push_back(dst_counter);
    PeriodicCounterUpdater::RegisterPeriodicCounter(src_counter, NULL, dst_counter,
        PeriodicCounterUpdater::RATE_COUNTER);
    has_active_periodic_counters_ = true;
    return dst_counter;
  }
}

RuntimeProfileBase::Counter* RuntimeProfile::AddRateCounter(
    const string& name, SampleFunction fn, TUnit::type dst_unit) {
  lock_guard<SpinLock> l(counter_map_lock_);
  bool created;
  Counter* dst_counter = AddCounterLocked(name, dst_unit, ROOT_COUNTER, &created);
  if (!created) return dst_counter;
  rate_counters_.push_back(dst_counter);
  PeriodicCounterUpdater::RegisterPeriodicCounter(NULL, fn, dst_counter,
      PeriodicCounterUpdater::RATE_COUNTER);
  has_active_periodic_counters_ = true;
  return dst_counter;
}

RuntimeProfileBase::Counter* RuntimeProfile::AddSamplingCounter(
    const string& name, Counter* src_counter) {
  DCHECK(src_counter->unit() == TUnit::UNIT);
  lock_guard<SpinLock> l(counter_map_lock_);
  bool created;
  Counter* dst_counter =
      AddCounterLocked(name, TUnit::DOUBLE_VALUE, ROOT_COUNTER, &created);
  if (!created) return dst_counter;
  sampling_counters_.push_back(dst_counter);
  PeriodicCounterUpdater::RegisterPeriodicCounter(src_counter, NULL, dst_counter,
      PeriodicCounterUpdater::SAMPLING_COUNTER);
  has_active_periodic_counters_ = true;
  return dst_counter;
}

RuntimeProfileBase::Counter* RuntimeProfile::AddSamplingCounter(
    const string& name, SampleFunction sample_fn) {
  lock_guard<SpinLock> l(counter_map_lock_);
  bool created;
  Counter* dst_counter =
      AddCounterLocked(name, TUnit::DOUBLE_VALUE, ROOT_COUNTER, &created);
  if (!created) return dst_counter;
  sampling_counters_.push_back(dst_counter);
  PeriodicCounterUpdater::RegisterPeriodicCounter(NULL, sample_fn, dst_counter,
      PeriodicCounterUpdater::SAMPLING_COUNTER);
  has_active_periodic_counters_ = true;
  return dst_counter;
}

vector<RuntimeProfileBase::Counter*>* RuntimeProfile::AddBucketingCounters(
    Counter* src_counter, int num_buckets) {
  lock_guard<SpinLock> l(counter_map_lock_);
  vector<Counter*>* buckets = pool_->Add(new vector<Counter*>);
  for (int i = 0; i < num_buckets; ++i) {
    buckets->push_back(pool_->Add(new Counter(TUnit::DOUBLE_VALUE, 0)));
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

void RuntimeProfileBase::PrintChildCounters(const string& prefix, Verbosity verbosity,
    const string& counter_name, const CounterMap& counter_map,
    const ChildCounterMap& child_counter_map, ostream* s) {
  ostream& stream = *s;
  ChildCounterMap::const_iterator itr = child_counter_map.find(counter_name);
  if (itr != child_counter_map.end()) {
    const set<string>& child_counters = itr->second;
    for (const string& child_counter: child_counters) {
      CounterMap::const_iterator iter = counter_map.find(child_counter);
      if (iter == counter_map.end()) continue;
      iter->second->PrettyPrint(prefix, iter->first, verbosity, &stream);
      RuntimeProfileBase::PrintChildCounters(
          prefix + "  ", verbosity, child_counter, counter_map, child_counter_map, s);
    }
  }
}

RuntimeProfileBase::SummaryStatsCounter* RuntimeProfile::AddSummaryStatsCounter(
    const string& name, TUnit::type unit, const std::string& parent_counter_name) {
  lock_guard<SpinLock> l(summary_stats_map_lock_);
  if (summary_stats_map_.find(name) != summary_stats_map_.end()) {
    return summary_stats_map_[name];
  }
  SummaryStatsCounter* counter = pool_->Add(new SummaryStatsCounter(unit));
  summary_stats_map_[name] = counter;
  return counter;
}

RuntimeProfile::TimeSeriesCounter* RuntimeProfile::AddSamplingTimeSeriesCounter(
    const string& name, TUnit::type unit, SampleFunction fn, bool is_system) {
  DCHECK(fn != nullptr);
  lock_guard<SpinLock> l(counter_map_lock_);
  TimeSeriesCounterMap::iterator it = time_series_counter_map_.find(name);
  if (it != time_series_counter_map_.end()) return it->second;
  int32_t update_interval = is_system ?
      FLAGS_periodic_system_counter_update_period_ms :
      FLAGS_periodic_counter_update_period_ms;
  TimeSeriesCounter* counter = pool_->Add(new SamplingTimeSeriesCounter(name,
      unit, fn, update_interval));
  if (is_system) {
    counter->SetIsSystem();
  }
  time_series_counter_map_[name] = counter;
  PeriodicCounterUpdater::RegisterTimeSeriesCounter(counter);
  has_active_periodic_counters_ = true;
  return counter;
}

RuntimeProfile::TimeSeriesCounter* RuntimeProfile::AddSamplingTimeSeriesCounter(
    const string& name, Counter* src_counter, bool is_system) {
  DCHECK(src_counter != NULL);
  return AddSamplingTimeSeriesCounter(name, src_counter->unit(),
      bind(&Counter::value, src_counter), is_system);
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

void RuntimeProfile::SamplingTimeSeriesCounter::Reset() {
    samples_.Reset();
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

void RuntimeProfileBase::ClearChunkedTimeSeriesCounters() {
  {
    lock_guard<SpinLock> l(children_lock_);
    for (int i = 0; i < children_.size(); ++i) {
      children_[i].first->ClearChunkedTimeSeriesCounters();
    }
  }
}

void RuntimeProfile::ClearChunkedTimeSeriesCounters() {
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    for (auto& it : time_series_counter_map_) it.second->Clear();
  }
  RuntimeProfileBase::ClearChunkedTimeSeriesCounters();
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

RuntimeProfileBase::AveragedCounter::AveragedCounter(TUnit::type unit, int num_samples)
  : Counter(unit),
    num_values_(num_samples),
    has_value_(make_unique<AtomicBool[]>(num_samples)),
    values_(make_unique<AtomicInt64[]>(num_samples)) {}

RuntimeProfileBase::AveragedCounter::AveragedCounter(TUnit::type unit,
    const std::vector<bool>& has_value, const std::vector<int64_t>& values)
  : Counter(unit),
    num_values_(values.size()),
    has_value_(make_unique<AtomicBool[]>(values.size())),
    values_(make_unique<AtomicInt64[]>(values.size())) {
  DCHECK_EQ(has_value.size(), values.size());
  Update(0, has_value, values);
}

void RuntimeProfileBase::AveragedCounter::UpdateCounter(Counter* new_counter, int idx) {
  DCHECK_EQ(new_counter->unit(), unit_);
  DCHECK_GE(idx, 0);
  DCHECK_LT(idx, num_values_);
  if (unit_ == TUnit::DOUBLE_VALUE) {
    double new_val = new_counter->double_value();
    values_[idx].Store(BitcastToInt64(new_val));
  } else {
    values_[idx].Store(new_counter->value());
  }
  // Set has_value_ after the value is valid above so that readers won't
  // see invalid values.
  has_value_[idx].Store(true);
}

void RuntimeProfileBase::AveragedCounter::Update(int start_idx,
    const vector<bool>& has_value, const vector<int64_t>& values) {
  DCHECK_EQ(values.size(), has_value.size());
  DCHECK_LE(has_value.size() + start_idx, num_values_);
  for (int i = 0; i < values.size(); ++i) {
    if (has_value[i]) {
      values_[i + start_idx].Store(values[i]);
      // Set has_value_ after the value is valid above so that readers won't
      // see invalid values.
      has_value_[i + start_idx].Store(true);
    }
  }
}

int64_t RuntimeProfileBase::AveragedCounter::value() const {
  return unit_ == TUnit::DOUBLE_VALUE ? ComputeMean<double>() : ComputeMean<int64_t>();
}

template <typename T>
int64_t RuntimeProfileBase::AveragedCounter::ComputeMean() const {
  // Compute the mean in a single pass over the input. We could instead call GetStats(),
  // but this would add some additional overhead when serializing thrift profiles, which
  // include the mean counter values.
  T sum = 0;
  int num_vals = 0;
  for (int i = 0; i < num_values_; ++i) {
    if (has_value_[i].Load()) {
      sum += BitcastFromInt64<T>(values_[i].Load());
      ++num_vals;
    }
  }
  if (num_vals == 0) return 0; // Avoid divide-by-zero.
  return BitcastToInt64(sum / num_vals);
}

template <typename T>
RuntimeProfileBase::AveragedCounter::Stats<T>
RuntimeProfileBase::AveragedCounter::GetStats() const {
  static_assert(std::is_same<T, int64_t>::value || std::is_same<T, double>::value,
      "Only double and int64_t are supported");
  DCHECK_EQ(unit_ == TUnit::DOUBLE_VALUE, (std::is_same<T, double>::value));
  vector<T> vals;
  vals.reserve(num_values_);
  for (int i = 0; i < num_values_; ++i) {
    if (has_value_[i].Load()) vals.push_back(BitcastFromInt64<T>(values_[i].Load()));
  }
  Stats<T> result;

  if (!vals.empty()) {
    sort(vals.begin(), vals.end());
    result.num_vals = vals.size();
    T sum = std::accumulate(vals.begin(), vals.end(), 0L);
    result.mean = sum / result.num_vals;
    result.min = vals[0];
    result.max = vals.back();
    int end_idx = vals.size() - 1;
    result.p50 = vals[end_idx / 2];
    result.p75 = vals[end_idx * 3 / 4];
    result.p90 = vals[end_idx * 9 / 10];
    result.p95 = vals[end_idx * 19 / 20];
  }
  return result;
}

void RuntimeProfileBase::AveragedCounter::PrettyPrint(
    const string& prefix, const string& name, Verbosity verbosity, ostream* s) const {
  if (unit_ == TUnit::DOUBLE_VALUE) {
    PrettyPrintImpl<double>(prefix, name, verbosity, s);
  } else {
    PrettyPrintImpl<int64_t>(prefix, name, verbosity, s);
  }
}

template <typename T>
void RuntimeProfileBase::AveragedCounter::PrettyPrintImpl(
    const string& prefix, const string& name, Verbosity verbosity, ostream* s) const {
  Stats<T> stats = GetStats<T>();
  (*s) << prefix << "   - " << name << ": ";
  if (verbosity <= Verbosity::LEGACY || stats.num_vals == 1) {
    (*s) << PrettyPrinter::Print(stats.mean, unit_, true) << endl;
    return;
  }

  // For counters with <> 1 values, show summary stats and the values.
  (*s) << "mean=" << PrettyPrinter::Print(BitcastToInt64(stats.mean), unit_, true)
       << " min=" << PrettyPrinter::Print(BitcastToInt64(stats.min), unit_, true);
  if (verbosity >= Verbosity::EXTENDED) {
    (*s) << " p50=" << PrettyPrinter::Print(BitcastToInt64(stats.p50), unit_, true)
         << " p75=" << PrettyPrinter::Print(BitcastToInt64(stats.p75), unit_, true)
         << " p90=" << PrettyPrinter::Print(BitcastToInt64(stats.p90), unit_, true)
         << " p95=" << PrettyPrinter::Print(BitcastToInt64(stats.p95), unit_, true);
  }
  (*s) << " max=" << PrettyPrinter::Print(BitcastToInt64(stats.max), unit_, true) << endl;
  // Dump out individual values if we have FULL verbosity of with EXTENDED verbosity when
  // they are not all identical.
  if (verbosity >= Verbosity::FULL
      || (verbosity >= Verbosity::EXTENDED && stats.min != stats.max)) {
    (*s) << prefix << "     [";
    for (int i = 0; i < num_values_; ++i) {
      if (i != 0) {
        if (i % 8 == 0) {
          (*s) << ",\n" << prefix << "      ";
        } else {
          (*s) << ", ";
        }
      }
      if (has_value_[i].Load()) {
        (*s) << PrettyPrinter::Print(values_[i].Load(), unit_, false);
      } else {
        (*s) << "_";
      }
    }
    (*s) << "]\n";
  }
}

void RuntimeProfileBase::AveragedCounter::ToJson(
    Verbosity verbosity, Document& document, Value* val) const {
  if (unit_ == TUnit::DOUBLE_VALUE) {
    ToJsonImpl<double>(verbosity, document, val);
  } else {
    ToJsonImpl<int64_t>(verbosity, document, val);
  }
}

template <typename T>
void RuntimeProfileBase::AveragedCounter::ToJsonImpl(
    Verbosity verbosity, Document& document, Value* val) const {
  Value counter_json(kObjectType);
  auto unit_itr = _TUnit_VALUES_TO_NAMES.find(unit_);
  DCHECK(unit_itr != _TUnit_VALUES_TO_NAMES.end());
  Value unit_json(unit_itr->second, document.GetAllocator());
  counter_json.AddMember("unit", unit_json, document.GetAllocator());

  Stats<T> stats = GetStats<T>();
  if (verbosity <= Verbosity::LEGACY || stats.num_vals == 1) {
    // Generate the traditional single-value representation if we're using the older
    // profile version, or if the detailed statistics would just be noise.
    counter_json.AddMember("value", stats.mean, document.GetAllocator());
    *val = counter_json;
    return;
  }

  // For counters with <> 1 values, show summary stats and the values.
  counter_json.AddMember("count", stats.num_vals, document.GetAllocator());
  if (stats.num_vals > 0) {
    counter_json.AddMember("min", stats.min, document.GetAllocator());
    counter_json.AddMember("max", stats.max, document.GetAllocator());
    // Only include detailed statistics if they are meaningful
    // (i.e. min and max are not the same).
    if (stats.min != stats.max) {
      counter_json.AddMember("mean", stats.mean, document.GetAllocator());
      counter_json.AddMember("p50", stats.p50, document.GetAllocator());
      counter_json.AddMember("p75", stats.p75, document.GetAllocator());
      counter_json.AddMember("p90", stats.p90, document.GetAllocator());
      counter_json.AddMember("p95", stats.p95, document.GetAllocator());
    }
  }
  *val = counter_json;
}


void RuntimeProfileBase::AveragedCounter::ToThrift(
    const string& name, TAggCounter* tcounter) const {
  tcounter->name = name;
  tcounter->unit = unit_;
  tcounter->has_value.resize(num_values_);
  tcounter->values.resize(num_values_);
  for (int i = 0; i < num_values_; ++i) {
    tcounter->has_value[i] = has_value_[i].Load();
    tcounter->values[i] = values_[i].Load();
  }
}

bool RuntimeProfile::AveragedCounter::HasSkew(double threshold, string* details) {
  bool has_skew = false;
  stringstream ss_details;
  if (unit_ == TUnit::DOUBLE_VALUE) {
    has_skew = EvaluateSkewWithCoV<double>(threshold, &ss_details);
  } else {
    has_skew = EvaluateSkewWithCoV<int64_t>(threshold, &ss_details);
  }
  if (has_skew && details) {
    *details = ss_details.str();
    return true;
  }
  return false;
}

template <typename T>
bool RuntimeProfile::AveragedCounter::EvaluateSkewWithCoV(
    double threshold, stringstream* details) {
  T mean = value();
  if (mean > ROW_AVERAGE_LIMIT) {
    double stddev = 0.0;
    string valid_value_list;
    ComputeStddevPForValidValues<T>(&valid_value_list, &stddev);
    double cov = stddev / mean;
    if (cov > threshold) {
      DCHECK(details);
      *details << "(" << valid_value_list << ", CoV=" << std::fixed
               << std::setprecision(2) << cov << ", mean=" << std::fixed
               << std::setprecision(2) << mean << ")";
      return true;
    }
  }
  return false;
}

template <typename T>
void RuntimeProfile::AveragedCounter::ComputeStddevPForValidValues(
    string* valid_value_list, double* stddev) {
  DCHECK(valid_value_list);
  DCHECK(stddev);
  // Collect raw values.
  vector<T> values;
  stringstream ss;
  ss << "[";
  for (int i = 0; i < num_values_; i++) {
    if (has_value_[i].Load()) {
      T v = BitcastFromInt64<T>(values_[i].Load());
      values.emplace_back(v);
    }
  }
  ss << boost::algorithm::join(
      values | boost::adaptors::transformed([](T d) { return std::to_string(d); }), ",");
  ss << "]";
  *valid_value_list = ss.str();
  T mean = value();
  // Finally compute the population stddev.
  StatUtil::ComputeStddevP<T>(values.data(), values.size(), mean, stddev);
}

void RuntimeProfileBase::SummaryStatsCounter::ToThrift(
    TSummaryStatsCounter* counter, const std::string& name) {
  lock_guard<SpinLock> l(lock_);
  counter->name = name;
  counter->unit = unit_;
  counter->sum = sum_;
  counter->total_num_values = total_num_values_;
  counter->min_value = min_;
  counter->max_value = max_;
}

void RuntimeProfileBase::SummaryStatsCounter::ToThrift(const string& name,
    TUnit::type unit, const vector<SummaryStatsCounter*>& counters,
    TAggSummaryStatsCounter* tcounter) {
  int num_vals = counters.size();
  tcounter->name = name;
  tcounter->unit = unit;
  tcounter->has_value.resize(num_vals);
  tcounter->sum.resize(num_vals);
  tcounter->total_num_values.resize(num_vals);
  tcounter->min_value.resize(num_vals);
  tcounter->max_value.resize(num_vals);
  for (int i = 0; i < num_vals; ++i) {
    SummaryStatsCounter* counter = counters[i];
    if (counter == nullptr) continue;
    lock_guard<SpinLock> l(counter->lock_);
    tcounter->has_value[i] = true;
    tcounter->sum[i] = counter->sum_;
    tcounter->min_value[i] = counter->min_;
    tcounter->max_value[i] = counter->max_;
    tcounter->total_num_values[i] = counter->total_num_values_;
  }
}

void RuntimeProfileBase::SummaryStatsCounter::UpdateCounter(int64_t new_value) {
  lock_guard<SpinLock> l(lock_);

  ++total_num_values_;
  sum_ += new_value;
  value_.Store(sum_ / total_num_values_);

  if (new_value < min_) min_ = new_value;
  if (new_value > max_) max_ = new_value;
}

void RuntimeProfileBase::SummaryStatsCounter::SetStats(
    const TSummaryStatsCounter& counter) {
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

void RuntimeProfileBase::SummaryStatsCounter::SetStats(const SummaryStatsCounter& other) {
  lock_guard<SpinLock> ol(other.lock_);
  lock_guard<SpinLock> l(lock_);
  DCHECK_EQ(unit_, other.unit_);
  total_num_values_ = other.total_num_values_;
  min_ = other.min_;
  max_ = other.max_;
  sum_ = other.sum_;
  value_.Store(total_num_values_ == 0 ? 0 : sum_ / total_num_values_);
}

void RuntimeProfileBase::SummaryStatsCounter::Merge(const SummaryStatsCounter& other) {
  lock_guard<SpinLock> ol(other.lock_);
  lock_guard<SpinLock> l(lock_);
  DCHECK_EQ(unit_, other.unit_);
  total_num_values_ += other.total_num_values_;
  min_ = min(min_, other.min_);
  max_ = max(max_, other.max_);
  sum_ += other.sum_;
  value_.Store(total_num_values_ == 0 ? 0 : sum_ / total_num_values_);
}

int64_t RuntimeProfileBase::SummaryStatsCounter::MinValue() {
  lock_guard<SpinLock> l(lock_);
  return min_;
}

int64_t RuntimeProfileBase::SummaryStatsCounter::MaxValue() {
  lock_guard<SpinLock> l(lock_);
  return max_;
}

int32_t RuntimeProfileBase::SummaryStatsCounter::TotalNumValues() {
  lock_guard<SpinLock> l(lock_);
  return total_num_values_;
}

void RuntimeProfileBase::SummaryStatsCounter::PrettyPrint(
    const string& prefix, const string& name, Verbosity verbosity, ostream* s) const {
  ostream& stream = *s;
  stream << prefix << "   - " << name << ": ";
  lock_guard<SpinLock> l(lock_);
  if (total_num_values_ == 0) {
    // No point printing all the stats if number of samples is zero.
    stream << PrettyPrinter::Print(value_.Load(), unit_, true)
           << " (Number of samples: " << total_num_values_ << ")";
  } else {
    stream << "(Avg: " << PrettyPrinter::Print(value_.Load(), unit_, true)
           << " ; Min: " << PrettyPrinter::Print(min_, unit_, true)
           << " ; Max: " << PrettyPrinter::Print(max_, unit_, true)
           << " ; Number of samples: " << total_num_values_ << ")";
  }
  stream << endl;
}

void RuntimeProfileBase::Counter::PrettyPrint(
    const string& prefix, const string& name, Verbosity verbosity, ostream* s) const {
  (*s) << prefix << "   - " << name << ": " << PrettyPrinter::Print(value(), unit_, true)
       << endl;
}

void RuntimeProfileBase::Counter::ToJson(
    Verbosity verbosity, Document& document, Value* val) const {
  Value counter_json(kObjectType);
  if (unit_ == TUnit::DOUBLE_VALUE) {
    counter_json.AddMember("value", double_value(), document.GetAllocator());
  } else {
    counter_json.AddMember("value", value(), document.GetAllocator());
  }
  auto unit_itr = _TUnit_VALUES_TO_NAMES.find(unit_);
  DCHECK(unit_itr != _TUnit_VALUES_TO_NAMES.end());
  Value unit_json(unit_itr->second, document.GetAllocator());
  counter_json.AddMember("unit", unit_json, document.GetAllocator());
  *val = counter_json;
}

void RuntimeProfile::TimeSeriesCounter::ToJson(
    Verbosity verbosity, Document& document, Value* val) {
  lock_guard<SpinLock> lock(lock_);
  Value counter_json(kObjectType);

  Value counter_name_json(name_.c_str(), name_.size(), document.GetAllocator());
  counter_json.AddMember("counter_name", counter_name_json, document.GetAllocator());
  auto unit_itr = _TUnit_VALUES_TO_NAMES.find(unit_);
  DCHECK(unit_itr != _TUnit_VALUES_TO_NAMES.end());
  Value unit_json(unit_itr->second, document.GetAllocator());
  counter_json.AddMember("unit", unit_json, document.GetAllocator());

  int num, period;
  const int64_t* samples = GetSamplesLocked(&num, &period);

  counter_json.AddMember("num", num, document.GetAllocator());
  counter_json.AddMember("period", period, document.GetAllocator());
  stringstream stream;
  // Clamp number of printed values at 64, the maximum number of values in the
  // SamplingTimeSeriesCounter.
  int step = 1 + (num - 1) / 64;
  period *= step;

  for (int i = 0; i < num; i += step) {
    stream << samples[i];
    if (i + step < num) stream << ",";
  }

  Value samples_data_json(stream.str().c_str(), document.GetAllocator());
  counter_json.AddMember("data", samples_data_json, document.GetAllocator());
  *val = counter_json;
}

void RuntimeProfile::EventSequence::ToJson(
    Verbosity verbosity, Document& document, Value* value) {
  lock_guard<SpinLock> event_lock(lock_);
  SortEvents();

  Value event_sequence_json(kObjectType);
  event_sequence_json.AddMember("offset", offset_, document.GetAllocator());

  Value events_json(kArrayType);

  for (const Event& ev: events_) {
    Value event_json(kObjectType);
    Value label_json(ev.first.c_str(), ev.first.size(), document.GetAllocator());
    event_json.AddMember("label", label_json, document.GetAllocator());
    event_json.AddMember("timestamp", ev.second, document.GetAllocator());
    events_json.PushBack(event_json, document.GetAllocator());
  }

  event_sequence_json.AddMember("events", events_json, document.GetAllocator());
  *value = event_sequence_json;
}

AggregatedRuntimeProfile::AggregatedRuntimeProfile(ObjectPool* pool, const string& name,
    int num_input_profiles, bool is_root, bool add_default_counters)
  : RuntimeProfileBase(pool, name,
      pool->Add(new AveragedCounter(TUnit::TIME_NS, num_input_profiles)),
      pool->Add(new AveragedCounter(TUnit::TIME_NS, num_input_profiles)),
      add_default_counters),
    num_input_profiles_(num_input_profiles) {
  DCHECK_GE(num_input_profiles, 0);
  if (is_root) input_profile_names_.resize(num_input_profiles);
}

AggregatedRuntimeProfile* AggregatedRuntimeProfile::Create(ObjectPool* pool,
    const string& name, int num_input_profiles, bool is_root, bool add_default_counters) {
  return pool->Add(new AggregatedRuntimeProfile(
      pool, name, num_input_profiles, is_root, add_default_counters));
}

void AggregatedRuntimeProfile::InitFromThrift(
    const TRuntimeProfileNode& node, ObjectPool* pool) {
  DCHECK(node.__isset.aggregated);
  DCHECK(node.aggregated.__isset.counters);

  if (node.aggregated.__isset.input_profiles) {
    input_profile_names_ = node.aggregated.input_profiles;
  }

  // Most of the logic can be shared with the update code path.
  UpdateFromInstances(node, 0, pool);
}

// Print a sorted vector of indices in compressed form with subsequent indices
// printed as ranges. E.g. [1, 2, 3, 4, 6] would result in "1-4,6".
static void PrettyPrintIndexRanges(ostream* s, const vector<int32_t>& indices) {
  if (indices.empty()) return;
  ostream& stream = *s;
  int32_t start_idx = indices[0];
  int32_t prev_idx = indices[0];
  for (int i = 0; i < indices.size(); ++i) {
    int32_t idx = indices[i];
    if (idx > prev_idx + 1) {
      // Start of a new range. Print the previous range of values.
      stream << start_idx;
      if (start_idx < prev_idx) stream << "-" << prev_idx;
      stream << ",";
      start_idx = idx;
    }
    prev_idx = idx;
  }
  // Print the last range of values.
  stream << start_idx;
  if (start_idx < prev_idx) stream << "-" << prev_idx;
}

void AggregatedRuntimeProfile::PrettyPrintInfoStrings(
    ostream* s, const string& prefix, Verbosity verbosity) const {
  // Aggregated info strings were not shown in averaged profile in legacy mode.
  if (verbosity <= Verbosity::LEGACY) return;
  ostream& stream = *s;
  {
    lock_guard<SpinLock> l(input_profile_name_lock_);
    if (!input_profile_names_.empty()) {
      stream << prefix << "Instances:" << endl;
      for (int i = 0; i < input_profile_names_.size(); ++i) {
          stream << prefix << "  [" << i << "] " << input_profile_names_[i] << endl;
      }
    }
  }

  {
    lock_guard<SpinLock> l(agg_info_strings_lock_);
    for (const auto& entry : agg_info_strings_) {
      map<string, vector<int32_t>> distinct_vals = GroupDistinctInfoStrings(entry.second);
      for (const auto& distinct_entry : distinct_vals) {
        stream << prefix << "  " << entry.first;
        stream << "[";
        PrettyPrintIndexRanges(s, distinct_entry.second);
        stream << "]: " << distinct_entry.first << endl;
      }
    }
  }
}

void AggregatedRuntimeProfile::PrettyPrintSubclassCounters(
    ostream* s, const string& prefix, Verbosity verbosity) const {
  ostream& stream = *s;
  // Legacy profile did not show aggregated time series counters.
  // Showing a time series per instance is too verbose for the default mode,
  // so just show first.
  if (verbosity >= Verbosity::DEFAULT) {
    lock_guard<SpinLock> l(counter_map_lock_);
    for (const auto& v : time_series_counter_map_) {
      const TAggTimeSeriesCounter& counter = v.second;
      int num_to_print = verbosity >= Verbosity::EXTENDED ? counter.values.size() : 1;
      for (int idx = 0; idx < num_to_print; ++idx) {
        const vector<int64_t>& values = counter.values[idx];
        const string& label = Substitute("$0[$1]", counter.name, idx);
        PrettyPrintTimeSeries(label, values.data(), values.size(),
            counter.period_ms[idx], prefix, counter.unit, s);
      }
    }
  }
  // Legacy profile did not show aggregated event sequences.
  if (verbosity >= Verbosity::DEFAULT) {
    // Print all the event timers as the following:
    // <EventKey>[instance index] Timeline: 2s719ms
    //     - Event 1: 6.522us (6.522us)
    //     - Event 2: 2s288ms (2s288ms)
    //     - Event 3: 2s410ms (121.138ms)
    // The times in parentheses are the time elapsed since the last event.
    lock_guard<SpinLock> l(event_sequence_lock_);
    for (const auto& entry : event_sequence_map_) {
      const AggEventSequence& seq = entry.second;
      vector<const string*> labels(seq.labels.size());
      for (const auto& lab_entry : seq.labels) {
        labels[lab_entry.second] = &lab_entry.first;
      }
      for (int idx = 0; idx < seq.label_idxs.size(); ++idx) {
        DCHECK_EQ(seq.label_idxs[idx].size(), seq.timestamps[idx].size());
        int64_t last = seq.timestamps[idx].empty() ? 0 : seq.timestamps[idx].back();
        stream << prefix << "  " << entry.first << "[" << idx << "]: "
             << PrettyPrinter::Print(last, TUnit::TIME_NS)
             << endl;
        int64_t prev = 0L;
        for (int j = 0; j < seq.label_idxs[idx].size(); ++j) {
          const string& label = *labels[seq.label_idxs[idx][j]];
          int64_t ts = seq.timestamps[idx][j];
          stream << prefix << "     - " << label << ": "
                 << PrettyPrinter::Print(ts, TUnit::TIME_NS) << " ("
                 << PrettyPrinter::Print(ts - prev, TUnit::TIME_NS) << ")"
                 << endl;
          prev = ts;
        }
      }
      // Showing an event sequence per instance is too verbose for the default mode,
      // so just show first.
      if (verbosity < Verbosity::EXTENDED) break;
    }
  }

  {
    lock_guard<SpinLock> l(summary_stats_map_lock_);
    for (const auto& v : summary_stats_map_) {
      // Display fully aggregated stats first.
      SummaryStatsCounter aggregated_stats(v.second.first);
      AggregateSummaryStats(v.second.second, &aggregated_stats);
      aggregated_stats.PrettyPrint(prefix, v.first, verbosity, s);

      // Display per-instance stats, if there is more than one instance.
      // Legacy profile did not show per-instance stats for averaged profile.
      // Per-instance stats are too verbose for the default mode.
      if (verbosity >= Verbosity::EXTENDED && v.second.second.size() > 1) {
        for (int idx = 0; idx < v.second.second.size(); ++idx) {
          if (v.second.second[idx] == nullptr) continue;
          const string& per_instance_prefix = Substitute("$0[$1]", prefix, idx);
          v.second.second[idx]->PrettyPrint(per_instance_prefix, v.first, verbosity, s);
        }
      }
    }
  }
}

void AggregatedRuntimeProfile::ToThriftSubclass(
    vector<pair<const string&, const Counter*>>& counter_map_entries,
    TRuntimeProfileNode* out_node) const {
  out_node->__isset.aggregated = true;
  out_node->aggregated.__set_num_instances(num_input_profiles_);
  if (!input_profile_names_.empty()) {
    out_node->aggregated.__isset.input_profiles = true;
    out_node->aggregated.input_profiles = input_profile_names_;
  }

  // Populate both the aggregated counters, which contain the full information from
  // the counters, and the regular counters, which can be understood by older
  // readers.
  out_node->counters.reserve(counter_map_entries.size());
  out_node->aggregated.__isset.counters = true;
  out_node->aggregated.counters.reserve(counter_map_entries.size());
  for (const auto& entry : counter_map_entries) {
    out_node->counters.emplace_back();
    TCounter& counter = out_node->counters.back();
    counter.name = entry.first;
    counter.value = entry.second->value();
    counter.unit = entry.second->unit();

    const AveragedCounter* avg_counter =
        dynamic_cast<const AveragedCounter*>(entry.second);
    DCHECK(avg_counter != nullptr) << entry.first;
    out_node->aggregated.counters.emplace_back();
    avg_counter->ToThrift(entry.first, &out_node->aggregated.counters.back());
  }

  out_node->aggregated.__isset.time_series_counters = true;
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    out_node->aggregated.time_series_counters.reserve(time_series_counter_map_.size());
    for (const auto& entry : time_series_counter_map_) {
      out_node->aggregated.time_series_counters.push_back(entry.second);
      DCHECK_EQ(out_node->aggregated.time_series_counters.back().values.size(),
                out_node->aggregated.time_series_counters.back().period_ms.size());
      DCHECK_EQ(out_node->aggregated.time_series_counters.back().values.size(),
                out_node->aggregated.time_series_counters.back().start_index.size());
    }
  }

  out_node->aggregated.__isset.info_strings = true;
  {
    lock_guard<SpinLock> l(agg_info_strings_lock_);
    for (const auto& entry : agg_info_strings_) {
      out_node->aggregated.info_strings[entry.first] =
          GroupDistinctInfoStrings(entry.second);
    }
  }

  out_node->aggregated.__isset.event_sequences = true;
  {
    lock_guard<SpinLock> l(event_sequence_lock_);
    for (const auto& entry : event_sequence_map_) {
      const AggEventSequence& seq = entry.second;
      out_node->aggregated.event_sequences.emplace_back();
      TAggEventSequence& tseq = out_node->aggregated.event_sequences.back();
      tseq.name = entry.first;

      tseq.label_dict.resize(seq.labels.size());
      for (const auto& lab_entry : seq.labels) {
        tseq.label_dict[lab_entry.second] = lab_entry.first;
      }
      DCHECK_EQ(seq.label_idxs.size(), seq.timestamps.size());
      tseq.label_idxs.resize(seq.label_idxs.size());
      tseq.timestamps.resize(seq.timestamps.size());
      for (int idx = 0; idx < seq.label_idxs.size(); ++idx) {
        DCHECK_EQ(seq.label_idxs[idx].size(), seq.timestamps[idx].size());
        for (int j = 0; j < seq.label_idxs[idx].size(); ++j) {
          tseq.label_idxs[idx].push_back(seq.label_idxs[idx][j]);
          tseq.timestamps[idx].push_back(seq.timestamps[idx][j]);
        }
      }
    }
  }

  out_node->__isset.summary_stats_counters = true;
  {
    lock_guard<SpinLock> l(summary_stats_map_lock_);
    out_node->summary_stats_counters.resize(summary_stats_map_.size());
    out_node->aggregated.__isset.summary_stats_counters = true;
    out_node->aggregated.summary_stats_counters.resize(summary_stats_map_.size());
    int counter_idx = 0;
    for (const auto& entry : summary_stats_map_) {
      DCHECK_GT(entry.second.second.size(), 0)
          << "no counters can be created w/o instances";
      DCHECK_EQ(num_input_profiles_, entry.second.second.size());
      SummaryStatsCounter::ToThrift(entry.first, entry.second.first, entry.second.second,
          &out_node->aggregated.summary_stats_counters[counter_idx]);
      // Compute summarized stats to include at the top level.
      SummaryStatsCounter aggregated_stats(entry.second.first);
      AggregateSummaryStats(entry.second.second, &aggregated_stats);
      aggregated_stats.ToThrift(
          &out_node->summary_stats_counters[counter_idx++], entry.first);
    }
  }
}

void AggregatedRuntimeProfile::ToJsonSubclass(
    Verbosity verbosity, Value* parent, Document* d) const {
  Document::AllocatorType& allocator = d->GetAllocator();
  // We do not include all of the per-instance values in the JSON representation. We do
  // not need to be able to round-trip RuntimeProfile to/from the JSON representation so
  // we can afford to discard some less-valuable information.
  //
  // The following information is omitted deliberately:
  // * Time series counters from time_series_counter_map_ are not included - they would
  //    simply be too large for queries with 100s or 1000s of instances.
  // * Event sequences from event_sequence_map_ are not included for the same reason.
  // * Per-instance summary stat values are omitted (although we include the aggregates).
  // * Per-instance values for regular counters.

  // SummaryStatsCounter
  // Only include the aggregated summary stats, not the per-instance values to avoid
  // blowing up profile size.
  {
    lock_guard<SpinLock> l(summary_stats_map_lock_);
    if (!summary_stats_map_.empty()) {
      Value summary_stats_counters_json(kArrayType);
      for (const auto& v : summary_stats_map_) {
        SummaryStatsCounter aggregated_stats(v.second.first);
        AggregateSummaryStats(v.second.second, &aggregated_stats);
        Value summary_stats_counter(kObjectType);
        Value summary_name_json(v.first.c_str(), v.first.size(), allocator);
        aggregated_stats.ToJson(verbosity, *d, &summary_stats_counter);
        summary_stats_counter.AddMember("counter_name", summary_name_json, allocator);
        summary_stats_counters_json.PushBack(summary_stats_counter, allocator);
      }
      parent->AddMember("summary_stats_counters", summary_stats_counters_json, allocator);
    }
  }
}

map<string, vector<int32_t>> AggregatedRuntimeProfile::GroupDistinctInfoStrings(
    const vector<string>& info_string_values) const {
  map<string, vector<int32_t>> distinct_vals;
  DCHECK_EQ(info_string_values.size(), num_input_profiles_);
  for (int idx = 0; idx < num_input_profiles_; ++idx) {
    if (!info_string_values[idx].empty()) {
      distinct_vals[info_string_values[idx]].push_back(idx);
    }
  }
  return distinct_vals;
}

void AggregatedRuntimeProfile::AggregateSummaryStats(
    const vector<SummaryStatsCounter*> counters, SummaryStatsCounter* result) {
  for (SummaryStatsCounter* counter : counters) {
    if (counter != nullptr) result->Merge(*counter);
  }
}
}
