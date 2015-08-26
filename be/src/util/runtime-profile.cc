// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "util/runtime-profile.h"

#include <iomanip>
#include <iostream>

#include <boost/bind.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread.hpp>

#include "common/object-pool.h"
#include "rpc/thrift-util.h"
#include "util/compress.h"
#include "util/container-util.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/periodic-counter-updater.h"
#include "util/redactor.h"
#include "util/url-coding.h"

#include "common/names.h"

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

RuntimeProfile::RuntimeProfile(ObjectPool* pool, const string& name,
    bool is_averaged_profile)
  : pool_(pool),
    own_pool_(false),
    name_(name),
    metadata_(-1),
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
  map<string, Counter*>::const_iterator iter;
  for (iter = counter_map_.begin(); iter != counter_map_.end(); ++iter) {
    PeriodicCounterUpdater::StopRateCounter(iter->second);
    PeriodicCounterUpdater::StopSamplingCounter(iter->second);
  }

  set<vector<Counter*>* >::const_iterator buckets_iter;
  for (buckets_iter = bucketing_counters_.begin();
      buckets_iter != bucketing_counters_.end(); ++buckets_iter) {
    // This is just a clean up. No need to perform conversion. Also, the underlying
    // counters might be gone already.
    PeriodicCounterUpdater::StopBucketingCounters(*buckets_iter, false);
  }

  TimeSeriesCounterMap::const_iterator time_series_it;
  for (time_series_it = time_series_counter_map_.begin();
      time_series_it != time_series_counter_map_.end(); ++time_series_it) {
    PeriodicCounterUpdater::StopTimeSeriesCounter(time_series_it->second);
  }

  if (own_pool_) delete pool_;
}

RuntimeProfile* RuntimeProfile::CreateFromThrift(ObjectPool* pool,
    const TRuntimeProfileTree& profiles) {
  if (profiles.nodes.size() == 0) return NULL;
  int idx = 0;
  return RuntimeProfile::CreateFromThrift(pool, profiles.nodes, &idx);
}

RuntimeProfile* RuntimeProfile::CreateFromThrift(ObjectPool* pool,
    const vector<TRuntimeProfileNode>& nodes, int* idx) {
  DCHECK_LT(*idx, nodes.size());

  const TRuntimeProfileNode& node = nodes[*idx];
  RuntimeProfile* profile = pool->Add(new RuntimeProfile(pool, node.name));
  profile->metadata_ = node.metadata;
  for (int i = 0; i < node.counters.size(); ++i) {
    const TCounter& counter = node.counters[i];
    profile->counter_map_[counter.name] =
      pool->Add(new Counter(counter.unit, counter.value));
  }

  if (node.__isset.event_sequences) {
    BOOST_FOREACH(const TEventSequence& sequence, node.event_sequences) {
      profile->event_sequence_map_[sequence.name] =
          pool->Add(new EventSequence(sequence.timestamps, sequence.labels));
    }
  }

  if (node.__isset.time_series_counters) {
    BOOST_FOREACH(const TTimeSeriesCounter& val, node.time_series_counters) {
      profile->time_series_counter_map_[val.name] =
          pool->Add(new TimeSeriesCounter(val.name, val.unit, val.period_ms, val.values));
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
    // Recursively merge children with matching names
    for (int i = 0; i < other->children_.size(); ++i) {
      RuntimeProfile* other_child = other->children_[i].first;
      ChildMap::iterator j = child_map_.find(other_child->name_);
      RuntimeProfile* child = NULL;
      if (j != child_map_.end()) {
        child = j->second;
      } else {
        child = pool_->Add(new RuntimeProfile(pool_, other_child->name_, true));
        child->metadata_ = other_child->metadata_;
        bool indent_other_child = other->children_[i].second;
        child_map_[child->name_] = child;
        children_.push_back(make_pair(child, indent_other_child));
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
    BOOST_FOREACH(const string& key, node.info_strings_display_order) {
      // Look for existing info strings and update in place. If there
      // are new strings, add them to the end of the display order.
      // TODO: Is nodes.info_strings always a superset of
      // info_strings_? If so, can just copy the display order.
      InfoStrings::const_iterator it = info_strings.find(key);
      DCHECK(it != info_strings.end());
      InfoStrings::iterator existing = info_strings_.find(key);
      if (existing == info_strings_.end()) {
        info_strings_.insert(make_pair(key, it->second));
        info_strings_display_order_.push_back(key);
      } else {
        info_strings_[key] = it->second;
      }
    }
  }

  {
    lock_guard<SpinLock> l(time_series_counter_map_lock_);
    for (int i = 0; i < node.time_series_counters.size(); ++i) {
      const TTimeSeriesCounter& c = node.time_series_counters[i];
      TimeSeriesCounterMap::iterator it = time_series_counter_map_.find(c.name);
      if (it == time_series_counter_map_.end()) {
        time_series_counter_map_[c.name] =
            pool_->Add(new TimeSeriesCounter(c.name, c.unit, c.period_ms, c.values));
        it = time_series_counter_map_.find(c.name);
      } else {
        it->second->samples_.SetSamples(c.period_ms, c.values);
      }
    }
  }

  ++*idx;
  {
    lock_guard<SpinLock> l(children_lock_);
    // Update children with matching names; create new ones if they don't match.
    for (int i = 0; i < node.num_children; ++i) {
      const TRuntimeProfileNode& tchild = nodes[*idx];
      ChildMap::iterator j = child_map_.find(tchild.name);
      RuntimeProfile* child = NULL;
      if (j != child_map_.end()) {
        child = j->second;
      } else {
        child = pool_->Add(new RuntimeProfile(pool_, tchild.name));
        child->metadata_ = tchild.metadata;
        child_map_[tchild.name] = child;
        children_.push_back(make_pair(child, tchild.indent));
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
        iter->second->value_ = iter->second->value() / n;
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
  if (total == 0) return;

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
    // Add all the total times of all the children.
    int64_t total_child_time = 0;
    lock_guard<SpinLock> l(children_lock_);
    for (int i = 0; i < children_.size(); ++i) {
      total_child_time += children_[i].first->total_time_counter()->value();
    }
    local_time_ns_ = total_time_counter()->value() - total_child_time;
    if (!is_averaged_profile_) {
      local_time_ns_ -= inactive_timer()->value();
    }
  }
  // Counters have some margin, set to 0 if it was negative.
  local_time_ns_ = ::max(0L, local_time_ns_);
  local_time_percent_ =
      static_cast<double>(local_time_ns_) / total_time_counter()->value();
  local_time_percent_ = ::min(1.0, local_time_percent_) * 100;

  // Recurse on children
  for (int i = 0; i < children_.size(); ++i) {
    children_[i].first->ComputeTimeInProfile(total);
  }
}

void RuntimeProfile::AddChild(RuntimeProfile* child, bool indent, RuntimeProfile* loc) {
  DCHECK(child != NULL);
  lock_guard<SpinLock> l(children_lock_);
  if (child_map_.count(child->name_) > 0) {
    // This child has already been added, so do nothing.
    // Otherwise, the map and vector will be out of sync.
    return;
  }
  child_map_[child->name_] = child;
  if (loc == NULL) {
    children_.push_back(make_pair(child, indent));
  } else {
    for (ChildVector::iterator it = children_.begin(); it != children_.end(); ++it) {
      if (it->first == loc) {
        children_.insert(++it, make_pair(child, indent));
        return;
      }
    }
    DCHECK(false) << "Invalid loc";
  }
}

void RuntimeProfile::GetChildren(vector<RuntimeProfile*>* children) {
  children->clear();
  lock_guard<SpinLock> l(children_lock_);
  for (ChildMap::iterator i = child_map_.begin(); i != child_map_.end(); ++i) {
    children->push_back(i->second);
  }
}

void RuntimeProfile::GetAllChildren(vector<RuntimeProfile*>* children) {
  lock_guard<SpinLock> l(children_lock_);
  for (ChildMap::iterator i = child_map_.begin(); i != child_map_.end(); ++i) {
    children->push_back(i->second);
    i->second->GetAllChildren(children);
  }
}

void RuntimeProfile::AddInfoString(const string& key, const string& value) {
  // Values may contain sensitive data, such as a query.
  const string& info = RedactCopy(value);
  lock_guard<SpinLock> l(info_strings_lock_);
  InfoStrings::iterator it = info_strings_.find(key);
  if (it == info_strings_.end()) {
    info_strings_.insert(make_pair(key, info));
    info_strings_display_order_.push_back(key);
  } else {
    it->second = info;
  }
}

const string* RuntimeProfile::GetInfoString(const string& key) const {
  lock_guard<SpinLock> l(info_strings_lock_);
  InfoStrings::const_iterator it = info_strings_.find(key);
  if (it == info_strings_.end()) return NULL;
  return &it->second;
}

#define ADD_COUNTER_IMPL(NAME, T) \
  RuntimeProfile::T* RuntimeProfile::NAME(\
      const string& name, TUnit::type unit, const string& parent_counter_name) {\
    DCHECK_EQ(is_averaged_profile_, false);\
    lock_guard<SpinLock> l(counter_map_lock_);\
    if (counter_map_.find(name) != counter_map_.end()) {\
      return reinterpret_cast<T*>(counter_map_[name]);\
    }\
    DCHECK(parent_counter_name == ROOT_COUNTER ||\
           counter_map_.find(parent_counter_name) != counter_map_.end()); \
    T* counter = pool_->Add(new T(unit));\
    counter_map_[name] = counter;\
    set<string>* child_counters =\
        FindOrInsert(&child_counter_map_, parent_counter_name, set<string>());\
    child_counters->insert(name);\
    return counter;\
  }

ADD_COUNTER_IMPL(AddCounter, Counter);
ADD_COUNTER_IMPL(AddHighWaterMarkCounter, HighWaterMarkCounter);

RuntimeProfile::DerivedCounter* RuntimeProfile::AddDerivedCounter(
    const string& name, TUnit::type unit,
    const DerivedCounterFunction& counter_fn, const string& parent_counter_name) {
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

void RuntimeProfile::AddLocalTimeCounter(const DerivedCounterFunction& counter_fn) {
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
    BOOST_FOREACH(const string& key, info_strings_display_order_) {
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
    BOOST_FOREACH(
        const EventSequenceMap::value_type& event_sequence, event_sequence_map_) {
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
      BOOST_FOREACH(const EventSequence::Event& event, events) {
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
    // <Name> (<period>): <val1>, <val2>, <etc>
    SpinLock* lock;
    int num, period;
    lock_guard<SpinLock> l(time_series_counter_map_lock_);
    BOOST_FOREACH(const TimeSeriesCounterMap::value_type& v, time_series_counter_map_) {
      const int64_t* samples = v.second->samples_.GetSamples(&num, &period, &lock);
      if (num > 0) {
        stream << prefix << "  " << v.first << "("
               << PrettyPrinter::Print(period * 1000000L, TUnit::TIME_NS)
               << "): ";
        for (int i = 0; i < num; ++i) {
          stream << PrettyPrinter::Print(samples[i], v.second->unit_);
          if (i != num - 1) stream << ", ";
        }
        stream << endl;
      }
      lock->unlock();
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

string RuntimeProfile::SerializeToArchiveString() const {
  stringstream ss;
  SerializeToArchiveString(&ss);
  return ss.str();
}

void RuntimeProfile::SerializeToArchiveString(stringstream* out) const {
  TRuntimeProfileTree thrift_object;
  const_cast<RuntimeProfile*>(this)->ToThrift(&thrift_object);
  ThriftSerializer serializer(true);
  vector<uint8_t> serialized_buffer;
  Status status = serializer.Serialize(&thrift_object, &serialized_buffer);
  if (!status.ok()) return;

  // Compress the serialized thrift string.  This uses string keys and is very
  // easy to compress.
  scoped_ptr<Codec> compressor;
  status = Codec::CreateCompressor(NULL, false, THdfsCompression::DEFAULT, &compressor);
  DCHECK(status.ok()) << status.GetDetail();
  if (!status.ok()) return;

  vector<uint8_t> compressed_buffer;
  compressed_buffer.resize(compressor->MaxOutputLen(serialized_buffer.size()));
  int64_t result_len = compressed_buffer.size();
  uint8_t* compressed_buffer_ptr = &compressed_buffer[0];
  compressor->ProcessBlock(true, serialized_buffer.size(), &serialized_buffer[0],
      &result_len, &compressed_buffer_ptr);
  compressed_buffer.resize(result_len);

  Base64Encode(compressed_buffer, out);
  compressor->Close();
}

void RuntimeProfile::ToThrift(TRuntimeProfileTree* tree) const {
  tree->nodes.clear();
  ToThrift(&tree->nodes);
}

void RuntimeProfile::ToThrift(vector<TRuntimeProfileNode>* nodes) const {
  nodes->reserve(nodes->size() + children_.size());

  int index = nodes->size();
  nodes->push_back(TRuntimeProfileNode());
  TRuntimeProfileNode& node = (*nodes)[index];
  node.name = name_;
  node.num_children = children_.size();
  node.metadata = metadata_;
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
      BOOST_FOREACH(const EventSequenceMap::value_type& val, event_sequence_map_) {
        TEventSequence* seq = &node.event_sequences[idx++];
        seq->name = val.first;
        val.second->GetEvents(&events);
        BOOST_FOREACH(const EventSequence::Event& ev, events) {
          seq->labels.push_back(ev.first);
          seq->timestamps.push_back(ev.second);
        }
      }
    }
  }

  {
    lock_guard<SpinLock> l(time_series_counter_map_lock_);
    if (time_series_counter_map_.size() != 0) {
      node.__set_time_series_counters(vector<TTimeSeriesCounter>());
      node.time_series_counters.resize(time_series_counter_map_.size());
      int idx = 0;
      BOOST_FOREACH(const TimeSeriesCounterMap::value_type& val,
          time_series_counter_map_) {
        val.second->ToThrift(&node.time_series_counters[idx++]);
      }
    }
  }

  ChildVector children;
  {
    lock_guard<SpinLock> l(children_lock_);
    children = children_;
  }
  for (int i = 0; i < children.size(); ++i) {
    int child_idx = nodes->size();
    children[i].first->ToThrift(nodes);
    // fix up indentation flag
    (*nodes)[child_idx].indent = children[i].second;
  }
}

int64_t RuntimeProfile::UnitsPerSecond(
    const RuntimeProfile::Counter* total_counter,
    const RuntimeProfile::Counter* timer) {
  DCHECK(total_counter->unit() == TUnit::BYTES ||
         total_counter->unit() == TUnit::UNIT);
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
  Counter* dst_counter = AddCounter(name, dst_unit);
  PeriodicCounterUpdater::RegisterPeriodicCounter(src_counter, NULL, dst_counter,
      PeriodicCounterUpdater::RATE_COUNTER);
  return dst_counter;
}

RuntimeProfile::Counter* RuntimeProfile::AddRateCounter(
    const string& name, DerivedCounterFunction fn, TUnit::type dst_unit) {
  Counter* dst_counter = AddCounter(name, dst_unit);
  PeriodicCounterUpdater::RegisterPeriodicCounter(NULL, fn, dst_counter,
      PeriodicCounterUpdater::RATE_COUNTER);
  return dst_counter;
}

RuntimeProfile::Counter* RuntimeProfile::AddSamplingCounter(
    const string& name, Counter* src_counter) {
  DCHECK(src_counter->unit() == TUnit::UNIT);
  Counter* dst_counter = AddCounter(name, TUnit::DOUBLE_VALUE);
  PeriodicCounterUpdater::RegisterPeriodicCounter(src_counter, NULL, dst_counter,
      PeriodicCounterUpdater::SAMPLING_COUNTER);
  return dst_counter;
}

RuntimeProfile::Counter* RuntimeProfile::AddSamplingCounter(
    const string& name, DerivedCounterFunction sample_fn) {
  Counter* dst_counter = AddCounter(name, TUnit::DOUBLE_VALUE);
  PeriodicCounterUpdater::RegisterPeriodicCounter(NULL, sample_fn, dst_counter,
      PeriodicCounterUpdater::SAMPLING_COUNTER);
  return dst_counter;
}

void RuntimeProfile::RegisterBucketingCounters(Counter* src_counter,
    vector<Counter*>* buckets) {
  {
    lock_guard<SpinLock> l(counter_map_lock_);
    bucketing_counters_.insert(buckets);
  }
  PeriodicCounterUpdater::RegisterBucketingCounters(src_counter, buckets);
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
    BOOST_FOREACH(const string& child_counter, child_counters) {
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

RuntimeProfile::TimeSeriesCounter* RuntimeProfile::AddTimeSeriesCounter(
    const string& name, TUnit::type unit, DerivedCounterFunction fn) {
  DCHECK(fn != NULL);
  TimeSeriesCounter* counter = NULL;
  {
    lock_guard<SpinLock> l(time_series_counter_map_lock_);
    TimeSeriesCounterMap::iterator it = time_series_counter_map_.find(name);
    if (it != time_series_counter_map_.end()) return it->second;
    counter = pool_->Add(new TimeSeriesCounter(name, unit, fn));
    time_series_counter_map_[name] = counter;
  }
  PeriodicCounterUpdater::RegisterTimeSeriesCounter(counter);
  return counter;
}

RuntimeProfile::TimeSeriesCounter* RuntimeProfile::AddTimeSeriesCounter(
    const string& name, Counter* src_counter) {
  DCHECK(src_counter != NULL);
  return AddTimeSeriesCounter(name, src_counter->unit(),
      bind(&Counter::value, src_counter));
}

void RuntimeProfile::TimeSeriesCounter::ToThrift(TTimeSeriesCounter* counter) {
  counter->name = name_;
  counter->unit = unit_;

  int num, period;
  SpinLock* lock;
  const int64_t* samples = samples_.GetSamples(&num, &period, &lock);
  counter->values.resize(num);
  memcpy(&counter->values[0], samples, num * sizeof(int64_t));
  lock->unlock();
  counter->period_ms = period;
}

string RuntimeProfile::TimeSeriesCounter::DebugString() const {
  stringstream ss;
  ss << "Counter=" << name_ << endl
     << samples_.DebugString();
  return ss.str();
}

void RuntimeProfile::EventSequence::ToThrift(TEventSequence* seq) const {
  BOOST_FOREACH(const EventSequence::Event& ev, events_) {
    seq->labels.push_back(ev.first);
    seq->timestamps.push_back(ev.second);
  }
}

}
