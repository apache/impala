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

#include "common/object-pool.h"
#include "util/compress.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/thrift-util.h"
#include "util/url-coding.h"
#include "util/container-util.h"

#include <iomanip>
#include <iostream>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread.hpp>
#include <boost/foreach.hpp>

using namespace boost;
using namespace std;

namespace impala {

// Period to update rate counters and sampling counters in ms.
DEFINE_int32(periodic_counter_update_period_ms, 500, "Period to update rate counters and"
    " sampling counters in ms");

// Thread counters name
static const string THREAD_TOTAL_TIME = "TotalWallClockTime";
static const string THREAD_USER_TIME = "UserTime";
static const string THREAD_SYS_TIME = "SysTime";
static const string THREAD_VOLUNTARY_CONTEXT_SWITCHES = "VoluntaryContextSwitches";
static const string THREAD_INVOLUNTARY_CONTEXT_SWITCHES = "InvoluntaryContextSwitches";

// The root counter name for all top level counters.
static const string ROOT_COUNTER = "";

RuntimeProfile::PeriodicCounterUpdateState RuntimeProfile::periodic_counter_update_state_;

RuntimeProfile::RuntimeProfile(ObjectPool* pool, const string& name) :
  pool_(pool),
  own_pool_(false),
  name_(name),
  metadata_(-1),
  counter_total_time_(TCounterType::TIME_NS),
  local_time_percent_(0) {
  counter_map_["TotalTime"] = &counter_total_time_;
}

RuntimeProfile::~RuntimeProfile() {
  map<string, Counter*>::const_iterator iter;
  for (iter = counter_map_.begin(); iter != counter_map_.end(); ++iter) {
    StopRateCounterUpdates(iter->second);
    StopSamplingCounterUpdates(iter->second);
  }

  set<vector<Counter*>* >::const_iterator buckets_iter;
  for (buckets_iter = bucketing_counters_.begin();
      buckets_iter != bucketing_counters_.end(); ++buckets_iter) {
    // This is just a clean up. No need to perform conversion. Also, the underlying
    // counters might be gone already.
    StopBucketingCountersUpdates(*buckets_iter, false);
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
      pool->Add(new Counter(counter.type, counter.value));
  }

  if (node.__isset.event_sequences) {
    BOOST_FOREACH(const TEventSequence& sequence, node.event_sequences) {
      profile->event_sequence_map_[sequence.name] =
          pool->Add(new EventSequence(sequence.timestamps, sequence.labels));
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

void RuntimeProfile::Merge(RuntimeProfile* other) {
  DCHECK(other != NULL);

  // Merge this level
  {
    CounterMap::iterator dst_iter;
    CounterMap::const_iterator src_iter;
    lock_guard<mutex> l(counter_map_lock_);
    lock_guard<mutex> m(other->counter_map_lock_);
    for (src_iter = other->counter_map_.begin();
         src_iter != other->counter_map_.end(); ++src_iter) {
      dst_iter = counter_map_.find(src_iter->first);
      if (dst_iter == counter_map_.end()) {
        counter_map_[src_iter->first] =
          pool_->Add(new Counter(src_iter->second->type(), src_iter->second->value()));
      } else {
        DCHECK(dst_iter->second->type() == src_iter->second->type());
        if (dst_iter->second->type() == TCounterType::DOUBLE_VALUE) {
          double new_val = dst_iter->second->double_value() +
              src_iter->second->double_value();
          dst_iter->second->Set(new_val);
        } else {
          dst_iter->second->Update(src_iter->second->value());
        }
      }
    }

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
    lock_guard<mutex> l(children_lock_);
    lock_guard<mutex> m(other->children_lock_);
    // Recursively merge children with matching names
    for (int i = 0; i < other->children_.size(); ++i) {
      RuntimeProfile* other_child = other->children_[i].first;
      ChildMap::iterator j = child_map_.find(other_child->name_);
      RuntimeProfile* child = NULL;
      if (j != child_map_.end()) {
        child = j->second;
      } else {
        child = pool_->Add(new RuntimeProfile(pool_, other_child->name_));
        child->local_time_percent_ = other_child->local_time_percent_;
        child->metadata_ = other_child->metadata_;
        bool indent_other_child = other->children_[i].second;
        child_map_[child->name_] = child;
        children_.push_back(make_pair(child, indent_other_child));
      }
      child->Merge(other_child);
    }
  }
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
    lock_guard<mutex> l(counter_map_lock_);
    // update this level
    map<string, Counter*>::iterator dst_iter;
    for (int i = 0; i < node.counters.size(); ++i) {
      const TCounter& tcounter = node.counters[i];
      CounterMap::iterator j = counter_map_.find(tcounter.name);
      if (j == counter_map_.end()) {
        counter_map_[tcounter.name] =
          pool_->Add(new Counter(tcounter.type, tcounter.value));
      } else {
        if (j->second->type() != tcounter.type) {
          LOG(ERROR) << "Cannot update counters with the same name (" <<
              j->first << ") but different types.";
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
    lock_guard<mutex> l(info_strings_lock_);
    const InfoStrings& info_strings = node.info_strings;
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

  ++*idx;
  {
    lock_guard<mutex> l(children_lock_);
    // update children with matching names; create new ones if they don't match
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
    lock_guard<mutex> l(counter_map_lock_);
    for (iter = counter_map_.begin(); iter != counter_map_.end(); ++iter) {
      if (iter->second->type() == TCounterType::DOUBLE_VALUE) {
        iter->second->Set(iter->second->double_value() / n);
      } else {
        iter->second->value_ /= n;
      }
    }
  }
  {
    lock_guard<mutex> l(children_lock_);
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

  // Add all the total times in all the children
  int64_t total_child_time = 0;
  lock_guard<mutex> l(children_lock_);
  for (int i = 0; i < children_.size(); ++i) {
    total_child_time += children_[i].first->total_time_counter()->value();
  }

  int64_t local_time = total_time_counter()->value() - total_child_time;
  // Counters have some margin, set to 0 if it was negative.
  local_time = ::max(0L, local_time);
  local_time_percent_ = static_cast<double>(local_time) / total;
  local_time_percent_ = ::min(1.0, local_time_percent_) * 100;

  // Recurse on children
  for (int i = 0; i < children_.size(); ++i) {
    children_[i].first->ComputeTimeInProfile(total);
  }
}

void RuntimeProfile::AddChild(RuntimeProfile* child, bool indent, RuntimeProfile* loc) {
  DCHECK(child != NULL);
  lock_guard<mutex> l(children_lock_);
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
  lock_guard<mutex> l(children_lock_);
  for (ChildMap::iterator i = child_map_.begin(); i != child_map_.end(); ++i) {
    children->push_back(i->second);
  }
}

void RuntimeProfile::GetAllChildren(vector<RuntimeProfile*>* children) {
  lock_guard<mutex> l(children_lock_);
  for (ChildMap::iterator i = child_map_.begin(); i != child_map_.end(); ++i) {
    children->push_back(i->second);
    i->second->GetAllChildren(children);
  }
}

void RuntimeProfile::AddInfoString(const string& key, const string& value) {
  lock_guard<mutex> l(info_strings_lock_);
  InfoStrings::iterator it = info_strings_.find(key);
  if (it == info_strings_.end()) {
    info_strings_.insert(make_pair(key, value));
    info_strings_display_order_.push_back(key);
  } else {
    it->second = value;
  }
}

const string* RuntimeProfile::GetInfoString(const string& key) {
  lock_guard<mutex> l(info_strings_lock_);
  InfoStrings::const_iterator it = info_strings_.find(key);
  if (it == info_strings_.end()) return NULL;
  return &it->second;
}

RuntimeProfile::Counter* RuntimeProfile::AddCounter(
    const string& name, TCounterType::type type, const string& parent_counter_name) {
  lock_guard<mutex> l(counter_map_lock_);
  if (counter_map_.find(name) != counter_map_.end()) {
    // TODO: should we make sure that we don't return existing derived counters?
    return counter_map_[name];
  }
  DCHECK(parent_counter_name == ROOT_COUNTER ||
      counter_map_.find(parent_counter_name) != counter_map_.end());
  Counter* counter = pool_->Add(new Counter(type));
  counter_map_[name] = counter;
  set<string>* child_counters =
      FindOrInsert(&child_counter_map_, parent_counter_name, set<string>());
  child_counters->insert(name);
  return counter;
}

RuntimeProfile::DerivedCounter* RuntimeProfile::AddDerivedCounter(
    const string& name, TCounterType::type type,
    const DerivedCounterFunction& counter_fn, const string& parent_counter_name) {
  lock_guard<mutex> l(counter_map_lock_);
  if (counter_map_.find(name) != counter_map_.end()) return NULL;
  DerivedCounter* counter = pool_->Add(new DerivedCounter(type, counter_fn));
  counter_map_[name] = counter;
  set<string>* child_counters =
      FindOrInsert(&child_counter_map_, parent_counter_name, set<string>());
  child_counters->insert(name);
  return counter;
}

RuntimeProfile::ThreadCounters* RuntimeProfile::AddThreadCounters(
    const string& prefix) {
  ThreadCounters* counter = pool_->Add(new ThreadCounters());
  counter->total_time_ = AddCounter(prefix + THREAD_TOTAL_TIME, TCounterType::TIME_NS);
  counter->user_time_ = AddCounter(prefix + THREAD_USER_TIME, TCounterType::TIME_NS,
      prefix + THREAD_TOTAL_TIME);
  counter->sys_time_ = AddCounter(prefix + THREAD_SYS_TIME, TCounterType::TIME_NS,
      prefix + THREAD_TOTAL_TIME);
  counter->voluntary_context_switches_ =
      AddCounter(prefix + THREAD_VOLUNTARY_CONTEXT_SWITCHES, TCounterType::UNIT);
  counter->involuntary_context_switches_ =
      AddCounter(prefix + THREAD_INVOLUNTARY_CONTEXT_SWITCHES, TCounterType::UNIT);
  return counter;
}

RuntimeProfile::Counter* RuntimeProfile::GetCounter(const string& name) {
  lock_guard<mutex> l(counter_map_lock_);
  if (counter_map_.find(name) != counter_map_.end()) {
    return counter_map_[name];
  }
  return NULL;
}

void RuntimeProfile::GetCounters(const string& name, vector<Counter*>* counters) {
  Counter* c = GetCounter(name);
  if (c != NULL) counters->push_back(c);

  lock_guard<mutex> l(children_lock_);
  for (int i = 0; i < children_.size(); ++i) {
    children_[i].first->GetCounters(name, counters);
  }
}

RuntimeProfile::EventSequence* RuntimeProfile::GetEventSequence(const string& name) const
{
  lock_guard<mutex> l(event_sequence_lock_);
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

  // create copy of counter_map_ and child_counter_map_ so we don't need to hold lock
  // while we call value() on the counters (some of those might be DerivedCounters)
  CounterMap counter_map;
  ChildCounterMap child_counter_map;
  {
    lock_guard<mutex> l(counter_map_lock_);
    counter_map = counter_map_;
    child_counter_map = child_counter_map_;
  }

  map<string, Counter*>::const_iterator total_time = counter_map.find("TotalTime");
  DCHECK(total_time != counter_map.end());

  stream.flags(ios::fixed);
  stream << prefix << name_ << ":";
  if (total_time->second->value() != 0) {
    stream << "(Active: "
           << PrettyPrinter::Print(total_time->second->value(),
               total_time->second->type())
           << ", % non-child: "
           << setprecision(2) << local_time_percent_
           << "%)";
  }
  stream << endl;

  {
    lock_guard<mutex> l(info_strings_lock_);
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
    lock_guard<mutex> l(event_sequence_lock_);
    BOOST_FOREACH(
        const EventSequenceMap::value_type& event_sequence, event_sequence_map_) {
      stream << prefix << "  " << event_sequence.first << ": "
             << PrettyPrinter::Print(
                 event_sequence.second->ElapsedTime(), TCounterType::TIME_NS)
             << endl;

      int64_t last = 0L;
      BOOST_FOREACH(const EventSequence::Event& event, event_sequence.second->events()) {
        stream << prefix << "     - " << event.first << ": "
               << PrettyPrinter::Print(
                   event.second, TCounterType::TIME_NS) << " ("
               << PrettyPrinter::Print(
                   event.second - last, TCounterType::TIME_NS) << ")"
               << endl;
        last = event.second;
      }
    }
  }

  RuntimeProfile::PrintChildCounters(
      prefix, ROOT_COUNTER, counter_map, child_counter_map, s);

  // create copy of children_ so we don't need to hold lock while we call
  // PrettyPrint() on the children
  ChildVector children;
  {
    lock_guard<mutex> l(children_lock_);
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
  GzipCompressor compressor(GzipCompressor::ZLIB);
  vector<uint8_t> compressed_buffer;
  compressed_buffer.resize(compressor.MaxOutputLen(serialized_buffer.size()));
  int result_len = compressed_buffer.size();
  uint8_t* compressed_buffer_ptr = &compressed_buffer[0];
  compressor.ProcessBlock(true, serialized_buffer.size(), &serialized_buffer[0],
      &result_len, &compressed_buffer_ptr);
  compressed_buffer.resize(result_len);

  Base64Encode(compressed_buffer, out);
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
    lock_guard<mutex> l(counter_map_lock_);
    counter_map = counter_map_;
    node.child_counters_map = child_counter_map_;
  }
  for (map<string, Counter*>::const_iterator iter = counter_map.begin();
       iter != counter_map.end(); ++iter) {
    TCounter counter;
    counter.name = iter->first;
    counter.value = iter->second->value();
    counter.type = iter->second->type();
    node.counters.push_back(counter);
  }

  {
    lock_guard<mutex> l(info_strings_lock_);
    node.info_strings = info_strings_;
    node.info_strings_display_order = info_strings_display_order_;
  }

  {
    lock_guard<mutex> l(event_sequence_lock_);
    if (event_sequence_map_.size() != 0) {
      node.__set_event_sequences(vector<TEventSequence>());
      BOOST_FOREACH(const EventSequenceMap::value_type& val, event_sequence_map_) {
        TEventSequence seq;
        seq.name = val.first;
        BOOST_FOREACH(const EventSequence::Event& ev, val.second->events()) {
          seq.labels.push_back(ev.first);
          seq.timestamps.push_back(ev.second);
        }
        node.event_sequences.push_back(seq);
      }
    }
  }


  ChildVector children;
  {
    lock_guard<mutex> l(children_lock_);
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
  DCHECK(total_counter->type() == TCounterType::BYTES ||
         total_counter->type() == TCounterType::UNIT);
  DCHECK(timer->type() == TCounterType::TIME_NS);

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
  TCounterType::type dst_type;
  switch (src_counter->type()) {
    case TCounterType::BYTES:
      dst_type = TCounterType::BYTES_PER_SECOND;
      break;
    case TCounterType::UNIT:
      dst_type = TCounterType::UNIT_PER_SECOND;
      break;
    default:
      DCHECK(false) << "Unsupported src counter type: " << src_counter->type();
      return NULL;
  }
  Counter* dst_counter = AddCounter(name, dst_type);
  RegisterPeriodicCounter(src_counter, NULL, dst_counter, RATE_COUNTER);
  return dst_counter;
}

RuntimeProfile::Counter* RuntimeProfile::AddRateCounter(
    const string& name, SampleFn fn, TCounterType::type dst_type) {
  Counter* dst_counter = AddCounter(name, dst_type);
  RegisterPeriodicCounter(NULL, fn, dst_counter, RATE_COUNTER);
  return dst_counter;
}

RuntimeProfile::Counter* RuntimeProfile::AddSamplingCounter(
    const string& name, Counter* src_counter) {
  DCHECK(src_counter->type() == TCounterType::UNIT);
  Counter* dst_counter = AddCounter(name, TCounterType::DOUBLE_VALUE);
  RegisterPeriodicCounter(src_counter, NULL, dst_counter, SAMPLING_COUNTER);
  return dst_counter;
}

RuntimeProfile::Counter* RuntimeProfile::AddSamplingCounter(
    const string& name, SampleFn sample_fn) {
  Counter* dst_counter = AddCounter(name, TCounterType::DOUBLE_VALUE);
  RegisterPeriodicCounter(NULL, sample_fn, dst_counter, SAMPLING_COUNTER);
  return dst_counter;
}

void RuntimeProfile::RegisterBucketingCounters(Counter* src_counter,
    vector<Counter*>* buckets) {
  {
    lock_guard<mutex> l(counter_map_lock_);
    bucketing_counters_.insert(buckets);
  }

  lock_guard<mutex> l(periodic_counter_update_state_.lock);
  if (periodic_counter_update_state_.update_thread.get() == NULL) {
    periodic_counter_update_state_.update_thread.reset(
        new Thread("runtime-profile", "counter-update-loop", &PeriodicCounterUpdateLoop));
  }
  BucketCountersInfo info;
  info.src_counter = src_counter;
  info.num_sampled = 0;
  periodic_counter_update_state_.bucketing_counters[buckets] = info;
}

RuntimeProfile::EventSequence* RuntimeProfile::AddEventSequence(const string& name) {
  lock_guard<mutex> l(event_sequence_lock_);
  EventSequenceMap::iterator timer_it = event_sequence_map_.find(name);
  if (timer_it != event_sequence_map_.end()) return timer_it->second;

  EventSequence* timer = pool_->Add(new EventSequence());
  event_sequence_map_[name] = timer;
  return timer;
}

void RuntimeProfile::RegisterPeriodicCounter(Counter* src_counter, SampleFn sample_fn,
    Counter* dst_counter, PeriodicCounterType type) {
  DCHECK(src_counter == NULL || sample_fn == NULL);

  lock_guard<mutex> l(periodic_counter_update_state_.lock);
  if (periodic_counter_update_state_.update_thread.get() == NULL) {
    periodic_counter_update_state_.update_thread.reset(
        new Thread("runtime-profile", "counter-update-loop", &PeriodicCounterUpdateLoop));
  }
  switch (type) {
    case RATE_COUNTER: {
      RateCounterInfo counter;
      counter.src_counter = src_counter;
      counter.sample_fn = sample_fn;
      counter.elapsed_ms = 0;
      periodic_counter_update_state_.rate_counters[dst_counter] = counter;
      break;
    }
    case SAMPLING_COUNTER: {
      SamplingCounterInfo counter;
      counter.src_counter = src_counter;
      counter.sample_fn = sample_fn;
      counter.num_sampled = 0;
      counter.total_sampled_value = 0;
      periodic_counter_update_state_.sampling_counters[dst_counter] = counter;
      break;
    }
    default:
      DCHECK(false) << "Unsupported PeriodicCounterType:" << type;
  }
}

void RuntimeProfile::StopRateCounterUpdates(Counter* rate_counter) {
  lock_guard<mutex> l(periodic_counter_update_state_.lock);
  periodic_counter_update_state_.rate_counters.erase(rate_counter);
}

void RuntimeProfile::StopSamplingCounterUpdates(Counter* sampling_counter) {
  lock_guard<mutex> l(periodic_counter_update_state_.lock);
  periodic_counter_update_state_.sampling_counters.erase(sampling_counter);
}

void RuntimeProfile::StopBucketingCountersUpdates(vector<Counter*>* buckets,
    bool convert) {
  int64_t num_sampled = 0;
  {
    lock_guard<mutex> l(periodic_counter_update_state_.lock);
    PeriodicCounterUpdateState::BucketCountersMap::const_iterator itr =
        periodic_counter_update_state_.bucketing_counters.find(buckets);
    if (itr != periodic_counter_update_state_.bucketing_counters.end()) {
      num_sampled = itr->second.num_sampled;
      periodic_counter_update_state_.bucketing_counters.erase(buckets);
    }
  }

  if (convert && num_sampled > 0) {
    BOOST_FOREACH(Counter* counter, *buckets) {
      double perc = 100 * counter->value() / (double)num_sampled;
      counter->Set(perc);
    }
  }
}

RuntimeProfile::PeriodicCounterUpdateState::PeriodicCounterUpdateState() : done_(false) {
}

RuntimeProfile::PeriodicCounterUpdateState::~PeriodicCounterUpdateState() {
  if (periodic_counter_update_state_.update_thread.get() != NULL) {
    {
      // Lock to ensure the update thread will see the update to done_
      lock_guard<mutex> l(periodic_counter_update_state_.lock);
      done_ = true;
    }
    periodic_counter_update_state_.update_thread->Join();
  }
}

void RuntimeProfile::PeriodicCounterUpdateLoop() {
  while (!periodic_counter_update_state_.done_) {
    system_time before_time = get_system_time();
    usleep(FLAGS_periodic_counter_update_period_ms * 1000);
    posix_time::time_duration elapsed = get_system_time() - before_time;
    int elapsed_ms = elapsed.total_milliseconds();

    lock_guard<mutex> l(periodic_counter_update_state_.lock);
    for (PeriodicCounterUpdateState::RateCounterMap::iterator it =
        periodic_counter_update_state_.rate_counters.begin();
        it != periodic_counter_update_state_.rate_counters.end(); ++it) {
      it->second.elapsed_ms += elapsed_ms;
      int64_t value;
      if (it->second.src_counter != NULL) {
        value = it->second.src_counter->value();
      } else {
        DCHECK(it->second.sample_fn != NULL);
        value = it->second.sample_fn();
      }
      int64_t rate = value * 1000 / (it->second.elapsed_ms);
      it->first->Set(rate);
    }

    for (PeriodicCounterUpdateState::SamplingCounterMap::iterator it =
        periodic_counter_update_state_.sampling_counters.begin();
        it != periodic_counter_update_state_.sampling_counters.end(); ++it) {
      ++it->second.num_sampled;
      int64_t value;
      if (it->second.src_counter != NULL) {
        value = it->second.src_counter->value();
      } else {
        DCHECK(it->second.sample_fn != NULL);
        value = it->second.sample_fn();
      }
      it->second.total_sampled_value += value;
      double average = static_cast<double>(it->second.total_sampled_value) /
          it->second.num_sampled;
      it->first->Set(average);
    }

    for (PeriodicCounterUpdateState::BucketCountersMap::iterator it =
        periodic_counter_update_state_.bucketing_counters.begin();
        it != periodic_counter_update_state_.bucketing_counters.end(); ++it) {
      int64_t val = it->second.src_counter->value();
      if (val >= it->first->size()) val = it->first->size() - 1;
      it->first->at(val)->Update(1);
      ++it->second.num_sampled;
    }
  }
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
      DCHECK(iter != counter_map.end());
      stream << prefix << "   - " << iter->first << ": "
             << PrettyPrinter::Print(iter->second->value(), iter->second->type())
             << endl;
      RuntimeProfile::PrintChildCounters(prefix + "  ", child_counter, counter_map,
          child_counter_map, s);
    }
  }
}

}
