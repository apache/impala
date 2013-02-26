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
#include "util/debug-util.h"
#include "util/cpu-info.h"

#include <iomanip>
#include <iostream>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread.hpp>

using namespace boost;
using namespace std;

namespace impala {
  
// Period to update rate counters in ms.  
static const int RATE_COUNTER_UPDATE_PERIOD = 500;

// Thread counters name
static const string THREAD_TOTAL_TIME = "TotalWallClockTime";
static const string THREAD_USER_TIME = "UserCpuTime";
static const string THREAD_SYS_TIME = "SysCpuTime";
static const string THREAD_VOLUNTARY_CONTEXT_SWITCH = "VoluntaryContextSwitch";
static const string THREAD_INVOLUNTARY_CONTEXT_SWITCH = "InvoluntaryContextSwitch";

RuntimeProfile::RateCounterUpdateState RuntimeProfile::rate_counters_state_;

RuntimeProfile::RuntimeProfile(ObjectPool* pool, const string& name) :
  pool_(pool),
  name_(name),
  metadata_(-1),
  counter_total_time_(TCounterType::CPU_TICKS),
  local_time_percent_(0) {
  counter_map_["TotalTime"] = &counter_total_time_;
}

RuntimeProfile::~RuntimeProfile() {
  map<string, Counter*>::const_iterator iter;
  for (iter = counter_map_.begin(); iter != counter_map_.end(); ++iter) {
    StopRateCounterUpdates(iter->second);
  }
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

  profile->info_strings_ = node.info_strings;
  
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
    map<string, Counter*>::iterator dst_iter;
    map<string, Counter*>::const_iterator src_iter;
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
        dst_iter->second->Update(src_iter->second->value());
      }
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
  }
  
  {
    lock_guard<mutex> l(info_strings_lock_);
    const InfoStrings& info_strings = node.info_strings;
    for (InfoStrings::const_iterator it = info_strings.begin(); 
        it != info_strings.end(); ++it) {
      info_strings_[it->first] = it->second;
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
      iter->second->value_ /= n;
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
  info_strings_[key] = value;
}

const string* RuntimeProfile::GetInfoString(const string& key) {
  lock_guard<mutex> l(info_strings_lock_);
  InfoStrings::const_iterator it = info_strings_.find(key);
  if (it == info_strings_.end()) return NULL;
  return &it->second;
}

RuntimeProfile::Counter* RuntimeProfile::AddCounter(
    const string& name, TCounterType::type type) {
  lock_guard<mutex> l(counter_map_lock_);
  if (counter_map_.find(name) != counter_map_.end()) {
    // TODO: should we make sure that we don't return existing derived counters?
    return counter_map_[name];
  }
  Counter* counter = pool_->Add(new Counter(type));
  counter_map_[name] = counter;
  return counter;
}

RuntimeProfile::DerivedCounter* RuntimeProfile::AddDerivedCounter(
    const std::string& name, TCounterType::type type, 
    const DerivedCounterFunction& counter_fn) {
  lock_guard<mutex> l(counter_map_lock_);
  if (counter_map_.find(name) != counter_map_.end()) return NULL;
  DerivedCounter* counter = pool_->Add(new DerivedCounter(type, counter_fn));
  counter_map_[name] = counter;
  return counter;
}

RuntimeProfile::ThreadCounters* RuntimeProfile::AddThreadCounters(
    const std::string& prefix) {
  ThreadCounters* counter = pool_->Add(new ThreadCounters());
  counter->total_time_ = AddCounter(prefix + THREAD_TOTAL_TIME, TCounterType::TIME_MS);
  counter->user_time_ = AddCounter(prefix + THREAD_USER_TIME, TCounterType::TIME_MS);
  counter->sys_time_ = AddCounter(prefix + THREAD_SYS_TIME, TCounterType::TIME_MS);
  counter->voluntary_context_switch_counter_ =
      AddCounter(prefix + THREAD_VOLUNTARY_CONTEXT_SWITCH, TCounterType::UNIT);
  counter->involuntary_context_switch_counter_ =
      AddCounter(prefix + THREAD_INVOLUNTARY_CONTEXT_SWITCH, TCounterType::UNIT);
  return counter;
}

RuntimeProfile::Counter* RuntimeProfile::GetCounter(const string& name) {
  lock_guard<mutex> l(counter_map_lock_);
  if (counter_map_.find(name) != counter_map_.end()) {
    return counter_map_[name];
  }
  return NULL;
}

void RuntimeProfile::GetCounters(
    const std::string& name, std::vector<Counter*>* counters) {
  Counter* c = GetCounter(name);
  if (c != NULL) counters->push_back(c);

  lock_guard<mutex> l(children_lock_);
  for (int i = 0; i < children_.size(); ++i) {
    children_[i].first->GetCounters(name, counters);
  }
}

// Print the profile:
//  1. Profile Name
//  2. Info Strings
//  3. Counters
//  4. Children
void RuntimeProfile::PrettyPrint(ostream* s, const string& prefix) const {
  ostream& stream = *s;

  // create copy of counter_map_ so we don't need to hold lock while we call
  // value() on the counters (some of those might be DerivedCounters)
  CounterMap counter_map;
  {
    lock_guard<mutex> l(counter_map_lock_);
    counter_map = counter_map_;
  }

  map<string, Counter*>::const_iterator total_time = counter_map.find("TotalTime");
  DCHECK(total_time != counter_map.end());

  stream.flags(ios::fixed);
  stream << prefix << name_ << ":";
  if (total_time->second->value() != 0) {
    stream << "("
           << PrettyPrinter::Print(total_time->second->value(), TCounterType::CPU_TICKS)
           << " " << setprecision(2) << local_time_percent_ 
           << "%)";
  }
  stream << endl;
  
  {
    lock_guard<mutex> l(info_strings_lock_);
    for (InfoStrings::const_iterator it = info_strings_.begin(); 
        it != info_strings_.end(); ++it) {
      stream << prefix << "  " << it->first << ": " << it->second << endl;
    }
  }

  for (map<string, Counter*>::const_iterator iter = counter_map.begin();
       iter != counter_map.end(); ++iter) {
    if (iter == total_time) continue;
    stream << prefix << "   - " << iter->first << ": "
           << PrettyPrinter::Print(iter->second->value(), iter->second->type())
           << endl;
  }

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

void RuntimeProfile::ToThrift(TRuntimeProfileTree* tree) {
  tree->nodes.clear();
  ToThrift(&tree->nodes);
}

void RuntimeProfile::ToThrift(vector<TRuntimeProfileNode>* nodes) {
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
  DCHECK_EQ(timer->type(), TCounterType::CPU_TICKS);
  if (timer->value() == 0) return 0;
  double secs = static_cast<double>(timer->value())
      / static_cast<double>(CpuInfo::cycles_per_ms()) / 1000.0;
  return total_counter->value() / secs;
}

int64_t RuntimeProfile::CounterSum(const std::vector<Counter*>* counters) {
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
  RegisterRateCounter(src_counter, dst_counter);
  return dst_counter;
}
  
void RuntimeProfile::RegisterRateCounter(Counter* src_counter, Counter* dst_counter) {
  RateCounterInfo counter;
  counter.src_counter = src_counter;
  counter.elapsed_ms = 0;
  
  lock_guard<mutex> l(rate_counters_state_.lock);
  if (rate_counters_state_.update_thread.get() == NULL) {
    rate_counters_state_.update_thread.reset(
        new thread(&RuntimeProfile::RateCounterUpdateLoop));
  }
  rate_counters_state_.counters[dst_counter] = counter;
}

void RuntimeProfile::StopRateCounterUpdates(Counter* rate_counter) {
  lock_guard<mutex> l(rate_counters_state_.lock);
  rate_counters_state_.counters.erase(rate_counter);
}

RuntimeProfile::RateCounterUpdateState::RateCounterUpdateState() : done_(false) {
}

RuntimeProfile::RateCounterUpdateState::~RateCounterUpdateState() {
  if (rate_counters_state_.update_thread.get() != NULL) {
    {
      // Lock to ensure the update thread will see the update to done_
      lock_guard<mutex> l(rate_counters_state_.lock);
      done_ = true;
    }
    rate_counters_state_.update_thread->join();
  }
}

void RuntimeProfile::RateCounterUpdateLoop() {
  while (!rate_counters_state_.done_) {
    system_time before_time = get_system_time();
    usleep(RATE_COUNTER_UPDATE_PERIOD * 1000);
    posix_time::time_duration elapsed = get_system_time() - before_time;
    int elapsed_ms = elapsed.total_milliseconds();

    lock_guard<mutex> l(rate_counters_state_.lock);
    for (RateCounterUpdateState::RateCounterMap::iterator it = 
        rate_counters_state_.counters.begin(); 
        it != rate_counters_state_.counters.end(); ++it) {
      it->second.elapsed_ms += elapsed_ms;
      int64_t value = it->second.src_counter->value();
      int64_t rate = value * 1000 / (it->second.elapsed_ms);
      it->first->Set(rate);
    }
  }
}

}
