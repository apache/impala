// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "util/runtime-profile.h"

#include "common/object-pool.h"
#include "util/debug-util.h"
#include "util/cpu-info.h"

#include <iostream>
#include <boost/thread/locks.hpp>

using namespace boost;
using namespace std;

namespace impala {

RuntimeProfile::RuntimeProfile(ObjectPool* pool, const string& name) :
  pool_(pool),
  name_(name),
  counter_total_time_(TCounterType::CPU_TICKS) {
  counter_map_["TotalTime"] = &counter_total_time_;
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
  for (int i = 0; i < node.counters.size(); ++i) {
    const TCounter& counter = node.counters[i];
    profile->counter_map_[counter.name] =
      pool->Add(new Counter(counter.type, counter.value));
  }
  ++*idx;
  for (int i = 0; i < node.num_children; ++i) {
    profile->AddChild(RuntimeProfile::CreateFromThrift(pool, nodes, idx));
  }
  return profile;
}

void RuntimeProfile::Merge(RuntimeProfile* other) {
  // Merge this level
  map<string, Counter*>::iterator dst_iter;
  map<string, Counter*>::const_iterator src_iter;
  {
    lock_guard<mutex> l(counter_map_lock_);
    lock_guard<mutex> m(other->counter_map_lock_);
    for (src_iter = other->counter_map_.begin();
         src_iter != other->counter_map_.end(); ++src_iter) {
      dst_iter = counter_map_.find(src_iter->first);
      if (dst_iter == counter_map_.end()) {
        counter_map_[src_iter->first] =
          pool_->Add(new Counter(src_iter->second->type(), src_iter->second->value()));
      } else {
        if (dst_iter->second->type() != src_iter->second->type()) {
          LOG(ERROR) << "Cannot merge counters with the same name (" <<
           dst_iter->first << ") but different types.";
        } else {
          dst_iter->second->Update(src_iter->second->value());
        }
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
        child_map_[tchild.name] = child;
        children_.push_back(make_pair(child, tchild.indent));
      }
      child->Update(nodes, idx);
    }
  }
}

void RuntimeProfile::Divide(int n) {
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

void RuntimeProfile::AddChild(RuntimeProfile* child, bool indent) {
  DCHECK(child != NULL);
  lock_guard<mutex> l(children_lock_);
  child_map_[child->name_] = child;
  children_.push_back(make_pair(child, indent));
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

void RuntimeProfile::PrettyPrint(ostream* s, const string& prefix) {
  ostream& stream = *s;
  {
    lock_guard<mutex> l(counter_map_lock_);
    map<string, Counter*>::const_iterator total_time = counter_map_.find("TotalTime");
    DCHECK(total_time != counter_map_.end());
    stream << prefix << name_ << ":";
    if (total_time->second->value() != 0) {
      stream << "("
             << PrettyPrinter::Print(total_time->second->value(),
                                     TCounterType::CPU_TICKS)
             << ")";
     }
    stream << endl;
    for (map<string, Counter*>::const_iterator iter = counter_map_.begin();
         iter != counter_map_.end(); ++iter) {
      if (iter == total_time) continue;
      stream << prefix << "   - " << iter->first << ": "
             << PrettyPrinter::Print(iter->second->value(), iter->second->type())
             << endl;
    }
  }
  {
    lock_guard<mutex> l(children_lock_);
    for (int i = 0; i < children_.size(); ++i) {
      RuntimeProfile* profile = children_[i].first;
      bool indent = children_[i].second;
      profile->PrettyPrint(s, prefix + (indent ? "  " : ""));
    }
  }
}

void RuntimeProfile::ToThrift(TRuntimeProfileTree* tree) {
  ToThrift(&tree->nodes);
}

void RuntimeProfile::ToThrift(vector<TRuntimeProfileNode>* nodes) {
  nodes->reserve(nodes->size() + children_.size());

  int index = nodes->size();
  nodes->push_back(TRuntimeProfileNode());
  TRuntimeProfileNode& node = (*nodes)[index];
  node.name = name_;
  node.num_children = children_.size();
  node.indent = true;

  // TODO: we shouldn't need locks here, this shouldn't get called
  // when there is still registration going on (and we're probably not
  // getting updates for it, either)
  {
    lock_guard<mutex> l(counter_map_lock_);
    for (map<string, Counter*>::const_iterator iter = counter_map_.begin();
         iter != counter_map_.end(); ++iter) {
      TCounter counter;
      counter.name = iter->first;
      counter.value = iter->second->value();
      counter.type = iter->second->type();
      node.counters.push_back(counter);
    }
  }
  {
    lock_guard<mutex> l(children_lock_);
    for (int i = 0; i < children_.size(); ++i) {
      int child_idx = nodes->size();
      children_[i].first->ToThrift(nodes);
      // fix up indentation flag
      (*nodes)[child_idx].indent = children_[i].second;
    }
  }
}

int64_t RuntimeProfile::BytesPerSecond(
    const RuntimeProfile::Counter* total_bytes_counter,
    const RuntimeProfile::Counter* timer) {
  DCHECK_EQ(total_bytes_counter->type(), TCounterType::BYTES);
  DCHECK_EQ(timer->type(), TCounterType::CPU_TICKS);
  if (timer->value() == 0) return 0;
  double secs = static_cast<double>(timer->value())
      / static_cast<double>(CpuInfo::cycles_per_ms()) / 1000.0;
  return total_bytes_counter->value() / secs;
}

int64_t RuntimeProfile::CounterSum(const std::vector<Counter*>* counters) {
  int64_t value = 0;
  for (int i = 0; i < counters->size(); ++i) {
    value += (*counters)[i]->value();
  }
  return value;
}

}
