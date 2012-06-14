// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "util/runtime-profile.h"

#include "common/object-pool.h"
#include "util/debug-util.h"

#include <iostream>

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
  DCHECK(*idx < nodes.size());

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

void RuntimeProfile::Merge(const RuntimeProfile& other) {
  // Merge this level
  map<string, Counter*>::iterator dst_iter;
  map<string, Counter*>::const_iterator src_iter;
  for (src_iter = other.counter_map_.begin();
       src_iter != other.counter_map_.end(); ++src_iter) {
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

  // Recursively merge children with matching names
  for (int i = 0; i < other.children_.size(); ++i) {
    const RuntimeProfile* other_child = other.children_[i];
    bool merged = false;
    for (int j = 0; j < children_.size(); ++j) {
      if (children_[j]->name_.compare(other_child->name_) == 0) {
        children_[j]->Merge(*other_child);
        merged = true;
        break;
      }
    }
    if (!merged) {
      RuntimeProfile* new_profile = pool_->Add(
          new RuntimeProfile(pool_, other_child->name_));
      children_.push_back(new_profile);
      new_profile->Merge(*other_child);
    }
  }
}

void RuntimeProfile::Divide(int n) {
  map<string, Counter*>::iterator iter;
  for (iter = counter_map_.begin(); iter != counter_map_.end(); ++iter) {
    iter->second->value_ /= n;
  }
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->Divide(n);
  }
}

void RuntimeProfile::AddChild(RuntimeProfile* parent) {
  children_.push_back(parent);
}

RuntimeProfile::Counter* RuntimeProfile::AddCounter(
    const string& name, TCounterType::type type) {
  if (counter_map_.find(name) != counter_map_.end()) {
    return counter_map_[name];
  }
  Counter* counter = pool_->Add(new Counter(type));
  counter_map_[name] = counter;
  return counter;
}

RuntimeProfile::Counter* RuntimeProfile::GetCounter(const string& name) {
  if (counter_map_.find(name) != counter_map_.end()) {
    return counter_map_[name];
  }
  return NULL;
}

void RuntimeProfile::PrettyPrint(ostream* s, const string& prefix) const {
  ostream& stream = *s;
  map<string, Counter*>::const_iterator total_time = counter_map_.find("TotalTime");
  DCHECK(total_time != counter_map_.end());
  stream << prefix << name_ << ": ("
         << PrettyPrinter::Print(total_time->second->value(), TCounterType::CPU_TICKS)
         << ")" << endl;
  for (map<string, Counter*>::const_iterator iter = counter_map_.begin();
       iter != counter_map_.end(); ++iter) {
    if (iter == total_time) continue;
    stream << prefix << "   - " << iter->first << ": "
           << PrettyPrinter::Print(iter->second->value(), iter->second->type())
           << endl;
  }
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->PrettyPrint(s, prefix + "  ");
  }
}

void RuntimeProfile::ToThrift(vector<TRuntimeProfileNode>* nodes) const {
  nodes->reserve(nodes->size() + children_.size());

  int index = nodes->size();
  nodes->push_back(TRuntimeProfileNode());
  TRuntimeProfileNode& node = (*nodes)[index];
  node.name = name_;
  node.num_children = children_.size();

  for (map<string, Counter*>::const_iterator iter = counter_map_.begin();
       iter != counter_map_.end(); ++iter) {
    TCounter counter;
    counter.name = iter->first;
    counter.value = iter->second->value();
    counter.type = iter->second->type();
    node.counters.push_back(counter);
  }

  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->ToThrift(nodes);
  }
}

}
