// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_NON_PRIMITIVE_METRICS_H
#define IMPALA_UTIL_NON_PRIMITIVE_METRICS_H

#include <string>
#include <vector>
#include <set>
#include <boost/algorithm/string/join.hpp>
#include <boost/foreach.hpp>
#include "util/metrics.h"

namespace impala {

// Non-primitive metrics are those whose values have more structure than simple
// primitive types. Therefore they need specialist PrintValue methods, and
// perhaps update methods that alter the value in place, rather than replacing
// it with a new one.

// Utility method to print an iterable type to a stringstream like ["v1", "v2", "v3"] 
// or [v1, v2, v3], depending on whether quotes are requested via the first parameter
template <typename T, typename I>
void PrintStringList(bool quoted_items, const I& iterable, std::stringstream* out) {
  std::vector<std::string> strings;
  BOOST_FOREACH(const T& item, iterable) {
    std::stringstream ss;
    if (quoted_items) {
      ss << "\"" << item << "\"";
    } else  {
      ss << item;
    }
    strings.push_back(ss.str());    
  }

  (*out) <<"[" << boost::algorithm::join(strings, ", ") << "]";
}

// Metric whose value is a list of primitive items
template <typename T>
class ListMetric : public Metrics::Metric<std::vector<T> > {
 public:
  ListMetric(const std::string& key, const std::vector<T>& value)
      : Metrics::Metric<std::vector<T> >(key, value) {
  }

 protected:
  virtual void PrintValueJson(std::stringstream* out) {
    PrintStringList<T, std::vector<T> >(true, this->value(), out);
  }

  virtual void PrintValue(std::stringstream* out) {
    PrintStringList<T, std::vector<T> >(false, this->value(), out);
  }
};

// Metric whose value is a set of primitive items
template <typename T>
class SetMetric : public Metrics::Metric<std::set<T> > {
 public:
  SetMetric(const std::string& key, const std::set<T>& value)
      : Metrics::Metric<std::set<T> >(key, value) {
  }

  void Add(const T& item) {
    boost::lock_guard<boost::mutex> l(this->lock_);
    this->value_.insert(item);
  }

  void Remove(const T& item) {
    boost::lock_guard<boost::mutex> l(this->lock_);
    this->value_.erase(item);
  }

 protected:
  virtual void PrintValueJson(std::stringstream* out) {    
    PrintStringList<T, std::set<T> >(true, this->value(), out);
  }

  virtual void PrintValue(std::stringstream* out) {
    PrintStringList<T, std::set<T> >(false, this->value(), out);
  }
};

};

#endif
