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


#ifndef IMPALA_UTIL_NON_PRIMITIVE_METRICS_H
#define IMPALA_UTIL_NON_PRIMITIVE_METRICS_H

#include "util/metrics.h"

#include <string>
#include <vector>
#include <set>
#include <boost/algorithm/string/join.hpp>
#include <boost/foreach.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/count.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/variance.hpp>

namespace impala {

// Non-primitive metrics are those whose values have more structure than simple
// primitive types. Therefore they need specialist PrintValue methods, and
// perhaps update methods that alter the value in place, rather than replacing
// it with a new one.
// TODO: StructuredMetrics or ContainerMetrics are probably better names here.

// Utility method to print an iterable type to a stringstream like ["v1", "v2", "v3"]
// or [v1, v2, v3], depending on whether quotes are requested via the first parameter
template <typename T, typename I>
void PrintStringList(bool print_as_json, const I& iterable, std::stringstream* out) {
  std::vector<std::string> strings;
  BOOST_FOREACH(const T& item, iterable) {
    std::stringstream ss;
    if (print_as_json) {
      PrintPrimitiveAsJson(item, &ss);
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
    PrintStringList<T, std::vector<T> >(true, this->value_, out);
  }

  virtual void PrintValue(std::stringstream* out) {
    PrintStringList<T, std::vector<T> >(false, this->value_, out);
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
    PrintStringList<T, std::set<T> >(true, this->value_, out);
  }

  virtual void PrintValue(std::stringstream* out) {
    PrintStringList<T, std::set<T> >(false, this->value_, out);
  }
};

// Metric whose value is a map from primitive type to primitive type
template <typename K, typename V>
class MapMetric : public Metrics::Metric<std::map<K, V> > {
 public:
  MapMetric(const std::string& key, const std::map<K, V>& value)
      : Metrics::Metric<std::map<K, V> >(key, value) { }

  void Add(const K& key, const V& value) {
    boost::lock_guard<boost::mutex> l(this->lock_);
    this->value_[key] = value;
  }

  void Remove(const K& key) {
    boost::lock_guard<boost::mutex> l(this->lock_);
    this->value_.erase(key);
  }

 protected:
  void PrintToString(bool quoted_items, std::stringstream* out) {
    std::vector<std::string> strings;
    typedef typename std::map<K, V>::value_type ValueType;
    BOOST_FOREACH(const ValueType& entry, this->value_) {
      std::stringstream ss;
      ss << "  ";
      if (quoted_items) {
        ss << "\"" << entry.first << "\" : ";
        PrintPrimitiveAsJson(entry.second, &ss);
      } else {
        ss << entry.first << " : " << entry.second;
      }

      strings.push_back(ss.str());
    }
    (*out) << "\n{\n" << boost::algorithm::join(strings, ",\n") << "\n} ";
  }


  virtual void PrintValueJson(std::stringstream* out) {
    PrintToString(true, out);
  }

  virtual void PrintValue(std::stringstream* out) {
    PrintToString(false, out);
  }
};

// Metric which accumulates min, max and mean of all values
// Printed output looks like:
// name: count: 4, last: 0.0141, min: 4.546e-06, max: 0.0243, mean: 0.0336, stddev: 0.0336
//
// After construction, all statistics are ill-defined. The first call
// to Update will initialise all stats.
template <typename T>
class StatsMetric : public Metrics::Metric<T> {
 public:
  StatsMetric(const std::string& key) : Metrics::Metric<T>(key) { }

  void Update(const T& value) {
    boost::lock_guard<boost::mutex> l(this->lock_);
    this->value_ = value;
    this->acc_(value);
  }

  virtual void PrintValueJson(std::stringstream* out) {
    (*out) << "{ \"count\": ";
    PrintPrimitiveAsJson(boost::accumulators::count(this->acc_), out);
    if (boost::accumulators::count(this->acc_) > 0) {
      (*out) << ", \"last\": ";
      PrintPrimitiveAsJson(this->value_, out);
      (*out) << ", \"min\": ";
      PrintPrimitiveAsJson(boost::accumulators::min(this->acc_), out);
      (*out) << ", \"max\": ";
      PrintPrimitiveAsJson(boost::accumulators::max(this->acc_), out);
      (*out) << ", \"mean\": ";
      PrintPrimitiveAsJson(boost::accumulators::mean(this->acc_), out);
      (*out) << ", \"stddev\": ";
      PrintPrimitiveAsJson(sqrt(boost::accumulators::variance(this->acc_)), out);
    }
    (*out) << " }";
  }

  virtual void PrintValue(std::stringstream* out) {
    (*out) << " count: " << boost::accumulators::count(this->acc_);
    if (boost::accumulators::count(this->acc_) > 0) {
      (*out) << ", last: " << this->value_
             << ", min: " << boost::accumulators::min(this->acc_)
             << ", max: " << boost::accumulators::max(this->acc_)
             << ", mean: " << boost::accumulators::mean(this->acc_)
             << ", stddev: " << sqrt(boost::accumulators::variance(this->acc_));
    }
  }
 private:
  boost::accumulators::accumulator_set<T,
      boost::accumulators::features<boost::accumulators::tag::mean,
                                    boost::accumulators::tag::count,
                                    boost::accumulators::tag::min,
                                    boost::accumulators::tag::max,
                                    boost::accumulators::tag::variance> > acc_;

};

};

#endif
