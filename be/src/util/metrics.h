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


#ifndef IMPALA_UTIL_METRICS_H
#define IMPALA_UTIL_METRICS_H

#include <map>
#include <string>
#include <sstream>
#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>

#include "common/logging.h"
#include "common/status.h"
#include "common/object-pool.h"
#include "util/webserver.h"

namespace impala {

// Publishes execution metrics to a webserver page
// TODO: Reconsider naming here; Metrics is too general.
class Metrics {
 private:
  // Superclass for metric types, to allow for a single container to hold all metrics
  class GenericMetric {
   public:
    // Empty virtual destructor
    virtual ~GenericMetric() {}

    // Print key and value to a string
    virtual void Print(std::stringstream* out) = 0;

    // Print key and value in Json format
    virtual void PrintJson(std::stringstream* out) = 0;
  };

 public:
  // Structure containing a metric value. Provides for thread-safe update and 
  // test-and-set operations.
  template<typename T>
  class Metric : GenericMetric {
   public:
    // Sets current metric value to parameter
    void Update(const T& value) { 
      boost::lock_guard<boost::mutex> l(lock_);
      value_ = value;
    }

    void Increment(const T& delta) {
      boost::lock_guard<boost::mutex> l(lock_);
      value_ += delta;
    }

    // If current value == test_, update with new value. In all cases return
    // current value so that success can be detected. 
    T TestAndSet(const T& value, const T& test) {
      boost::lock_guard<boost::mutex> l(lock_);
      if (value_ == test) {
        value_ = value;
        return test;
      }
      return value_;
    }

    // Reads the current value under the metric lock
    T value() {
      boost::lock_guard<boost::mutex> l(lock_);
      return value_;
    }

    virtual void Print(std::stringstream* out) {
      boost::lock_guard<boost::mutex> l(lock_);
      (*out) << key_ << ":";
      PrintValue(out);
    }    

    virtual void PrintJson(std::stringstream* out) {
      boost::lock_guard<boost::mutex> l(lock_);
      (*out) << "\"" << key_ << "\": ";
      PrintValueJson(out);
    }

    Metric(const std::string& key, const T& value) 
        : value_(value), key_(key) { }

   protected:
    // Subclasses are required to implement this to print a string
    // representation of the metric to the supplied stringstream.
    // Both methods are always called with lock_ taken, so implementations must
    // not try and take lock_ themselves..
    virtual void PrintValue(std::stringstream* out) = 0;
    virtual void PrintValueJson(std::stringstream* out) = 0;

    // Guards access to value
    boost::mutex lock_;
    T value_;

    // Unique key identifying this metric
    const std::string key_;

    friend class Metrics;
  };

  // PrimitiveMetrics are the most common metric type, whose values natively
  // support operator<< and optionally operator+. 
  template<typename T>
  class PrimitiveMetric : public Metric<T> {
   public:
    PrimitiveMetric(const std::string& key, const T& value) 
        : Metric<T>(key, value) {
    }

    // Requires that T supports operator+. Returns value of metric after increment
    T Increment(const T& delta) {
      boost::lock_guard<boost::mutex> l(this->lock_);
      this->value_ += delta;
      return this->value_;
    }

   protected:
    virtual void PrintValue(std::stringstream* out)  {
      (*out) << this->value_;
    }    

    virtual void PrintValueJson(std::stringstream* out)  {
      (*out) << "\"" << this->value_ << "\"";
    }    
  };

  // Convenient typedefs for common primitive metric types.
  typedef class PrimitiveMetric<int64_t> IntMetric;
  typedef class PrimitiveMetric<double> DoubleMetric;
  typedef class PrimitiveMetric<std::string> StringMetric;
  typedef class PrimitiveMetric<bool> BooleanMetric;

  Metrics();

  // Create a primitive metric object with given key and initial value (owned by
  // this object) If a metric is already registered to this name it will be
  // overwritten (in debug builds it is an error)
  template<typename T>
  PrimitiveMetric<T>* CreateAndRegisterPrimitiveMetric(const std::string& key, 
      const T& value) {
    return RegisterMetric(new PrimitiveMetric<T>(key, value));
  }

  // Registers a new metric. Ownership of the metric will be transferred to this
  // Metrics object, so callers should take care not to destroy the Metric they
  // pass in.
  // If a metric already exists with the supplied metric's key, it is replaced. 
  // The template parameter M must be a subclass of Metric.
  template <typename M>
  M* RegisterMetric(M* metric) {
    boost::lock_guard<boost::mutex> l(lock_);    
    DCHECK(!metric->key_.empty());
    DCHECK(metric_map_.find(metric->key_) == metric_map_.end()) 
      << "Multiple registrations of metric key: " << metric->key_;

    M* mt = obj_pool_->Add(metric);
    metric_map_[metric->key_] = mt;
    return mt;    
  }

  // Returns a metric by key.  Returns NULL if there is no metric with that 
  // key.  This is not a very cheap operation and should not be called in a loop.
  // If the metric needs to be updated in a loop, the returned metric should be cached.
  template <typename M>
  M* GetMetric(const std::string& key) {
    boost::lock_guard<boost::mutex> l(lock_);    
    MetricMap::iterator it = metric_map_.find(key);
    if (it == metric_map_.end()) return NULL;
    return reinterpret_cast<M*>(it->second);
  }

  // Register page callbacks with the webserver
  Status Init(Webserver* webserver);

  // Useful for debuggers, returns the output of TextCallback
  std::string DebugString();

 private:
  // Pool containing all metric objects
  boost::scoped_ptr<ObjectPool> obj_pool_;

  // Contains all Metric objects, indexed by key
  typedef std::map<std::string, GenericMetric*> MetricMap;
  MetricMap metric_map_;

  // Guards metric_map_
  boost::mutex lock_;

  // Writes metric_map_ as a list of key : value pairs
  void PrintMetricMap(std::stringstream* output);

  // Builds a list of metrics as Json-style "key": "value" pairs
  void PrintMetricMapAsJson(std::vector<std::string>* metrics);

  // Webserver callback (on /metrics), renders metrics as single text page
  void TextCallback(const Webserver::ArgumentMap& args, std::stringstream* output);

  // Webserver callback (on /jsonmetrics), renders metrics as a single json document
  void JsonCallback(const Webserver::ArgumentMap& args, std::stringstream* output);
};

}

#endif // IMPALA_UTIL_METRICS_H
