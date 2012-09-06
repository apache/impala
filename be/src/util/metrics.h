// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_METRICS_H
#define IMPALA_UTIL_METRICS_H

#include <map>
#include <string>
#include <gflags/gflags.h>
#include <sstream>
#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>

#include "common/status.h"
#include "common/object-pool.h"

namespace impala {

class Webserver;

// Publishes execution metrics to a webserver page
class Metrics {
 private:
  // Superclass for metric types, to allow for a single container to hold all metrics
  class GenericMetric {
   public:
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

    // Requires that T supports operator+. Returns value of metric after increment
    T Increment(const T& delta) {
      boost::lock_guard<boost::mutex> l(lock_);
      value_ += delta;
      return value_;
    }

    // Reads the current value under the metric lock
    T value() {
      boost::lock_guard<boost::mutex> l(lock_);
      return value_;
    }

    virtual void Print(std::stringstream* out) {
      (*out) << key_ << ":" << value();
    }    

    virtual void PrintJson(std::stringstream* out) {
      (*out) << "\"" << key_ << "\": \"" << value() << "\"";
    }

   private:
    // Guards access to value
    boost::mutex lock_;
    T value_;

    // Unique key identifying this metric
    const std::string key_;

    // Keep the constructor private so that only Metrics may create Metric instances
    Metric(const std::string& key, const T& value) 
        : value_(value), key_(key) { }

    friend class Metrics;
  };

  // Convenient typedefs for common metric types.
  typedef struct Metric<int64_t> IntMetric;
  typedef struct Metric<double> DoubleMetric;
  typedef struct Metric<std::string> StringMetric;
  typedef struct Metric<bool> BooleanMetric;

  Metrics();

  // Create a metric object with given key and initial value (owned by this object)
  // If a metric is already registered to this name it will be overwritten (in debug
  // builds it is an error)
  template<typename T>
  Metric<T>* CreateMetric(const std::string& key, const T& value) {
    boost::lock_guard<boost::mutex> l(lock_);
    DCHECK(!key.empty());
    DCHECK(metric_map_.find(key) == metric_map_.end()) 
      << "Multiple registrations of metric key: " << key;
    Metric<T>* mt = obj_pool_->Add(new Metric<T>(key, value));
    metric_map_[key] = mt;
    return mt;
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
  void TextCallback(std::stringstream* output);

  // Webserver callback (on /jsonmetrics), renders metrics as a single json document
  void JsonCallback(std::stringstream* output);
};

}

#endif // IMPALA_UTIL_METRICS_H
