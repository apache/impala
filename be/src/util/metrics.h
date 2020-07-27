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

#pragma once

#include <iosfwd>
#include <map>
#include <mutex>
#include <string>
#include <vector>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest_prod.h> // for FRIEND_TEST
#include <rapidjson/fwd.h>

#include "common/atomic.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "kudu/util/web_callback_registry.h"
#include "util/debug-util.h"
#include "util/metrics-fwd.h"
#include "util/spinlock.h"

using kudu::HttpStatusCode;

namespace impala {

class Webserver;

/// Singleton that provides metric definitions. Metrics are defined in metrics.json
/// and generate_metrics.py produces MetricDefs.thrift. This singleton wraps an instance
/// of the thrift definitions.
class MetricDefs {
 public:
  /// Gets the TMetricDef for the metric key. 'arg' is an optional argument to the
  /// TMetricDef for metrics defined by a format string. The key must exist or a DCHECK
  /// will fail.
  /// TODO: Support multiple arguments.
  static TMetricDef Get(const std::string& key, const std::string& arg = "");

 private:
  friend class MetricsTest;

  /// Gets the MetricDefs singleton.
  static MetricDefs* GetInstance();

  /// Contains the map of all TMetricDefs, non-const for testing
  MetricDefsConstants metric_defs_;

  MetricDefs() { }
  DISALLOW_COPY_AND_ASSIGN(MetricDefs);
};

/// A metric is a container for some value, identified by a string key. Most metrics are
/// numeric, but this metric base-class is general enough such that metrics may be lists,
/// maps, histograms or other arbitrary structures.
//
/// Metrics must be able to convert themselves to JSON (for integration with our monitoring
/// tools, and for rendering in webpages). See ToJson(), and also ToLegacyJson() which
/// ensures backwards compatibility with older versions of CM.
//
/// Metrics should be supplied with a description, which is included in JSON output for
/// display by monitoring systems / Impala's webpages.
//
/// TODO: Add ToThrift() for conversion to an RPC-friendly format.
class Metric {
 public:
  /// Empty virtual destructor
  virtual ~Metric() {}

  /// Builds a new Value into 'val', using (if required) the allocator from
  /// 'document'. Should set the following fields where appropriate:
  //
  /// name, value, human_readable, description
  virtual void ToJson(rapidjson::Document* document, rapidjson::Value* val) = 0;

  /// Adds a new json value directly to 'document' of the form:
  /// "name" : "human-readable-string"
  //
  /// This method is kept for backwards-compatibility with CM5.0.
  virtual void ToLegacyJson(rapidjson::Document* document) = 0;

  /// Builds a new Value into 'val', based on prometheus text exposition format
  /// Details of this format can be found below:
  /// https://github.com/prometheus/docs/blob/master/content/docs/instrumenting/
  //  exposition_formats.md
  /// Should set the following fields where appropriate:
  //
  /// name, value, metric_kind
  virtual TMetricKind::type ToPrometheus(
      string name, std::stringstream* val, std::stringstream* metric_kind) = 0;

  /// Writes a human-readable representation of this metric to 'out'. This is the
  /// representation that is often displayed in webpages etc.
  virtual std::string ToHumanReadable() = 0;

  const std::string& key() const { return key_; }
  const std::string& description() const { return description_; }

  bool IsUnitTimeBased(TUnit::type type) {
    return (type == TUnit::type::TIME_MS || type == TUnit::type::TIME_US
        || type == TUnit::type::TIME_NS);
  }

 protected:
  /// Unique key identifying this metric
  const std::string key_;

  /// Description of this metric.
  /// TODO: share one copy amongst metrics with the same description.
  const std::string description_;

  friend class MetricGroup;

  Metric(const TMetricDef& def) : key_(def.key), description_(def.description) { }

  /// Convenience method to add standard fields (name, description, human readable string)
  /// to 'val'.
  void AddStandardFields(rapidjson::Document* document, rapidjson::Value* val);
};

/// A ScalarMetric has a value which is a simple primitive type: e.g. integers, strings
/// and floats. It is parameterised not only by the type of its value, but by both the
/// unit (e.g. bytes/s), drawn from TUnit and the 'kind' of the metric itself.
/// The kind can be one of:
/// - 'gauge', which may increase or decrease over time,
/// - 'counter' which can only increase over time
/// - 'property' which is a value store which can be read and written only
///
/// Note that management software may use the metric kind as hint on how to display
/// the value. ScalarMetrics return their current value through the GetValue() method
/// and set/initialize the value with SetValue(). Both methods are thread safe.
template<typename T, TMetricKind::type metric_kind_t>
class ScalarMetric: public Metric {
 public:
  ScalarMetric(const TMetricDef& metric_def)
    : Metric(metric_def), unit_(metric_def.units) {
    DCHECK_EQ(metric_kind_t, metric_def.kind) << "Metric kind does not match definition: "
        << metric_def.key;
  }

  virtual ~ScalarMetric() { }

  /// Returns the current value. Thread-safe.
  virtual T GetValue() = 0;

  virtual void ToJson(rapidjson::Document* document, rapidjson::Value* val) override;

  virtual TMetricKind::type ToPrometheus(
      std::string name, std::stringstream* val, std::stringstream* metric_kind) override;

  virtual std::string ToHumanReadable() override;

  virtual void ToLegacyJson(rapidjson::Document* document) override;

  TUnit::type unit() const { return unit_; }
  TMetricKind::type kind() const { return metric_kind_t; }

 protected:
  /// Units of this metric.
  const TUnit::type unit_;
};

/// An implementation of scalar metric with spinlock.
template<typename T, TMetricKind::type metric_kind_t>
class LockedMetric : public ScalarMetric<T, metric_kind_t> {
 public:
  LockedMetric(const TMetricDef& metric_def, const T& initial_value)
    : ScalarMetric<T, metric_kind_t>(metric_def), value_(initial_value) {}

  virtual ~LockedMetric() {}

  /// Atomically reads the current value.
  virtual T GetValue() override {
    std::lock_guard<SpinLock> l(lock_);
    return value_;
  }

  /// Atomically sets the value.
  void SetValue(const T& value) {
    std::lock_guard<SpinLock> l(lock_);
    value_ = value;
  }

 protected:
  /// Guards access to value_.
  SpinLock lock_;

  /// The current value of the metric
  T value_;
};

/// An implementation of 'gauge' or 'counter' metric kind. The metric can be incremented
/// atomically via the Increment() interface.
template<TMetricKind::type metric_kind_t>
class AtomicMetric : public ScalarMetric<int64_t, metric_kind_t> {
 public:
  AtomicMetric(const TMetricDef& metric_def, const int64_t initial_value)
    : ScalarMetric<int64_t, metric_kind_t>(metric_def), value_(initial_value) {
    DCHECK(metric_kind_t == TMetricKind::GAUGE || metric_kind_t == TMetricKind::COUNTER);
  }

  virtual ~AtomicMetric() {}

  /// Atomically reads the current value. May be overridden by derived classes.
  /// The default implementation just atomically loads 'value_'. Derived classes
  /// which derive the return value from multiple sources other than 'value_'
  /// need to take care of synchronization among sources.
  virtual int64_t GetValue() override { return value_.Load(); }

  /// Atomically sets the value.
  void SetValue(const int64_t& value) { value_.Store(value); }

  /// Adds 'delta' to the current value atomically and returns the new value.
  int64_t Increment(int64_t delta) {
    DCHECK(metric_kind_t != TMetricKind::COUNTER || delta >= 0)
        << "Can't decrement value of COUNTER metric: " << this->key();
    return value_.Add(delta);
  }

 protected:
  /// The current value of the metric.
  AtomicInt64 value_;
};

/// An AtomicMetric that keeps track of the highest value seen and the current value.
///
/// Implementation notes:
/// The hwm_value_ member maintains the HWM while the current_value_ metric member
/// maintains the current value. Note that since two separate atomics are used
/// for maintaining the current value and HWM, they could be out of sync for a short
/// duration. This behavior is acceptable for current use case. However, it is very
/// important that both the hwm_value_ and current_value_ members are updated together
/// using the interfaces from this class.
class AtomicHighWaterMarkGauge : public ScalarMetric<int64_t, TMetricKind::GAUGE> {
 public:
  AtomicHighWaterMarkGauge(
      const TMetricDef& metric_def, int64_t initial_value, IntGauge* current_value)
    : ScalarMetric<int64_t, TMetricKind::GAUGE>(metric_def),
      hwm_value_(initial_value),
      current_value_(current_value) {
    DCHECK(current_value_ != NULL && initial_value == current_value->GetValue());
  }

  ~AtomicHighWaterMarkGauge() {}

  /// Returns the current high water mark value.
  int64_t GetValue() override { return hwm_value_.Load(); }

  /// Atomically sets the current value and atomically sets the high water mark value.
  void SetValue(const int64_t& value) {
    current_value_->SetValue(value);
    UpdateMax(value);
  }

  /// Adds 'delta' to the current value atomically.
  /// The hwm value is also updated atomically.
  void Increment(int64_t delta) {
    const int64_t new_val = current_value_->Increment(delta);
    UpdateMax(new_val);
  }

  IntGauge* current_value() const { return current_value_; }

 private:
  FRIEND_TEST(MetricsTest, AtomicHighWaterMarkGauge);
  friend class TmpFileMgrTest;

  /// Set 'hwm_value_' to 'v' if 'v' is larger than 'hwm_value_'. The entire operation is
  /// atomic.
  void UpdateMax(int64_t v) {
    while (true) {
      int64_t old_max = hwm_value_.Load();
      int64_t new_max = std::max(old_max, v);
      if (new_max == old_max) break; // Avoid atomic update.
      if (LIKELY(hwm_value_.CompareAndSwap(old_max, new_max))) break;
    }
  }

  /// The high water mark value.
  AtomicInt64 hwm_value_;
  /// The metric representing the current value.
  IntGauge* current_value_;
};

/// Gauge metric that computes the sum of several gauges.
class SumGauge : public IntGauge {
 public:
  SumGauge(const TMetricDef& metric_def, const std::vector<IntGauge*>& gauges)
    : IntGauge(metric_def, 0), gauges_(gauges) {}

  virtual ~SumGauge() {}

  virtual int64_t GetValue() override {
    // Note that this doesn't hold the locks of all gauages before computing the sum so
    // it's possible for one of the gauages to change after being read and added to sum.
    int64_t sum = 0;
    for (auto gauge : gauges_) sum += gauge->GetValue();
    return sum;
  }

 private:
  /// The gauges to be summed.
  std::vector<IntGauge*> gauges_;
};

/// Gauge metric that negates another gauge.
class NegatedGauge : public IntGauge {
 public:
  NegatedGauge(const TMetricDef& metric_def, IntGauge* gauge)
    : IntGauge(metric_def, 0), gauge_(gauge) {}

  virtual ~NegatedGauge() {}

  virtual int64_t GetValue() override { return -gauge_->GetValue(); }

 private:
  /// The metric to be negated.
  IntGauge* gauge_;
};

/// Container for a set of metrics. A MetricGroup owns the memory for every metric
/// contained within it (see Add*() to create commonly used metric
/// types). Metrics are 'registered' with a MetricGroup and can be deleted/removed after
/// being registered. Note: deletion invalidates any pointers to the deleted metric.
//
/// MetricGroups may be organised hierarchically as a tree.
//
/// Typically a metric object is cached by its creator after registration. If a metric
/// must be retrieved without an available pointer, FindMetricForTesting() will search the
/// MetricGroup and all its descendent MetricGroups in turn.
//
/// TODO: Hierarchical naming: that is, resolve "group1.group2.metric-name" to a path
/// through the metric tree.
class MetricGroup {
 public:
  MetricGroup(const std::string& name);

  /// Registers a new metric. Ownership of the metric will be transferred to this
  /// MetricGroup object, so callers should take care not to destroy the Metric they pass
  /// in.
  //
  /// It is an error to call twice with metrics with the same key. The template parameter
  /// M must be a subclass of Metric.
  template <typename M>
  M* RegisterMetric(M* metric) {
    DCHECK(!metric->key_.empty());
    std::lock_guard<SpinLock> l(lock_);
    DCHECK(metric_map_.find(metric->key_) == metric_map_.end()) << metric->key_;
    std::shared_ptr<M> metric_ptr(metric);
    metric_map_[metric->key_] = metric_ptr;
    return metric_ptr.get();
  }

  /// Remove the metric from the metric group and release its memory. This invalidates any
  /// pointers to the deleted metric.
  /// It is an error to call this with a non-existent or already removed metric.
  void RemoveMetric(const std::string& key, const std::string& metric_def_arg = "") {
    TMetricDef metric_def = MetricDefs::Get(key, metric_def_arg);
    DCHECK(!metric_def.key.empty());
    std::lock_guard<SpinLock> l(lock_);
    DCHECK(metric_map_.find(metric_def.key) != metric_map_.end()) << metric_def.key;
    metric_map_.erase(metric_def.key);
    }

  /// Create a gauge metric object with given key and initial value (owned by this object)
  IntGauge* AddGauge(const std::string& key, const int64_t value,
      const std::string& metric_def_arg = "") {
    return RegisterMetric(new IntGauge(MetricDefs::Get(key, metric_def_arg), value));
  }

  DoubleGauge* AddDoubleGauge(const std::string& key, const double value,
      const std::string& metric_def_arg = "") {
    return RegisterMetric(new DoubleGauge(MetricDefs::Get(key, metric_def_arg), value));
  }

  template<typename T>
  LockedMetric<T, TMetricKind::PROPERTY>* AddProperty(const std::string& key,
      const T& value, const std::string& metric_def_arg = "") {
    return RegisterMetric(new LockedMetric<T, TMetricKind::PROPERTY>(
        MetricDefs::Get(key, metric_def_arg), value));
  }

  IntCounter* AddCounter(const std::string& key, const int64_t value,
      const std::string& metric_def_arg = "") {
    return RegisterMetric(new IntCounter(MetricDefs::Get(key, metric_def_arg), value));
  }

  AtomicHighWaterMarkGauge* AddHWMGauge(const std::string& key_hwm,
      const std::string& key_curent_value, const int64_t value,
      const std::string& metric_def_arg = "") {
    IntGauge* current_value_metric = RegisterMetric(
        new IntGauge(MetricDefs::Get(key_curent_value, metric_def_arg), value));
    return RegisterMetric(new AtomicHighWaterMarkGauge(
        MetricDefs::Get(key_hwm, metric_def_arg), value, current_value_metric));
  }

  /// Returns a metric by key. All MetricGroups reachable from this group are searched in
  /// depth-first order, starting with the root group.  Returns NULL if there is no metric
  /// with that key. This is not a very cheap operation; the result should be cached where
  /// possible.
  //
  /// Used for testing only.
  template <typename M>
  M* FindMetricForTesting(const std::string& key) {
    return reinterpret_cast<M*>(FindMetricForTestingInternal(key));
  }

  /// Register page callbacks with the webserver. Only the root of any metric group
  /// hierarchy needs to do this.
  Status RegisterHttpHandlers(Webserver* webserver);

  /// Converts this metric group (and optionally all of its children recursively) to JSON.
  void ToJson(bool include_children, rapidjson::Document* document,
      rapidjson::Value* out_val);

  /// Converts this metric group (and optionally all of its children recursively) to JSON.
  void ToPrometheus(bool include_children, std::stringstream* out_val);

  /// Creates or returns an already existing child metric group.
  MetricGroup* GetOrCreateChildGroup(const std::string& name);

  /// Returns a child metric group with name 'name', or NULL if that group doesn't exist
  MetricGroup* FindChildGroup(const std::string& name);

  /// Useful for debuggers, returns the output of CMCompatibleCallback().
  std::string DebugString();

  const std::string& name() const { return name_; }

 private:
  FRIEND_TEST(MetricsTest, PrometheusMetricNames);
  /// Pool containing all child metric groups.
  boost::scoped_ptr<ObjectPool> obj_pool_;

  /// Name of this metric group.
  std::string name_;

  /// Guards metric_map_ and children_
  SpinLock lock_;

  /// Contains all metric objects, indexed by key. The shared_ptr enclosing the metric
  /// pointer owns the memory and only a single instance of this shared pointer exists
  /// which ensures that it is release when the entry is removed from the map.
  typedef std::unordered_map<std::string, std::shared_ptr<Metric>> MetricMap;
  MetricMap metric_map_;

  /// All child metric groups
  typedef std::unordered_map<std::string, MetricGroup*> ChildGroupMap;
  ChildGroupMap children_;

  // Forward declaration for Webserver::WebRequest.
  using WebRequest = kudu::WebCallbackRegistry::WebRequest;

  /// Webserver callback for /metrics. Produces a tree of JSON values, each representing a
  /// metric group, and each including a list of metrics, and a list of immediate
  /// children.  If args contains a paramater 'metric', only the json for that metric is
  /// returned.
  void TemplateCallback(const WebRequest& req, rapidjson::Document* document);

  /// Webserver callback for /metricsPrometheus. Produces string in prometheus format,
  /// each representing metric group, and each including a list of metrics, and a list
  /// of immediate children.  If args contains a paramater 'metric', only the json for
  /// that metric is returned.
  void PrometheusCallback(const WebRequest& req, std::stringstream* data,
      HttpStatusCode* response);

  /// Legacy webpage callback for CM 5.0 and earlier. Produces a flattened map of (key,
  /// value) pairs for all metrics in this hierarchy.
  /// If args contains a paramater 'metric', only the json for that metric is returned.
  void CMCompatibleCallback(const WebRequest& req, rapidjson::Document* document);

  /// Non-templated implementation for FindMetricForTesting() that does not cast.
  Metric* FindMetricForTestingInternal(const std::string& key);

  /// Convert an Impala metric name into its equivalent name for Prometheus.
  /// All metrics that do not already have "impala_" as a prefix are prefixed with
  /// "impala_" and have their names transformed to fit the standard Prometheus
  /// metric naming conventions.
  /// E.g.
  /// * "impala-server.num-fragments" becomes "impala_server_num_fragments"
  /// * "catalog.num-databases" becomes "impala_catalog_num_databases"
  /// * "memory.rss" becomes "impala_memory_rss"
  static std::string ImpalaToPrometheusName(const std::string& impala_metric_name);
};

/// Convenience method to instantiate a TMetricDef with a subset of its fields defined.
/// Most externally-visible metrics should be defined in metrics.json and retrieved via
/// MetricDefs::Get(). This alternative method of instantiating TMetricDefs is only used
/// in special cases where the regular approach is unsuitable.
TMetricDef MakeTMetricDef(const std::string& key, TMetricKind::type kind,
    TUnit::type unit);

/// Helper to convert a value 'val' that is a time metric into fractional seconds
/// (the only unit of time supported by Prometheus).
template <typename T>
double ConvertToPrometheusSecs(const T& val, TUnit::type unit);

// These template classes are instantiated in the .cc file.
extern template class LockedMetric<bool, TMetricKind::PROPERTY>;
extern template class LockedMetric<std::string, TMetricKind::PROPERTY>;
extern template class LockedMetric<double, TMetricKind::GAUGE>;
extern template class AtomicMetric<TMetricKind::GAUGE>;
extern template class AtomicMetric<TMetricKind::COUNTER>;
}
