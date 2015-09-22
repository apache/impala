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
#include <stack>
#include <boost/foreach.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/locks.hpp>

#include "common/logging.h"
#include "common/status.h"
#include "common/object-pool.h"
#include "util/debug-util.h"
#include "util/json-util.h"
#include "util/pretty-printer.h"
#include "util/spinlock.h"
#include "util/webserver.h"

#include "gen-cpp/MetricDefs_types.h"
#include "gen-cpp/MetricDefs_constants.h"

namespace impala {

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

  MetricDefs() { };
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

  /// Writes a human-readable representation of this metric to 'out'. This is the
  /// representation that is often displayed in webpages etc.
  virtual std::string ToHumanReadable() = 0;

  const std::string& key() const { return key_; }
  const std::string& description() const { return description_; }

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

/// A SimpleMetric has a value which is a simple primitive type: e.g. integers, strings and
/// floats. It is parameterised not only by the type of its value, but by both the unit
/// (e.g. bytes/s), drawn from TUnit and the 'kind' of the metric itself. The kind
/// can be one of: 'gauge', which may increase or decrease over time, a 'counter' which is
/// increasing only over time, or a 'property' which is not numeric.
//
/// SimpleMetrics return their current value through the value() method. Access to value()
/// is thread-safe.
//
/// TODO: We can use type traits to select a more efficient lock-free implementation of
/// value() etc. where it is safe to do so.
/// TODO: CalculateValue() can be returning a value, its current interface is not clean.
template<typename T, TMetricKind::type metric_kind=TMetricKind::GAUGE>
class SimpleMetric : public Metric {
 public:
  SimpleMetric(const TMetricDef& metric_def, const T& initial_value)
      : Metric(metric_def), unit_(metric_def.units), value_(initial_value) {
    DCHECK_EQ(metric_kind, metric_def.kind) << "Metric kind does not match definition: "
        << metric_def.key;
  }

  virtual ~SimpleMetric() { }

  /// Returns the current value, updating it if necessary. Thread-safe.
  T value() {
    boost::lock_guard<SpinLock> l(lock_);
    CalculateValue();
    return value_;
  }

  /// Sets the current value. Thread-safe.
  void set_value(const T& value) {
    boost::lock_guard<SpinLock> l(lock_);
    value_ = value;
  }

  /// Adds 'delta' to the current value atomically.
  void Increment(const T& delta) {
    DCHECK(kind() != TMetricKind::PROPERTY)
        << "Can't change value of PROPERTY metric: " << key();
    DCHECK(kind() != TMetricKind::COUNTER || delta >= 0)
        << "Can't decrement value of COUNTER metric: " << key();
    if (delta == 0) return;
    boost::lock_guard<SpinLock> l(lock_);
    value_ += delta;
  }

  virtual void ToJson(rapidjson::Document* document, rapidjson::Value* val) {
    rapidjson::Value container(rapidjson::kObjectType);
    AddStandardFields(document, &container);

    rapidjson::Value metric_value;
    ToJsonValue(value(), TUnit::NONE, document, &metric_value);
    container.AddMember("value", metric_value, document->GetAllocator());

    rapidjson::Value type_value(PrintTMetricKind(kind()).c_str(),
        document->GetAllocator());
    container.AddMember("kind", type_value, document->GetAllocator());
    rapidjson::Value units(PrintTUnit(unit()).c_str(), document->GetAllocator());
    container.AddMember("units", units, document->GetAllocator());
    *val = container;
  }

  virtual std::string ToHumanReadable() {
    return PrettyPrinter::Print(value(), unit());
  }

  virtual void ToLegacyJson(rapidjson::Document* document) {
    rapidjson::Value val;
    ToJsonValue(value(), TUnit::NONE, document, &val);
    document->AddMember(key_.c_str(), val, document->GetAllocator());
  }

  const TUnit::type unit() const { return unit_; }
  const TMetricKind::type kind() const { return metric_kind; }

 protected:
  /// Called to compute value_ if necessary during calls to value(). The more natural
  /// approach would be to have virtual T value(), but that's not possible in C++.
  //
  /// TODO: Should be cheap to have a blank implementation, but if required we can cause
  /// the compiler to avoid calling this entirely through a compile-time constant.
  virtual void CalculateValue() { }

  /// Units of this metric.
  const TUnit::type unit_;

  /// Guards access to value_.
  SpinLock lock_;

  /// The current value of the metric
  T value_;
};

/// Container for a set of metrics. A MetricGroup owns the memory for every metric
/// contained within it (see Add*() to create commonly used metric
/// types). Metrics are 'registered' with a MetricGroup, once registered they cannot be
/// deleted.
//
/// MetricGroups may be organised hierarchically as a tree.
//
/// Typically a metric object is cached by its creator after registration. If a metric must
/// be retrieved without an available pointer, FindMetricForTesting() will search the
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
    M* mt = obj_pool_->Add(metric);

    boost::lock_guard<SpinLock> l(lock_);
    DCHECK(metric_map_.find(metric->key_) == metric_map_.end());
    metric_map_[metric->key_] = mt;
    return mt;
  }

  /// Create a gauge metric object with given key and initial value (owned by this object)
  template<typename T>
  SimpleMetric<T>* AddGauge(const std::string& key, const T& value,
      const std::string& metric_def_arg = "") {
    return RegisterMetric(new SimpleMetric<T, TMetricKind::GAUGE>(
        MetricDefs::Get(key, metric_def_arg), value));
  }

  template<typename T>
  SimpleMetric<T, TMetricKind::PROPERTY>* AddProperty(const std::string& key,
      const T& value, const std::string& metric_def_arg = "") {
    return RegisterMetric(new SimpleMetric<T, TMetricKind::PROPERTY>(
        MetricDefs::Get(key, metric_def_arg), value));
  }

  template<typename T>
  SimpleMetric<T, TMetricKind::COUNTER>* AddCounter(const std::string& key,
      const T& value, const std::string& metric_def_arg = "") {
    return RegisterMetric(new SimpleMetric<T, TMetricKind::COUNTER>(
        MetricDefs::Get(key, metric_def_arg), value));
  }

  /// Returns a metric by key. All MetricGroups reachable from this group are searched in
  /// depth-first order, starting with the root group.  Returns NULL if there is no metric
  /// with that key. This is not a very cheap operation; the result should be cached where
  /// possible.
  //
  /// Used for testing only.
  template <typename M>
  M* FindMetricForTesting(const std::string& key) {
    std::stack<MetricGroup*> groups;
    groups.push(this);
    boost::lock_guard<SpinLock> l(lock_);
    do {
      MetricGroup* group = groups.top();
      groups.pop();
      MetricMap::const_iterator it = group->metric_map_.find(key);
      if (it != group->metric_map_.end()) return reinterpret_cast<M*>(it->second);
      BOOST_FOREACH(const ChildGroupMap::value_type& child, group->children_) {
        groups.push(child.second);
      }
    } while (!groups.empty());
    return NULL;
  }

  /// Register page callbacks with the webserver. Only the root of any metric group
  /// hierarchy needs to do this.
  Status Init(Webserver* webserver);

  /// Converts this metric group (and optionally all of its children recursively) to JSON.
  void ToJson(bool include_children, rapidjson::Document* document,
      rapidjson::Value* out_val);

  /// Creates or returns an already existing child metric group.
  MetricGroup* GetChildGroup(const std::string& name);

  /// Useful for debuggers, returns the output of CMCompatibleCallback().
  std::string DebugString();

  const std::string& name() const { return name_; }

 private:
  /// Pool containing all metric objects
  boost::scoped_ptr<ObjectPool> obj_pool_;

  /// Name of this metric group.
  std::string name_;

  /// Guards metric_map_ and children_
  SpinLock lock_;

  /// Contains all Metric objects, indexed by key
  typedef std::map<std::string, Metric*> MetricMap;
  MetricMap metric_map_;

  /// All child metric groups
  typedef std::map<std::string, MetricGroup*> ChildGroupMap;
  ChildGroupMap children_;

  /// Webserver callback for /metrics. Produces a tree of JSON values, each representing a
  /// metric group, and each including a list of metrics, and a list of immediate children.
  /// If args contains a paramater 'metric', only the json for that metric is returned.
  void TemplateCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

  /// Legacy webpage callback for CM 5.0 and earlier. Produces a flattened map of (key,
  /// value) pairs for all metrics in this hierarchy.
  /// If args contains a paramater 'metric', only the json for that metric is returned.
  void CMCompatibleCallback(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);
};

/// We write 'Int' as a placeholder for all integer types.
typedef class SimpleMetric<int64_t, TMetricKind::GAUGE> IntGauge;
typedef class SimpleMetric<uint64_t, TMetricKind::GAUGE> UIntGauge;
typedef class SimpleMetric<double, TMetricKind::GAUGE> DoubleGauge;
typedef class SimpleMetric<int64_t, TMetricKind::COUNTER> IntCounter;

typedef class SimpleMetric<bool, TMetricKind::PROPERTY> BooleanProperty;
typedef class SimpleMetric<std::string, TMetricKind::PROPERTY> StringProperty;

}

#endif // IMPALA_UTIL_METRICS_H
