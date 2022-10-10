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

#include "util/metrics.h"

#include <sstream>
#include <stack>

#include <boost/algorithm/string/join.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <boost/math/special_functions/fpclassify.hpp>
#include <gutil/strings/substitute.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>

#include "common/logging.h"
#include "util/impalad-metrics.h"
#include "util/json-util.h"
#include "util/pretty-printer.h"
#include "util/webserver.h"

#include "common/names.h"

using namespace impala;
using namespace rapidjson;
using namespace strings;

namespace impala {

template <>
void ToJsonValue<string>(const string& value, const TUnit::type unit,
    Document* document, Value* out_val) {
  Value val(value.c_str(), document->GetAllocator());
  *out_val = val;
}

void Metric::AddStandardFields(Document* document, Value* val) {
  Value name(key_.c_str(), document->GetAllocator());
  val->AddMember("name", name, document->GetAllocator());
  Value desc(description_.c_str(), document->GetAllocator());
  val->AddMember("description", desc, document->GetAllocator());
  Value metric_value(ToHumanReadable().c_str(), document->GetAllocator());
  val->AddMember("human_readable", metric_value, document->GetAllocator());
}

template <typename T, TMetricKind::type metric_kind_t>
void ScalarMetric<T, metric_kind_t>::ToJson(Document* document, Value* val) {
  Value container(kObjectType);
  AddStandardFields(document, &container);

  Value metric_value;
  ToJsonValue(GetValue(), TUnit::NONE, document, &metric_value);
  container.AddMember("value", metric_value, document->GetAllocator());

  Value type_value(PrintValue(kind()).c_str(), document->GetAllocator());
  container.AddMember("kind", type_value, document->GetAllocator());
  Value units(PrintValue(unit()).c_str(), document->GetAllocator());
  container.AddMember("units", units, document->GetAllocator());
  *val = container;
}

template <typename T, TMetricKind::type metric_kind_t>
void ScalarMetric<T, metric_kind_t>::ToLegacyJson(Document* document) {
  Value val;
  ToJsonValue(GetValue(), TUnit::NONE, document, &val);
  Value key(key_.c_str(), document->GetAllocator());
  document->AddMember(key, val, document->GetAllocator());
}

template <typename T, TMetricKind::type metric_kind_t>
TMetricKind::type ScalarMetric<T, metric_kind_t>::ToPrometheus(
    string name, stringstream* val, stringstream* metric_kind) {
  string metric_type = PrintValue(kind()).c_str();
  // prometheus doesn't support 'property', so ignore it
  if (!metric_type.compare("property")) {
    return TMetricKind::PROPERTY;
  }

  if (IsUnitTimeBased(unit())) {
    // check if unit its 'TIME_MS','TIME_US' or 'TIME_NS' and convert it to seconds,
    // this is because prometheus only supports time format in seconds
    *val << ConvertToPrometheusSecs(GetValue(), unit());
  } else {
    *val << GetValue();
  }

  // convert metric type to lower case, that's what prometheus expects
  std::transform(metric_type.begin(), metric_type.end(), metric_type.begin(), ::tolower);

  *metric_kind << "# TYPE " << name << " " << metric_type;
  return kind();
}

template <typename T, TMetricKind::type metric_kind_t>
string ScalarMetric<T, metric_kind_t>::ToHumanReadable() {
  return PrettyPrinter::Print(GetValue(), unit());
}

MetricDefs* MetricDefs::GetInstance() {
  // Note that this is not thread-safe in C++03 (but will be in C++11 see
  // http://stackoverflow.com/a/19907903/132034). We don't bother with the double-check
  // locking pattern because it introduces complexity whereas a race is very unlikely
  // and it doesn't matter if we construct two instances since MetricDefsConstants is
  // just a constant map.
  static MetricDefs instance;
  return &instance;
}

TMetricDef MetricDefs::Get(const string& key, const string& arg) {
  MetricDefs* inst = GetInstance();
  map<string, TMetricDef>::iterator it = inst->metric_defs_.TMetricDefs.find(key);
  if (it == inst->metric_defs_.TMetricDefs.end()) {
    DCHECK(false) << "Could not find metric definition for key=" << key << " arg=" << arg;
    return TMetricDef();
  }
  TMetricDef md = it->second;
  md.__set_key(Substitute(md.key, arg));
  md.__set_description(Substitute(md.description, arg));
  return md;
}

MetricGroup::MetricGroup(const string& name)
    : obj_pool_(new ObjectPool()), name_(name) { }

Status MetricGroup::RegisterHttpHandlers(Webserver* webserver) {
  if (webserver != NULL) {
    Webserver::UrlCallback default_callback =
        bind<void>(mem_fn(&MetricGroup::CMCompatibleCallback), this, _1, _2);
    webserver->RegisterUrlCallback("/jsonmetrics", "legacy-metrics.tmpl",
        default_callback, false);

    Webserver::UrlCallback json_callback =
        bind<void>(mem_fn(&MetricGroup::TemplateCallback), this, _1, _2);
    webserver->RegisterUrlCallback("/metrics", "metrics.tmpl", json_callback, true);

    Webserver::RawUrlCallback prometheus_callback =
        bind<void>(mem_fn(&MetricGroup::PrometheusCallback), this, _1, _2, _3);
    webserver->RegisterUrlCallback("/metrics_prometheus", prometheus_callback);
  }

  return Status::OK();
}

void MetricGroup::CMCompatibleCallback(const Webserver::WebRequest& req,
    Document* document) {
  const auto& args = req.parsed_args;
  // If the request has a 'metric' argument, search all top-level metrics for that metric
  // only. Otherwise, return document with list of all metrics at the top level.
  Webserver::ArgumentMap::const_iterator metric_name = args.find("metric");

  lock_guard<SpinLock> l(lock_);
  if (metric_name != args.end()) {
    MetricMap::const_iterator metric = metric_map_.find(metric_name->second);
    if (metric != metric_map_.end()) {
      metric->second->ToLegacyJson(document);
    }
    return;
  }

  // Add all metrics in the metric_map_ to the given document.
  for (const MetricMap::value_type& m : metric_map_) {
    m.second->ToLegacyJson(document);
  }


  // Depth-first traversal of children to flatten all metrics, which is what was
  // expected by CM before we introduced metric groups.
  stack<MetricGroup*> groups;
  for (const ChildGroupMap::value_type& child : children_) {
    groups.push(child.second);
  }

  while (!groups.empty()) {
    MetricGroup* group = groups.top();
    groups.pop();

    // children_ and metric_map_ are protected by lock_, so acquire group->lock_ before
    // adding the metrics to the given document.
    lock_guard<SpinLock> l(group->lock_);
    for (const ChildGroupMap::value_type& child: group->children_) {
      groups.push(child.second);
    }

    for (const MetricMap::value_type& m: group->metric_map_) {
      m.second->ToLegacyJson(document);
    }
  }
}

void MetricGroup::TemplateCallback(const Webserver::WebRequest& req,
    Document* document) {
  const auto& args = req.parsed_args;
  Webserver::ArgumentMap::const_iterator metric_group = args.find("metric_group");

  lock_guard<SpinLock> l(lock_);
  // If no particular metric group is requested, render this metric group (and all its
  // children).
  if (metric_group == args.end()) {
    Value container;
    ToJson(true, document, &container);
    document->AddMember("metric_group", container, document->GetAllocator());
    return;
  }

  // Search all metric groups to find the one we're looking for. In the future, we'll
  // change this to support path-based resolution of metric groups.
  MetricGroup* found_group = NULL;
  stack<MetricGroup*> groups;
  groups.push(this);
  while (!groups.empty() && found_group == NULL) {
    // Depth-first traversal of children to flatten all metrics, which is what was
    // expected by CM before we introduced metric groups.
    MetricGroup* group = groups.top();
    groups.pop();
    for (const ChildGroupMap::value_type& child: group->children_) {
      if (child.first == metric_group->second) {
        found_group = child.second;
        break;
      }
      groups.push(child.second);
    }
  }
  if (found_group != NULL) {
    Value container;
    found_group->ToJson(false, document, &container);
    document->AddMember("metric_group", container, document->GetAllocator());
  } else {
    Value error(Substitute("Metric group $0 not found", metric_group->second).c_str(),
        document->GetAllocator());
    document->AddMember("error", error, document->GetAllocator());
  }
}

void MetricGroup::PrometheusCallback(
    const Webserver::WebRequest& req, stringstream* data, HttpStatusCode* response) {
  const auto& args = req.parsed_args;
  Webserver::ArgumentMap::const_iterator metric_group = args.find("metric_group");

  lock_guard<SpinLock> l(lock_);
  // If no particular metric group is requested, render this metric group (and all its
  // children).
  if (metric_group == args.end()) {
    Value container;
    ToPrometheus(true, data);
  }
}

void MetricGroup::ToJson(bool include_children, Document* document, Value* out_val) {
  Value metric_list(kArrayType);
  for (const MetricMap::value_type& m: metric_map_) {
    Value metric_value;
    m.second->ToJson(document, &metric_value);
    metric_list.PushBack(metric_value, document->GetAllocator());
  }

  Value container(kObjectType);
  container.AddMember("metrics", metric_list, document->GetAllocator());
  Value name(name_.c_str(), document->GetAllocator());
  container.AddMember("name", name, document->GetAllocator());
  if (include_children) {
    Value child_groups(kArrayType);
    for (const ChildGroupMap::value_type& child: children_) {
      Value child_value;
      child.second->ToJson(true, document, &child_value);
      child_groups.PushBack(child_value, document->GetAllocator());
    }
    container.AddMember("child_groups", child_groups, document->GetAllocator());
  }

  *out_val = container;
}

void MetricGroup::ToPrometheus(bool include_children, stringstream* out_val) {
  for (auto const& m : metric_map_) {
    stringstream metric_value;
    stringstream metric_kind;

    const string& name = ImpalaToPrometheusName(m.first);
    TMetricKind::type metric_type =
        m.second->ToPrometheus(name, &metric_value, &metric_kind);
    if (metric_type == TMetricKind::SET || metric_type == TMetricKind::PROPERTY) {
      // not supported in prometheus
      continue;
    }
    *out_val << "# HELP " << name << " ";
    *out_val << m.second->description_;
    *out_val << "\n";
    *out_val << metric_kind.str();
    *out_val << "\n";
    // append only if metric type is not stats, set or histogram
    if (metric_type != TMetricKind::HISTOGRAM && metric_type != TMetricKind::STATS) {
      *out_val << name;
      *out_val << " ";
    }
    *out_val << metric_value.str();
    *out_val << "\n";
  }

  if (include_children) {
    Value child_groups(kArrayType);
    for (const ChildGroupMap::value_type& child : children_) {
      child.second->ToPrometheus(true, out_val);
    }
  }
}

string MetricGroup::ImpalaToPrometheusName(const string& impala_metric_name) {
  string result = impala_metric_name;
  // Substitute characters as needed to match prometheus conventions. The string is
  // already the right size so we can do this in place.
  for (size_t i = 0; i < result.size(); ++i) {
    if (result[i] == '.' || result[i] == '-') result[i] = '_';
  }
  if (result.compare(0, 7, "impala_") != 0) {
    result.insert(0, "impala_");
  }
  return result;
}

MetricGroup* MetricGroup::GetOrCreateChildGroup(const string& name) {
  lock_guard<SpinLock> l(lock_);
  ChildGroupMap::iterator it = children_.find(name);
  if (it != children_.end()) return it->second;
  MetricGroup* group = obj_pool_->Add(new MetricGroup(name));
  children_[name] = group;
  return group;
}

MetricGroup* MetricGroup::FindChildGroup(const string& name) {
  lock_guard<SpinLock> l(lock_);
  ChildGroupMap::iterator it = children_.find(name);
  if (it != children_.end()) return it->second;
  return NULL;
}

Metric* MetricGroup::FindMetricForTestingInternal(const string& key) {
  stack<MetricGroup*> groups;
  groups.push(this);
  lock_guard<SpinLock> l(lock_);
  do {
    MetricGroup* group = groups.top();
    groups.pop();
    auto it = group->metric_map_.find(key);
    if (it != group->metric_map_.end()) return it->second.get();
    for (const auto& child : group->children_) {
      groups.push(child.second);
    }
  } while (!groups.empty());
  return nullptr;
}

string MetricGroup::DebugString() {
  Webserver::WebRequest empty_req;
  Document document;
  document.SetObject();
  TemplateCallback(empty_req, &document);
  StringBuffer strbuf;
  PrettyWriter<StringBuffer> writer(strbuf);
  document.Accept(writer);
  return strbuf.GetString();
}

TMetricDef MakeTMetricDef(const string& key, TMetricKind::type kind, TUnit::type unit) {
  TMetricDef ret;
  ret.__set_key(key);
  ret.__set_kind(kind);
  ret.__set_units(unit);
  return ret;
}

template <typename T>
double ConvertToPrometheusSecs(const T& val, TUnit::type unit) {
  double value = val;
  if (unit == TUnit::type::TIME_MS) {
    value /= 1000;
  } else if (unit == TUnit::type::TIME_US) {
    value /= 1000000;
  } else if (unit == TUnit::type::TIME_NS) {
    value /= 1000000000;
  }
  return value;
}

// Explicitly instantiate the variants that will be used.
template double ConvertToPrometheusSecs<>(const double&, TUnit::type);
template double ConvertToPrometheusSecs<>(const int64_t&, TUnit::type);
template double ConvertToPrometheusSecs<>(const uint64_t&, TUnit::type);

template <>
double ConvertToPrometheusSecs<string>(const string& val, TUnit::type unit) {
  DCHECK(false) << "Should not be called for string metrics";
  return 0.0;
}

// Explicitly instantiate template classes with parameter combinations that will be used.
// If these classes are instantiated with new parameters, the instantiation must be
// added to this list. This is required because some methods of these classes are
// defined in .cc files and are used from other translation units.
template class LockedMetric<bool, TMetricKind::PROPERTY>;
template class LockedMetric<std::string, TMetricKind::PROPERTY>;
template class LockedMetric<double, TMetricKind::GAUGE>;
template class AtomicMetric<TMetricKind::GAUGE>;
template class AtomicMetric<TMetricKind::COUNTER>;
} // namespace impala
