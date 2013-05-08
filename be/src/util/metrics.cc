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

#include "util/metrics.h"

#include <sstream>
#include <boost/algorithm/string/join.hpp>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <boost/math/special_functions/fpclassify.hpp>

#include "common/logging.h"
#include "util/impalad-metrics.h"

using namespace impala;
using namespace std;
using namespace boost;
using namespace boost::algorithm;

Metrics::Metrics()
  : obj_pool_(new ObjectPool()) { }

Status Metrics::Init(Webserver* webserver) {
  if (webserver != NULL) {
    Webserver::PathHandlerCallback default_callback =
        bind<void>(mem_fn(&Metrics::TextCallback), this, _1, _2);
    webserver->RegisterPathHandler("/metrics", default_callback);

    Webserver::PathHandlerCallback json_callback =
        bind<void>(mem_fn(&Metrics::JsonCallback), this, _1, _2);
    webserver->RegisterPathHandler("/jsonmetrics", json_callback, false, false);
  }

  return Status::OK;
}

string Metrics::DebugString() {
  stringstream ss;
  Webserver::ArgumentMap empty_map;
  TextCallback(empty_map, &ss);
  return ss.str();
}

string Metrics::DebugStringJson() {
  stringstream ss;
  Webserver::ArgumentMap empty_map;
  JsonCallback(empty_map, &ss);
  return ss.str();
}

void Metrics::TextCallback(const Webserver::ArgumentMap& args, stringstream* output) {
  Webserver::ArgumentMap::const_iterator metric_name = args.find("metric");
  lock_guard<mutex> l(lock_);
  if (args.find("raw") == args.end()) (*output) << "<pre>";
  if (metric_name == args.end()) {
    BOOST_FOREACH(const MetricMap::value_type& m, metric_map_) {
      m.second->Print(output);
      (*output) << endl;
    }
  } else {
    MetricMap::const_iterator metric = metric_map_.find(metric_name->second);
    if (metric == metric_map_.end()) {
      (*output) << "Metric '" << metric_name->second << "' not found";
    } else {
      metric->second->Print(output);
      (*output) << endl;
    }
  }
  if (args.find("raw") == args.end()) (*output) << "</pre>";
}

void Metrics::JsonCallback(const Webserver::ArgumentMap& args, stringstream* output) {
  Webserver::ArgumentMap::const_iterator metric_name = args.find("metric");
  lock_guard<mutex> l(lock_);
  (*output) << "{";
  if (metric_name == args.end()) {
    bool first = true;
    BOOST_FOREACH(const MetricMap::value_type& m, metric_map_) {
      if (first) {
        first = false;
      } else {
        (*output) << ",\n";
      }
      m.second->PrintJson(output);
    }
  } else {
    MetricMap::const_iterator metric = metric_map_.find(metric_name->second);
    if (metric != metric_map_.end()) {
      metric->second->PrintJson(output);
      (*output) << endl;
    }
  }
  (*output) << "}";
}

template<> void PrintPrimitiveAsJson<std::string>(const string& v, stringstream* out) {
  (*out) << "\"" << v << "\"";
}

template<> void PrintPrimitiveAsJson<double>(const double& v, stringstream* out) {
  if (isfinite(v)) {
    (*out) << v;
  } else {
    // This does not call the std::string override, but instead writes
    // the literal null, in keeping with the JSON spec.
    PrintPrimitiveAsJson("null", out);
  }
}
