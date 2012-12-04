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

#include "common/logging.h"
#include "util/metrics.h"
#include <sstream>
#include <boost/algorithm/string/join.hpp>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>

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
    webserver->RegisterPathHandler("/jsonmetrics", json_callback);
  }

  return Status::OK;
}

void Metrics::PrintMetricMap(stringstream* output) {
  lock_guard<mutex> l(lock_);
  BOOST_FOREACH(const MetricMap::value_type& m, metric_map_) {
    m.second->Print(output);
    (*output) << endl;
  }
}

void Metrics::PrintMetricMapAsJson(vector<string>* metrics) {
  lock_guard<mutex> l(lock_);
  BOOST_FOREACH(const MetricMap::value_type& m, metric_map_) {
    stringstream ss;
    m.second->PrintJson(&ss);
    metrics->push_back(ss.str());
  }
}

string Metrics::DebugString() {
  stringstream ss;
  Webserver::ArgumentMap empty_map;
  TextCallback(empty_map, &ss);
  return ss.str();
}

void Metrics::TextCallback(const Webserver::ArgumentMap& args, stringstream* output) {
  (*output) << "<pre>";
  PrintMetricMap(output);
  (*output) << "</pre>";
}

void Metrics::JsonCallback(const Webserver::ArgumentMap& args, stringstream* output) {
  (*output) << "{";
  vector<string> metrics;
  PrintMetricMapAsJson(&metrics);
  (*output) << join(metrics, ",\n");
  (*output) << "}";
}
