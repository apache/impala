// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "common/logging.h"
#include "util/webserver.h"
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
        bind<void>(mem_fn(&Metrics::TextCallback), this, _1);    
    webserver->RegisterPathHandler("/metrics", default_callback);

    Webserver::PathHandlerCallback json_callback =
        bind<void>(mem_fn(&Metrics::JsonCallback), this, _1);    
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
  TextCallback(&ss);
  return ss.str();
}

void Metrics::TextCallback(stringstream* output) {
  (*output) << "<pre>";
  PrintMetricMap(output);
  (*output) << "</pre>";
}

void Metrics::JsonCallback(stringstream* output) {
  (*output) << "{";
  vector<string> metrics;
  PrintMetricMapAsJson(&metrics);
  (*output) << join(metrics, ",\n");
  (*output) << "}";
}
