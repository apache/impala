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

#include "util/collection-metrics.h"

#include <ostream>

#include <boost/accumulators/statistics/count.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/variance.hpp>

#include "util/json-util.h"
#include "util/pretty-printer.h"

#include "common/names.h"

namespace impala {

template <typename T>
void SetMetric<T>::ToJson(rapidjson::Document* document, rapidjson::Value* value) {
  rapidjson::Value container(rapidjson::kObjectType);
  AddStandardFields(document, &container);
  rapidjson::Value metric_list(rapidjson::kArrayType);
  for (const T& s: value_) {
    rapidjson::Value entry_value;
    ToJsonValue(s, TUnit::NONE, document, &entry_value);
    metric_list.PushBack(entry_value, document->GetAllocator());
  }
  container.AddMember("items", metric_list, document->GetAllocator());
  *value = container;
}

template <typename T>
void SetMetric<T>::ToLegacyJson(rapidjson::Document* document) {
  rapidjson::Value metric_list(rapidjson::kArrayType);
  for (const T& s: value_) {
    rapidjson::Value entry_value;
    ToJsonValue(s, TUnit::NONE, document, &entry_value);
    metric_list.PushBack(entry_value, document->GetAllocator());
  }
  rapidjson::Value key(key_.c_str(), document->GetAllocator());
  document->AddMember(key, metric_list, document->GetAllocator());
}

template <typename T>
string SetMetric<T>::ToHumanReadable() {
  stringstream out;
  PrettyPrinter::PrintStringList<set<T>>(value_, TUnit::NONE, &out);
  return out.str();
}

template <typename T, int StatsSelection>
TMetricKind::type StatsMetric<T, StatsSelection>::ToPrometheus(
    string name, stringstream* val, stringstream* metric_kind) {
  lock_guard<mutex> l(lock_);

  *val << name << "_total " << boost::accumulators::count(acc_) << "\n";

  if (boost::accumulators::count(acc_) > 0) {
    if (IsUnitTimeBased(unit_)) {
      *val << name << "_last " << ConvertToPrometheusSecs(value_, unit_) << "\n";
    } else {
      *val << name << "_last " << value_ << "\n";
    }

    if (StatsSelection & StatsType::MIN) {
      if (IsUnitTimeBased(unit_)) {
        *val << name << "_min "
             << ConvertToPrometheusSecs(boost::accumulators::min(acc_), unit_) << "\n";
      } else {
        *val << name << "_min " << boost::accumulators::min(acc_) << "\n";
      }
    }

    if (StatsSelection & StatsType::MAX) {
      if (IsUnitTimeBased(unit_)) {
        *val << name << "_max "
             << ConvertToPrometheusSecs(boost::accumulators::max(acc_), unit_) << "\n";
      } else {
        *val << name << "_max " << boost::accumulators::max(acc_) << "\n";
      }
    }

    if (StatsSelection & StatsType::MEAN) {
      if (IsUnitTimeBased(unit_)) {
        *val << name << "_mean "
             << ConvertToPrometheusSecs(boost::accumulators::mean(acc_), unit_) << "\n";
      } else {
        *val << name << "_mean " << boost::accumulators::mean(acc_) << "\n";
      }
    }

    if (StatsSelection & StatsType::STDDEV) {
      if (IsUnitTimeBased(unit_)) {
        *val << name << "_stddev "
             << ConvertToPrometheusSecs(
                    std::sqrt(boost::accumulators::variance(acc_)), unit_)
             << "\n";
      } else {
        *val << name << "_stddev " << std::sqrt(boost::accumulators::variance(acc_))
             << "\n";
      }
    }
  }
  *metric_kind << "# TYPE " << name << " counter";
  return TMetricKind::STATS;
}

template <typename T, int StatsSelection>
void StatsMetric<T, StatsSelection>::ToJson(
    rapidjson::Document* document, rapidjson::Value* val) {
  lock_guard<mutex> l(lock_);
  rapidjson::Value container(rapidjson::kObjectType);
  AddStandardFields(document, &container);
  rapidjson::Value units(PrintValue(unit_).c_str(), document->GetAllocator());
  container.AddMember("units", units, document->GetAllocator());

  if (StatsSelection & StatsType::COUNT) {
    container.AddMember("count",
        static_cast<uint64_t>(boost::accumulators::count(acc_)),
        document->GetAllocator());
  }

  if (boost::accumulators::count(acc_) > 0) {
    container.AddMember("last", value_, document->GetAllocator());

    if (StatsSelection & StatsType::MIN) {
      container.AddMember("min",
          static_cast<uint64_t>(boost::accumulators::min(acc_)),
          document->GetAllocator());
    }

    if (StatsSelection & StatsType::MAX) {
      container.AddMember("max", boost::accumulators::max(acc_),
          document->GetAllocator());
    }

    if (StatsSelection & StatsType::MEAN) {
      container.AddMember("mean", boost::accumulators::mean(acc_),
        document->GetAllocator());
    }

    if (StatsSelection & StatsType::STDDEV) {
      container.AddMember("stddev", sqrt(boost::accumulators::variance(acc_)),
          document->GetAllocator());
    }
  }
  *val = container;
}

template <typename T, int StatsSelection>
void StatsMetric<T, StatsSelection>::ToLegacyJson(rapidjson::Document* document) {
  stringstream ss;
  lock_guard<mutex> l(lock_);
  rapidjson::Value container(rapidjson::kObjectType);

  if (StatsSelection & StatsType::COUNT) {
    container.AddMember("count", boost::accumulators::count(acc_),
        document->GetAllocator());
  }

  if (boost::accumulators::count(acc_) > 0) {
    container.AddMember("last", value_, document->GetAllocator());
    if (StatsSelection & StatsType::MIN) {
      container.AddMember("min", boost::accumulators::min(acc_),
          document->GetAllocator());
    }

    if (StatsSelection & StatsType::MAX) {
      container.AddMember("max", boost::accumulators::max(acc_),
          document->GetAllocator());
    }

    if (StatsSelection & StatsType::MEAN) {
      container.AddMember("mean", boost::accumulators::mean(acc_),
        document->GetAllocator());
    }

    if (StatsSelection & StatsType::STDDEV) {
      container.AddMember("stddev", sqrt(boost::accumulators::variance(acc_)),
          document->GetAllocator());
    }
  }
  rapidjson::Value key(key_.c_str(), document->GetAllocator());
  document->AddMember(key, container, document->GetAllocator());
}

template <typename T, int StatsSelection>
string StatsMetric<T, StatsSelection>::ToHumanReadable() {
  stringstream out;
  if (StatsSelection & StatsType::COUNT) {
    out << "count: " << boost::accumulators::count(acc_);
    if (boost::accumulators::count(acc_) > 0) out << ", ";
  }
  if (boost::accumulators::count(acc_) > 0) {

    out << "last: " << PrettyPrinter::Print(value_, unit_);
    if (StatsSelection & StatsType::MIN) {
      out << ", min: " << PrettyPrinter::Print(boost::accumulators::min(acc_), unit_);
    }

    if (StatsSelection & StatsType::MAX) {
      out << ", max: " << PrettyPrinter::Print(boost::accumulators::max(acc_), unit_);
    }

    if (StatsSelection & StatsType::MEAN) {
      out << ", mean: " << PrettyPrinter::Print(boost::accumulators::mean(acc_), unit_);
    }

    if (StatsSelection & StatsType::STDDEV) {
      out << ", stddev: " << PrettyPrinter::Print(
          sqrt(boost::accumulators::variance(acc_)), unit_);
    }
  }
  return out.str();
}

// Explicitly instantiate template classes with parameter combinations that will be used.
// If these classes are instantiated with new parameters, the instantiation must be
// added to this list. This is required because some methods of these classes are
// defined in .cc files and are used from other translation units.
template class SetMetric<int32_t>;
template class SetMetric<string>;
template class StatsMetric<uint64_t, StatsType::ALL>;
template class StatsMetric<double, StatsType::ALL>;
template class StatsMetric<uint64_t, StatsType::MEAN>;
template class StatsMetric<uint64_t, StatsType::MAX | StatsType::MEAN>;
}
