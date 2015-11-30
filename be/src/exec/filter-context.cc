// Copyright 2016 Cloudera Inc.
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

#include "exec/filter-context.h"

using namespace impala;
using namespace strings;

const std::string FilterStats::ROW_GROUPS_KEY = "RowGroups";
const std::string FilterStats::FILES_KEY = "Files";
const std::string FilterStats::SPLITS_KEY = "Splits";
const std::string FilterStats::ROWS_KEY = "Rows";

FilterStats::FilterStats(RuntimeProfile* runtime_profile, bool is_partition_filter) {
  DCHECK(runtime_profile != NULL);
  profile = runtime_profile;
  if (is_partition_filter) {
    RegisterCounterGroup(FilterStats::SPLITS_KEY);
    RegisterCounterGroup(FilterStats::FILES_KEY);
  }

  // TODO: These only apply to Parquet, so only register them in that case.
  RegisterCounterGroup(FilterStats::ROWS_KEY);
  if (is_partition_filter) RegisterCounterGroup(FilterStats::ROW_GROUPS_KEY);
}

void FilterStats::IncrCounters(const string& key, int32_t total, int32_t processed,
    int32_t rejected) const {
  CountersMap::const_iterator it = counters.find(key);
  DCHECK(it != counters.end()) << "Tried to increment unknown counter group";
  it->second.total->Add(total);
  it->second.processed->Add(processed);
  it->second.rejected->Add(rejected);
}

/// Adds a new counter group with key 'key'. Not thread safe.
void FilterStats::RegisterCounterGroup(const string& key) {
  CounterGroup counter;
  counter.total =
      ADD_COUNTER(profile, Substitute("$0 total", key), TUnit::UNIT);
  counter.processed =
      ADD_COUNTER(profile, Substitute("$0 processed", key), TUnit::UNIT);
  counter.rejected =
      ADD_COUNTER(profile, Substitute("$0 rejected", key), TUnit::UNIT);
  counters[key] = counter;
}

Status FilterContext::CloneFrom(const FilterContext& from, RuntimeState* state) {
  filter = from.filter;
  stats = from.stats;
  return from.expr->Clone(state, &expr);
}
