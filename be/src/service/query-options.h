// Copyright 2014 Cloudera Inc.
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

#ifndef IMPALA_SERVICE_QUERY_OPTIONS_H
#define IMPALA_SERVICE_QUERY_OPTIONS_H

#include <string>
#include <map>

#include "common/status.h"

/// Utility methods to process per-query options

namespace impala {

class TQueryOptions;

/// Converts a TQueryOptions struct into a map of key, value pairs
void TQueryOptionsToMap(const TQueryOptions& query_options,
    std::map<std::string, std::string>* configuration);

/// Set the key/value pair in TQueryOptions. It will override existing setting in
/// query_options.
Status SetQueryOption(const std::string& key, const std::string& value,
    TQueryOptions* query_options);

/// Parse a "," separated key=value pair of query options and set it in 'query_options'.
/// If the same query option is specified more than once, the last one wins.
/// Return an error if the input is invalid (bad format or invalid query option).
Status ParseQueryOptions(const std::string& options, TQueryOptions* query_options);

}

#endif
