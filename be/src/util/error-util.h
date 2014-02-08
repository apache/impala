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


#ifndef IMPALA_UTIL_ERROR_UTIL_H
#define IMPALA_UTIL_ERROR_UTIL_H

#include <string>
#include <vector>
#include <boost/cstdint.hpp>
#include "gen-cpp/CatalogObjects_types.h"

namespace impala {

// Returns the error message for errno. We should not use strerror directly
// as that is not thread safe.
// Returns empty string if errno is 0.
std::string GetStrErrMsg();

// Returns an error message warning that the given table names are missing relevant
// table/and or column statistics.
std::string GetTablesMissingStatsWarning(
    const std::vector<TTableName>& tables_missing_stats);
}

#endif
