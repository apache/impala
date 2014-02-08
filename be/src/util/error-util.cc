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

#include "util/error-util.h"

#include <errno.h>
#include <string.h>
#include <sstream>

using namespace std;

namespace impala {

string GetStrErrMsg() {
  // Save errno. "<<" could reset it.
  int e = errno;
  if (e == 0) return "";
  stringstream ss;
  char buf[1024];
  ss << "Error(" << e << "): " << strerror_r(e, buf, 1024);
  return ss.str();
}

string GetTablesMissingStatsWarning(const vector<TTableName>& tables_missing_stats) {
  stringstream ss;
  if (tables_missing_stats.empty()) return string("");
  ss << "WARNING: The following tables are missing relevant table and/or column "
     << "statistics.\n";
  for (int i = 0; i < tables_missing_stats.size(); ++i) {
    const TTableName& table_name = tables_missing_stats[i];
    if (i != 0) ss << ",";
    ss << table_name.db_name << "." << table_name.table_name;
  }
  return ss.str();
}

}
