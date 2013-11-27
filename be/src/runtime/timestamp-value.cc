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

#include "runtime/timestamp-value.h"

using namespace std;
using namespace boost;
using namespace boost::posix_time;
using namespace boost::gregorian;

namespace impala {

const char* TimestampValue::LLVM_CLASS_NAME = "class.impala::TimestampValue";
const double TimestampValue::FRACTIONAL = 0.000000001;

time_t to_time_t(ptime t) {
  if (t == not_a_date_time) {
    return 0;
  }
  ptime epoch(date(1970, 1, 1));
  time_duration::sec_type x = (t - epoch).total_seconds();

  return time_t(x);
}

TimestampValue::TimestampValue(const char* str, int len) {
  TimestampParser::Parse(str, len, &date_, &time_of_day_);
}

TimestampValue::TimestampValue(const char* str, int len,
    const DateTimeFormatContext& dt_ctx) {
  TimestampParser::Parse(str, len, dt_ctx, &date_, &time_of_day_);
}

int TimestampValue::Format(const DateTimeFormatContext& dt_ctx, int len, char* buff) {
  return TimestampParser::Format(dt_ctx, date_, time_of_day_, len, buff);
}

ostream& operator<<(ostream& os, const TimestampValue& timestamp_value) {
  return os << timestamp_value.DebugString();
}

}
