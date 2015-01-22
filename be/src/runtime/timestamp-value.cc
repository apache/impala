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
const double TimestampValue::ONE_BILLIONTH = 0.000000001;

TimestampValue::TimestampValue(const char* str, int len) {
  TimestampParser::Parse(str, len, &date_, &time_);
}

TimestampValue::TimestampValue(const char* str, int len,
    const DateTimeFormatContext& dt_ctx) {
  TimestampParser::Parse(str, len, dt_ctx, &date_, &time_);
}

int TimestampValue::Format(const DateTimeFormatContext& dt_ctx, int len, char* buff) {
  return TimestampParser::Format(dt_ctx, date_, time_, len, buff);
}

void TimestampValue::UtcToLocal() {
  DCHECK(HasDateAndTime());
  try {
    tm temp_tm = to_tm(ptime(date_, time_));  // will throw if date/time is invalid
    time_t utc = timegm(&temp_tm);
    if (UNLIKELY(NULL == localtime_r(&utc, &temp_tm))) {
      *this = ptime(not_a_date_time);
      return;
    }
    // Unlikely but a time zone conversion may push the value over the min/max
    // boundary resulting in an exception.
    ptime local = ptime_from_tm(temp_tm);
    // Neither time_t nor struct tm allow fractional seconds so they have to be handled
    // separately.
    local += nanoseconds(time_.fractional_seconds());
    *this = local;
  } catch (std::exception& from_boost) {
    *this = ptime(not_a_date_time);
  }
}

ostream& operator<<(ostream& os, const TimestampValue& timestamp_value) {
  return os << timestamp_value.DebugString();
}

}
