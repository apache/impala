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

#include <algorithm>
#include <regex>
#include <sstream>

#include "util/time.h"
#include "testutil/gtest-util.h"

#include "exprs/timezone_db.h"
#include "common/names.h"

using namespace std;

namespace impala {
  // Basic tests for the time formatting APIs in util/time.h.
TEST(TimeTest, Basic) {
  const Timezone& utc_tz = cctz::utc_time_zone();
  EXPECT_EQ("1970-01-01 00:00:00", ToUtcStringFromUnix(0));
  EXPECT_EQ("1970-01-01 00:00:00.000", ToUtcStringFromUnixMillis(0));
  EXPECT_EQ("1970-01-01 00:00:00.001", ToUtcStringFromUnixMillis(1));
  EXPECT_EQ("1970-01-01 00:00:00", ToUtcStringFromUnixMillis(0, TimePrecision::Second));
  EXPECT_EQ("1970-01-01 00:00:00.000000", ToUtcStringFromUnixMicros(0));
  EXPECT_EQ("1970-01-01 00:00:00.000001", ToUtcStringFromUnixMicros(1));
  EXPECT_EQ("1970-01-01 00:00:00.000", ToUtcStringFromUnixMicros(0,
      TimePrecision::Millisecond));
  EXPECT_EQ("1970-01-01 00:00:00", ToUtcStringFromUnixMicros(0, TimePrecision::Second));
  EXPECT_EQ("1970-01-01 00:00:00.000000", ToStringFromUnixMicros(0, utc_tz));
  EXPECT_EQ("1970-01-01 00:00:00.000001", ToStringFromUnixMicros(1, utc_tz));
  EXPECT_EQ("1970-01-01 00:00:00.000", ToStringFromUnixMicros(0, utc_tz,
      TimePrecision::Millisecond));
  EXPECT_EQ("1970-01-01 00:00:00.000", ToStringFromUnixMicros(999, utc_tz,
      TimePrecision::Millisecond));
  EXPECT_EQ("1970-01-01 00:00:00", ToStringFromUnixMicros(0, utc_tz,
      TimePrecision::Second));
  EXPECT_EQ("1970-01-01 00:00:00", ToStringFromUnixMicros(999999, utc_tz,
      TimePrecision::Second));

  EXPECT_EQ("1970-01-01 00:01:00", ToUtcStringFromUnix(60));
  EXPECT_EQ("1970-01-01 01:00:00", ToUtcStringFromUnix(3600));
  // Check we are handling negative times - time before the Unix epoch
  EXPECT_EQ("1969-12-31 23:59:59", ToUtcStringFromUnix(-1));

  // The earliest date-time that can be represented with the high-resolution
  // chrono::system_clock is 1677-09-21 00:12:44 UTC, which is
  // -9223372036854775808 (INT64_MIN) nanoseconds before the Unix epoch.
  EXPECT_EQ("1677-09-21 00:12:44",
      ToUtcStringFromUnix(INT64_MIN / NANOS_PER_SEC));
  EXPECT_EQ("1677-09-21 00:12:43.146",
      ToUtcStringFromUnixMillis(INT64_MIN / NANOS_PER_MICRO / MICROS_PER_MILLI));
  EXPECT_EQ("1677-09-21 00:12:43.145225",
      ToUtcStringFromUnixMicros(INT64_MIN / NANOS_PER_MICRO));
  EXPECT_EQ("1677-09-21 00:12:43.145225",
      ToStringFromUnixMicros(INT64_MIN / NANOS_PER_MICRO, utc_tz));

  // The latest date-time that can be represented with the high-resoliution
  // chrono::system_clock is 2262-04-11 23:47:16 UTC, which is
  // 9223372036854775807 (INT64_MAX) nanoseconds since the Unix epoch.
  EXPECT_EQ("2262-04-11 23:47:16",
      ToUtcStringFromUnix(INT64_MAX / NANOS_PER_SEC));
  EXPECT_EQ("2262-04-11 23:47:16.854",
      ToUtcStringFromUnixMillis(INT64_MAX / NANOS_PER_MICRO / MICROS_PER_MILLI));
  EXPECT_EQ("2262-04-11 23:47:16.854775",
      ToUtcStringFromUnixMicros(INT64_MAX / NANOS_PER_MICRO));
  EXPECT_EQ("2262-04-11 23:47:16.854775",
      ToStringFromUnixMicros(INT64_MAX / NANOS_PER_MICRO, utc_tz));

  EXPECT_EQ("1969-12-31 23:59:59.999", ToUtcStringFromUnixMillis(-1));
  EXPECT_EQ("1969-12-31 23:59:59.001", ToUtcStringFromUnixMillis(-999));
  EXPECT_EQ("1969-12-31 23:59:59.000", ToUtcStringFromUnixMillis(-1000));
  EXPECT_EQ("1969-12-31 23:59:58.001", ToUtcStringFromUnixMillis(-1999));
  EXPECT_EQ("1969-12-31 23:59:58.000", ToUtcStringFromUnixMillis(-2000));
  EXPECT_EQ("1969-12-31 23:59:57.999", ToUtcStringFromUnixMillis(-2001));

  EXPECT_EQ("1969-12-31 23:59:59.999999", ToUtcStringFromUnixMicros(-1));
  EXPECT_EQ("1969-12-31 23:59:59.000001", ToUtcStringFromUnixMicros(-999999));
  EXPECT_EQ("1969-12-31 23:59:59.000000", ToUtcStringFromUnixMicros(-1000000));
  EXPECT_EQ("1969-12-31 23:59:58.000001", ToUtcStringFromUnixMicros(-1999999));
  EXPECT_EQ("1969-12-31 23:59:57.999999", ToUtcStringFromUnixMicros(-2000001));
  EXPECT_EQ("1969-12-31 23:59:59.000000", ToStringFromUnixMicros(-1000000, utc_tz));
  EXPECT_EQ("1969-12-31 23:59:58.000001", ToStringFromUnixMicros(-1999999, utc_tz));
  EXPECT_EQ("1969-12-31 23:59:57.999999", ToStringFromUnixMicros(-2000001, utc_tz));

  // Unix time does not represent leap seconds. Test continuous roll-over of
  // Unix time after 1998-12-31 23:59:59
  EXPECT_EQ("1998-12-31 23:59:59", ToUtcStringFromUnix(915148799));
  EXPECT_EQ("1999-01-01 00:00:00", ToUtcStringFromUnix(915148800));

  // Check that for the same Unix time, our output string agrees with the output
  // of strftime(3).
  int64_t now_s = UnixMillis() / MILLIS_PER_SEC;
  char now_buf[256];
  strftime(now_buf, sizeof(now_buf), "%F %T", localtime(static_cast<time_t *>(&now_s)));
  EXPECT_EQ(string(now_buf), ToStringFromUnix(now_s)) << "now_s=" << now_s;

  strftime(now_buf, sizeof(now_buf), "%F %T", gmtime(static_cast<time_t *>(&now_s)));
  EXPECT_EQ(string(now_buf), ToUtcStringFromUnix(now_s)) << "now_s=" << now_s;

  // Check zero-padding of date-time string's fractional second part if input
  // time's resolution is less than that requested by the caller.
  smatch sm; // Place holder to be passed to regex_search() below.
  string s1 = ToStringFromUnix(now_s, TimePrecision::Millisecond);
  string s2 = ToUtcStringFromUnix(now_s, TimePrecision::Millisecond);
  EXPECT_TRUE(regex_search(s1, sm, regex(R"(\.(000)$)")));
  EXPECT_TRUE(regex_search(s2, sm, regex(R"(\.(000)$)")));

  int64_t now_ms = UnixMillis();
  s1 = ToStringFromUnixMillis(now_ms, TimePrecision::Microsecond);
  s2 = ToUtcStringFromUnixMillis(now_ms, TimePrecision::Microsecond);
  EXPECT_TRUE(regex_search(s1, sm, regex(R"(\.\d{3}(000)$)")));
  EXPECT_TRUE(regex_search(s2, sm, regex(R"(\.\d{3}(000)$)")));

  int64_t now_us = UnixMicros();
  s1 = ToStringFromUnixMicros(now_us, TimePrecision::Nanosecond);
  s2 = ToUtcStringFromUnixMicros(now_us, TimePrecision::Nanosecond);
  string s3 = ToStringFromUnixMicros(now_us, utc_tz, TimePrecision::Nanosecond);
  EXPECT_TRUE(regex_search(s1, sm, regex(R"(\.\d{6}(000)$)")));
  EXPECT_TRUE(regex_search(s2, sm, regex(R"(\.\d{6}(000)$)")));
  EXPECT_TRUE(regex_search(s3, sm, regex(R"(\.\d{6}(000)$)")));
} // TEST
} // namespace impala

