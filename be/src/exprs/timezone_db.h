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


#ifndef IMPALA_EXPRS_TIMEZONE_DB_H
#define IMPALA_EXPRS_TIMEZONE_DB_H

#include <boost/date_time/compiler_config.hpp>
#include <boost/date_time/local_time/local_time_types.hpp>
#include <boost/date_time/local_time/tz_database.hpp>

#include "common/status.h"
#include "runtime/timestamp-value.h"

namespace impala {

/// Functions to load and access the timestamp database.
class TimezoneDatabase {
 public:
  /// Set up the static timezone database.
  static Status Initialize() WARN_UNUSED_RESULT;

  /// Converts the name of a timezone to a boost timezone object. In some cases, the
  /// timestamp is required to determine the timezone because occasionally timezone
  /// rules change. The flag "tv_in_utc" specifies whether the given timestamp value is
  /// in the UTC timezone. If the flag is false, then the timestamp value is assumed to
  /// be in the 'tz' timezone.
  static boost::local_time::time_zone_ptr FindTimezone(const std::string& tz,
      const TimestampValue& tv, bool tv_in_utc);

  /// Moscow timezone UTC+3 with DST, for use before March 27, 2011.
  static const boost::local_time::time_zone_ptr TIMEZONE_MSK_PRE_2011_DST;

  /// Moscow timezone UTC+4 with no DST, for use before October 26, 2014.
  static const boost::local_time::time_zone_ptr TIMEZONE_MSK_PRE_2014;

 private:
  static const char* TIMEZONE_DATABASE_STR;
  static boost::local_time::tz_database tz_database_;
  static std::vector<std::string> tz_region_list_;
};

} // namespace impala

#endif
