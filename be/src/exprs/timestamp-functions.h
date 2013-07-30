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


#ifndef IMPALA_EXPRS_TIMESTAMP_FUNCTIONS_H
#define IMPALA_EXPRS_TIMESTAMP_FUNCTIONS_H

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/thread/thread.hpp>
#include "runtime/string-value.h"

namespace impala {

class Expr;
class OpcodeRegistry;
class TupleRow;

class TimestampFunctions {
 public:
  // Return the unix time_t, seconds from 1970
  // With 0 argments, returns the current time.
  // With 1 arument, converts it to a unix time_t
  // With 2 aruments, the second argument is the format of the timestamp string.
  static void* Unix(Expr* e, TupleRow* row);

  // Return a timestamp string from a unix time_t
  // Optional second argument is the format of the string.
  static void* FromUnix(Expr* e, TupleRow* row);

  // Convert a timestamp to or from a particular timezone based time.
  static void* FromUtc(Expr* e, TupleRow* row);
  static void* ToUtc(Expr* e, TupleRow* row);

  // Returns the day's name as a string (e.g. 'Saturday').
  static void* DayName(Expr* e, TupleRow* row);

  // Functions to extract parts of the timestamp, return integers.
  static void* Year(Expr* e, TupleRow* row);
  static void* Month(Expr* e, TupleRow* row);
  static void* DayOfWeek(Expr* e, TupleRow* row);
  static void* DayOfMonth(Expr* e, TupleRow* row);
  static void* DayOfYear(Expr* e, TupleRow* row);
  static void* WeekOfYear(Expr* e, TupleRow* row);
  static void* Hour(Expr* e, TupleRow* row);
  static void* Minute(Expr* e, TupleRow* row);
  static void* Second(Expr* e, TupleRow* row);

  // Date/time functions.
  static void* Now(Expr* e, TupleRow* row);
  static void* ToDate(Expr* e, TupleRow* row);
  static void* DateDiff(Expr* e, TupleRow* row);

  // Add/sub functions on the date portion.
  template <bool ISADD, class VALTYPE, class UNIT>
  static void* DateAddSub(Expr* e, TupleRow* row);

  // Add/sub functions on the time portion.
  template <bool ISADD, class VALTYPE, class UNIT>
  static void* TimeAddSub(Expr* e, TupleRow* row);

  // Helper function to check date/time format strings.
  // TODO: eventually return format converted from Java to Boost.
  static StringValue* CheckFormat(StringValue* format);

  // Issue a warning for a bad format string.
  static void ReportBadFormat(StringValue* format);

 private:
  // Static result values for DayName() function.
  static const StringValue MONDAY;
  static const StringValue TUESDAY;
  static const StringValue WEDNESDAY;
  static const StringValue THURSDAY;
  static const StringValue FRIDAY;
  static const StringValue SATURDAY;
  static const StringValue SUNDAY;
};

// Functions to load and access the timestamp database.
class TimezoneDatabase {
 public:
   TimezoneDatabase();
   ~TimezoneDatabase();

  static boost::local_time::time_zone_ptr FindTimezone(const std::string& tz);

 private:
  static const char* TIMEZONE_DATABASE_STR;
  static boost::local_time::tz_database tz_database_;
  static std::vector<std::string> tz_region_list_;
};

}

#endif
