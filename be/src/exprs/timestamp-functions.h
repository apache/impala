// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

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

  // Functions to extract parts of the timestamp, return integers.
  static void* Year(Expr* e, TupleRow* row);
  static void* Month(Expr* e, TupleRow* row);
  static void* Day(Expr* e, TupleRow* row);
  static void* DayOfMonth(Expr* e, TupleRow* row);
  static void* WeekOfYear(Expr* e, TupleRow* row);
  static void* Hour(Expr* e, TupleRow* row);
  static void* Minute(Expr* e, TupleRow* row);
  static void* Second(Expr* e, TupleRow* row);

  // Date/time functions.
  static void* Now(Expr* e, TupleRow* row);
  static void* ToDate(Expr* e, TupleRow* row);
  static void* DateDiff(Expr* e, TupleRow* row);
  static void* YearsAdd(Expr* e, TupleRow* row);
  static void* YearsSub(Expr* e, TupleRow* row);
  static void* MonthsAdd(Expr* e, TupleRow* row);
  static void* MonthsSub(Expr* e, TupleRow* row);
  static void* WeeksAdd(Expr* e, TupleRow* row);
  static void* WeeksSub(Expr* e, TupleRow* row);
  static void* DaysAdd(Expr* e, TupleRow* row);
  static void* DaysSub(Expr* e, TupleRow* row);
  static void* HoursAdd(Expr* e, TupleRow* row);
  static void* HoursSub(Expr* e, TupleRow* row);
  static void* MinutesAdd(Expr* e, TupleRow* row);
  static void* MinutesSub(Expr* e, TupleRow* row);
  static void* SecondsAdd(Expr* e, TupleRow* row);
  static void* SecondsSub(Expr* e, TupleRow* row);
  static void* MillisAdd(Expr* e, TupleRow* row);
  static void* MillisSub(Expr* e, TupleRow* row);
  static void* MicrosAdd(Expr* e, TupleRow* row);
  static void* MicrosSub(Expr* e, TupleRow* row);
  static void* NanosAdd(Expr* e, TupleRow* row);
  static void* NanosSub(Expr* e, TupleRow* row);

  // Helper for add/sub functions on the date portion.
  template <class UNIT>
  static void* TimestampDateOp(Expr* e, TupleRow* row, bool is_add);

  // Helper for add/sub functions on the time portion.
  template <class UNIT>
  static void* TimestampTimeOp(Expr* e, TupleRow* row, bool is_add);

  // Convert a timestamp to or from a particular timezone based time.
  static void* FromUtc(Expr* e, TupleRow* row);
  static void* ToUtc(Expr* e, TupleRow* row);

  // Helper function to check date/time format strings.
  // TODO: eventually return format converted from Java to Boost.
  static StringValue* CheckFormat(StringValue* format);

  // Issue a warning for a bad format string.
  static void ReportBadFormat(StringValue* format);

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
