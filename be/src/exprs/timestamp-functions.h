// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_TIMESTAMP_FUNCTIONS_H
#define IMPALA_EXPRS_TIMESTAMP_FUNCTIONS_H

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/thread/thread.hpp>

namespace impala {

class Expr;
class OpcodeRegistry;
class TupleRow;

class TimestampFunctions {
 public:
  // Return the unix time_t, seconds from 1970
  static void* Unix(Expr* e, TupleRow* row);

  // Functions to extract parts of the timestamp, return integers.
  static void* Year(Expr* e, TupleRow* row);
  static void* Month(Expr* e, TupleRow* row);
  static void* Day(Expr* e, TupleRow* row);
  static void* DayOfMonth(Expr* e, TupleRow* row);
  static void* WeekOfYear(Expr* e, TupleRow* row);
  static void* Hour(Expr* e, TupleRow* row);
  static void* Minute(Expr* e, TupleRow* row);
  static void* Second(Expr* e, TupleRow* row);

  // Date functions.
  static void* ToDate(Expr* e, TupleRow* row);
  static void* DateAdd(Expr* e, TupleRow* row);
  static void* DateSub(Expr* e, TupleRow* row);
  static void* DateDiff(Expr* e, TupleRow* row);

  // Convert a timestamp to or from a particular timezone based time.
  static void* FromUtc(Expr* e, TupleRow* row);
  static void* ToUtc(Expr* e, TupleRow* row);
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
