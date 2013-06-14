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

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/date_time/local_time/local_time.hpp>

#include "exprs/timestamp-functions.h"
#include "exprs/expr.h"
#include "runtime/tuple-row.h"
#include "runtime/timestamp-value.h"
#include "util/path-builder.h"
#include "runtime/string-value.inline.h"

#define TIMEZONE_DATABASE "be/files/date_time_zonespec.csv"

using namespace boost;
using namespace boost::posix_time;
using namespace boost::local_time;
using namespace boost::gregorian;
using namespace std;

namespace impala {

local_time::tz_database TimezoneDatabase::tz_database_;
vector<string> TimezoneDatabase::tz_region_list_;

void* TimestampFunctions::FromUnix(Expr* e, TupleRow* row) {
  DCHECK_LE(e->GetNumChildren(), 2);
  DCHECK_NE(e->GetNumChildren(), 0);
  
  Expr* op = e->children()[0];
  uint32_t* intp = reinterpret_cast<uint32_t*>(op->GetValue(row));
  if (intp == NULL) return NULL;
  TimestampValue t(boost::posix_time::from_time_t(*intp));

  // If there is a second argument then it's a format statement.
  // Otherwise the string is in the default format.
  if (e->GetNumChildren() == 2) {
    Expr* fmtop = e->children()[1];
    StringValue* format = reinterpret_cast<StringValue*>(fmtop->GetValue(row));
    if (CheckFormat(format) == NULL) return NULL;
    if (format->len == 10) {
      // If the format is yyyy-MM-dd then set the time to invalid.
      t.set_time(time_duration(not_a_date_time));
    } 
  }

  e->result_.SetStringVal(lexical_cast<string>(t));
  return &e->result_.string_val;
}

void* TimestampFunctions::Unix(Expr* e, TupleRow* row) {
  DCHECK_LE(e->GetNumChildren(), 2);
  TimestampValue* tv;
  if (e->GetNumChildren() == 0) {
    // Expr::Prepare put the current timestamp here.
    tv = &e->result_.timestamp_val;
  } else if (e->GetNumChildren() == 1) {
    Expr* op = e->children()[0];
    tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  } else {
    Expr* op = e->children()[0];
    StringValue* value = reinterpret_cast<StringValue*>(op->GetValue(row));
    Expr* fmtop = e->children()[1];
    StringValue* format = reinterpret_cast<StringValue*>(fmtop->GetValue(row));

    if (value == NULL || format == NULL || CheckFormat(format) == NULL) return NULL;

    // Trim the value of blank space to be more user friendly.
    StringValue tvalue = value->Trim();

    // Emulate hive by truncating the value to be the same length as the format
    // (i.e. allow extra text beyond expected format).
    if (tvalue.len > format->len) {
      tvalue = tvalue.Substring(0, format->len);
    }

    // TimestampValue will accept just a date when format specifies date and
    // time, so check that the value is at least as long as the format.
    if (tvalue.len < format->len) {
      string fmt(format->ptr, format->len);
      string str(tvalue.ptr, tvalue.len);
      LOG(WARNING) << "Timestamp: " << str << " does not match format: " << fmt;
      return NULL;
    }

    TimestampValue val(tvalue.ptr, tvalue.len);
    tv = &val;
  }

  if (tv == NULL || tv->date().is_special()) return NULL;

  ptime temp;
  tv->ToPtime(&temp);
  e->result_.int_val = static_cast<int32_t>(to_time_t(temp));
  return &e->result_.int_val;
}

// TODO: accept Java data/time format strings:
// http://docs.oracle.com/javase/1.4.2/docs/api/java/text/SimpleDateFormat.html
// Convert them to boost format strings.
StringValue* TimestampFunctions::CheckFormat(StringValue* format) {
  if(format == NULL) return NULL;

  // For now the format  must be of the form: yyyy-MM-dd HH:mm:ss
  // where the time part is optional.
  switch(format->len) {
    case 10:
      if (strncmp(format->ptr, "yyyy-MM-dd", 10) == 0) return format;
      break;
    case 19:
      if (strncmp(format->ptr, "yyyy-MM-dd HH:mm:ss", 19) == 0) return format;
      break;
    default: 
      break;
  }
  ReportBadFormat(format);
  return NULL;
}

void TimestampFunctions::ReportBadFormat(StringValue* format) {
  string format_str(format->ptr, format->len);
  LOG(WARNING) << "Bad date/time conversion format: " << format_str 
               << " Format must be: 'yyyy-MM-dd[ HH:mm:ss]'";
}

void* TimestampFunctions::Year(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  // If the value has been set to not_a_date_time then it will be marked special
  // therefore there is no valid date component and this function returns NULL.
  if (tv->date().is_special()) return NULL;
  e->result_.int_val = tv->date().year();
  return &e->result_.int_val;
}

void* TimestampFunctions::Month(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->date().is_special()) return NULL;
  e->result_.int_val = tv->date().month();
  return &e->result_.int_val;
}

void* TimestampFunctions::DayOfWeek(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->date().is_special()) return NULL;
  // Sql has the result in [1,7] where 1 = Sunday.  Boost has 0 = Sunday.
  e->result_.int_val = tv->date().day_of_week() + 1;
  DCHECK_GE(e->result_.int_val, 1);
  DCHECK_LE(e->result_.int_val, 7);
  return &e->result_.int_val;
}

void* TimestampFunctions::DayOfMonth(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->date().is_special()) return NULL;
  e->result_.int_val = tv->date().day();
  return &e->result_.int_val;
}

void* TimestampFunctions::DayOfYear(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->date().is_special()) return NULL;
  e->result_.int_val = tv->date().day_of_year();
  return &e->result_.int_val;
}

void* TimestampFunctions::WeekOfYear(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->date().is_special()) return NULL;
  e->result_.int_val = tv->date().week_number();
  return &e->result_.int_val;
}

void* TimestampFunctions::Hour(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->time_of_day().is_special()) return NULL;

  e->result_.int_val = tv->time_of_day().hours();
  return &e->result_.int_val;
}

void* TimestampFunctions::Minute(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->time_of_day().is_special()) return NULL;

  e->result_.int_val = tv->time_of_day().minutes();
  return &e->result_.int_val;
}

void* TimestampFunctions::Second(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  if (tv->time_of_day().is_special()) return NULL;

  e->result_.int_val = tv->time_of_day().seconds();
  return &e->result_.int_val;
}

void* TimestampFunctions::Now(Expr* e, TupleRow* row) {
  // Make sure FunctionCall::Prepare() properly set the timestamp value.
  DCHECK(!e->result_.timestamp_val.date().is_special());
  return &e->result_.timestamp_val;
}

void* TimestampFunctions::ToDate(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  Expr* op = e->children()[0];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op->GetValue(row));
  if (tv == NULL) return NULL;

  string result = to_iso_extended_string(tv->date());
  e->result_.SetStringVal(result);
  return &e->result_.string_val;
}

template <bool ISADD, class VALTYPE, class UNIT>
void* TimestampFunctions::DateAddSub(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op1->GetValue(row));
  VALTYPE* count = reinterpret_cast<VALTYPE*>(op2->GetValue(row));
  if (tv == NULL || count == NULL) return NULL;

  if (tv->date().is_special()) return NULL;

  UNIT unit(*count);
  TimestampValue
      value((ISADD ? tv->date() + unit : tv->date() - unit), tv->time_of_day());
  e->result_.timestamp_val = value;

  return &e->result_.timestamp_val;
}

template <bool ISADD, class VALTYPE, class UNIT>
void* TimestampFunctions::TimeAddSub(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op1->GetValue(row));
  VALTYPE* count = reinterpret_cast<VALTYPE*>(op2->GetValue(row));
  if (tv == NULL || count == NULL) return NULL;

  if (tv->date().is_special()) return NULL;

  UNIT unit(*count);
  ptime p(tv->date(), tv->time_of_day());
  TimestampValue value(ISADD ? p + unit : p - unit);
  e->result_.timestamp_val = value;

  return &e->result_.timestamp_val;
}

void* TimestampFunctions::DateDiff(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  TimestampValue* tv1 = reinterpret_cast<TimestampValue*>(op1->GetValue(row));
  TimestampValue* tv2 = reinterpret_cast<TimestampValue*>(op2->GetValue(row));
  if (tv1 == NULL || tv2 == NULL) return NULL;

  if (tv1->date().is_special()) return NULL;
  if (tv2->date().is_special()) return NULL;

  e->result_.int_val = (tv1->date() - tv2->date()).days();
  return &e->result_.int_val;
}

void* TimestampFunctions::FromUtc(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op1->GetValue(row));
  StringValue* tz = reinterpret_cast<StringValue*>(op2->GetValue(row));
  if (tv == NULL || tz == NULL) return NULL;

  if (tv->NotADateTime()) return NULL;

  time_zone_ptr timezone = TimezoneDatabase::FindTimezone(tz->DebugString());
  // This should raise some sort of error or at least null. Hive just ignores it.
  if (timezone == NULL) {
    LOG(ERROR) << "Unknown timezone '" << *tz << "'" << endl;
    e->result_.timestamp_val = *tv;
    return &e->result_.timestamp_val;
  }
  ptime temp;
  tv->ToPtime(&temp);
  local_date_time lt(temp, timezone);
  e->result_.timestamp_val = lt.local_time();
  return &e->result_.timestamp_val;
}

void* TimestampFunctions::ToUtc(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  Expr* op1 = e->children()[0];
  Expr* op2 = e->children()[1];
  TimestampValue* tv = reinterpret_cast<TimestampValue*>(op1->GetValue(row));
  StringValue* tz = reinterpret_cast<StringValue*>(op2->GetValue(row));
  if (tv == NULL || tz == NULL) return NULL;

  if (tv->NotADateTime()) return NULL;

  time_zone_ptr timezone = TimezoneDatabase::FindTimezone(tz->DebugString());
  // This should raise some sort of error or at least null. Hive just ignores it.
  if (timezone == NULL) {
    LOG(ERROR) << "Unknown timezone '" << *tz << "'" << endl;
    e->result_.timestamp_val = *tv;
    return &e->result_.timestamp_val;
  }
  local_date_time lt(tv->date(), tv->time_of_day(),
                     timezone, local_date_time::NOT_DATE_TIME_ON_ERROR);
  e->result_.timestamp_val = TimestampValue(lt.utc_time());
  return &e->result_.timestamp_val;
}

TimezoneDatabase::TimezoneDatabase() {
  // Create a temporary file and write the timezone information.  The boost
  // interface only loads this format from a file.  We don't want to raise
  // an error here since this is done when the backend is created and this
  // information might not actually get used by any queries.
  char filestr[] = "/tmp/impala.tzdb.XXXXXXX";
  FILE* file;
  int fd;
  if ((fd = mkstemp(filestr)) == -1) {
    LOG(ERROR) << "Could not create temporary timezone file: " << filestr;
    return;
  }
  if ((file = fopen(filestr, "w")) == NULL) {
    unlink(filestr);
    close(fd);
    LOG(ERROR) << "Could not open temporary timezone file: " << filestr;
    return;
  }
  if (fputs(TIMEZONE_DATABASE_STR, file) == EOF) {
    unlink(filestr);
    close(fd);
    fclose(file);
    LOG(ERROR) << "Could not load temporary timezone file: " << filestr;
    return;
  }
  fclose(file);
  tz_database_.load_from_file(string(filestr));
  tz_region_list_ = tz_database_.region_list();
  unlink(filestr);
  close(fd);
}

TimezoneDatabase::~TimezoneDatabase() { }

time_zone_ptr TimezoneDatabase::FindTimezone(const string& tz) {
  // See if they specified a zone id
  if (tz.find_first_of('/') != string::npos)
    return  tz_database_.time_zone_from_region(tz);
  for (vector<string>::const_iterator iter = tz_region_list_.begin();
       iter != tz_region_list_.end(); ++iter) {
    time_zone_ptr tzp = tz_database_.time_zone_from_region(*iter);
    DCHECK(tzp != NULL);
    if (tzp->dst_zone_abbrev() == tz)
      return tzp;
    if (tzp->std_zone_abbrev() == tz)
      return tzp;
    if (tzp->dst_zone_name() == tz)
      return tzp;
    if (tzp->std_zone_name() == tz)
      return tzp;
  }
  return time_zone_ptr();

}

// Explicit template instantiation is required for proper linking. These functions
// are only indirectly called via a function pointer provided by the opcode registry
// which does not trigger implicit template instantiation.
// Must be kept in sync with common/function-registry/impala_functions.py.

template void*
TimestampFunctions::DateAddSub<true, int32_t, years>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<true, int64_t, years>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<false, int32_t, years>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<false, int64_t, years>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<true, int32_t, months>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<true, int64_t, months>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<false, int32_t, months>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<false, int64_t, months>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<true, int32_t, weeks>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<true, int64_t, weeks>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<false, int32_t, weeks>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<false, int64_t, weeks>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<true, int32_t, days>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<true, int64_t, days>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<false, int32_t, days>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::DateAddSub<false, int64_t, days>(Expr* e, TupleRow* row);

template void*
TimestampFunctions::TimeAddSub<true, int32_t, hours>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<true, int64_t, hours>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<false, int32_t, hours>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<false, int64_t, hours>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<true, int32_t, minutes>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<true, int64_t, minutes>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<false, int32_t, minutes>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<false, int64_t, minutes>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<true, int32_t, seconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<true, int64_t, seconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<false, int32_t, seconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<false, int64_t, seconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<true, int32_t, milliseconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<true, int64_t, milliseconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<false, int32_t, milliseconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<false, int64_t, milliseconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<true, int32_t, microseconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<true, int64_t, microseconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<false, int32_t, microseconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<false, int64_t, microseconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<true, int32_t, nanoseconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<true, int64_t, nanoseconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<false, int32_t, nanoseconds>(Expr* e, TupleRow* row);
template void*
TimestampFunctions::TimeAddSub<false, int64_t, nanoseconds>(Expr* e, TupleRow* row);

}
