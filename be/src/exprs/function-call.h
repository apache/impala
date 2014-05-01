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


#ifndef IMPALA_EXPRS_FUNCTION_CALL_H_
#define IMPALA_EXPRS_FUNCTION_CALL_H_

#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/regex.hpp>

#include "exprs/expr.h"
#include "runtime/timestamp-parse-util.h"

namespace impala {

class TExprNode;
class RuntimeState;

class FunctionCall: public Expr {
 protected:
  friend class DecimalFunctions;
  friend class Expr;
  friend class StringFunctions;
  friend class TimestampFunctions;

  FunctionCall(const TExprNode& node);
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  virtual std::string DebugString() const;

  // Returns false if the pattern is invalid, true otherwise.
  bool SetRegex(const std::string& pattern);
  const boost::regex* GetRegex() const { return regex_.get(); }

  void SetReplaceStr(const StringValue* str_val);
  const std::string* GetReplaceStr() const { return replace_str_.get(); }

  // Returns false when an invalid format is specified. NULL may also be passed to
  // indicate that a generic date/time context is required.
  bool SetDateTimeFormatCtx(StringValue* fmt);

  // Format context returned is intentionally mutable, as dynamic formats may be used
  // which require a place to store parsed tokens.
  DateTimeFormatContext* const GetDateTimeFormatCtx() {
    return date_time_format_ctx_.get();
  }

  int scale() const { return scale_; }

 private:
  // Used in regexp string functions to avoid re-compiling
  // a constant regexp for every function invocation.
  boost::scoped_ptr<boost::regex> regex_;

  // To avoid copying constant replace strings in regexp_replace.
  boost::scoped_ptr<std::string> replace_str_;

  // Used in timestamp date/time parsing with custom formats to avoid
  // parsing for every function invocation.
  boost::scoped_ptr<DateTimeFormatContext> date_time_format_ctx_;

  // Desired result scale for round/truncate.
  int scale_;
};

}

#endif
