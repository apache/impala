// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_FUNCTION_CALL_H_
#define IMPALA_EXPRS_FUNCTION_CALL_H_

#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/regex.hpp>

#include "exprs/expr.h"

namespace impala {

class TExprNode;

class FunctionCall: public Expr {
 protected:
  friend class Expr;
  friend class StringFunctions;

  FunctionCall(const TExprNode& node);

  virtual std::string DebugString() const;

  // Returns false if the pattern is invalid, true otherwise.
  bool SetRegex(const std::string& pattern);
  const boost::regex* GetRegex() const { return regex_.get(); }

  void SetReplaceStr(const StringValue* str_val);
  const std::string* GetReplaceStr() const { return replace_str_.get(); }

 private:
  // Used in regexp string functions to avoid re-compiling
  // a constant regexp for every function invocation.
  boost::scoped_ptr<boost::regex> regex_;
  // To avoid copying constant replace strings in regexp_replace.
  boost::scoped_ptr<std::string> replace_str_;
};

}

#endif
