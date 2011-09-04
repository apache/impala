// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_LIKE_PREDICATE_H_
#define IMPALA_EXPRS_LIKE_PREDICATE_H_

#include <string>
#include <boost/scoped_ptr.hpp> 
#include <boost/regex.hpp> 

#include "exprs/predicate.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

class LikePredicate: public Predicate {
 protected:
  friend class Expr;

  LikePredicate(const TExprNode& node);

  virtual Status Prepare(RuntimeState* state);

 private:
  const TExprOperator::type op_;
  char escape_char_;
  std::string substring_;
  boost::scoped_ptr<boost::regex> regex_;

  // Convert a LIKE pattern (with embedded % and _) into the corresponding
  // regular expression pattern. Escaped chars are copied verbatim.
  void ConvertLikePattern(const StringValue* pattern, std::string* re_pattern) const;

  static void* ConstantSubstringFn(Expr* e, TupleRow* row);
  static void* ConstantRegexFn(Expr* e, TupleRow* row);
  static void* LikeFn(Expr* e, TupleRow* row);
  static void* RegexFn(Expr* e, TupleRow* row);
  static void* RegexMatch(Expr* e, TupleRow* row, bool is_like_pattern);
};

}

#endif
