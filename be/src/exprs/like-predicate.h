// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_LIKE_PREDICATE_H_
#define IMPALA_EXPRS_LIKE_PREDICATE_H_

#include <string>
#include <boost/scoped_ptr.hpp> 
#include <boost/regex.hpp> 

#include "exprs/predicate.h"
#include "gen-cpp/Exprs_types.h"
#include "runtime/string-search.h"

namespace impala {

class LikePredicate: public Predicate {
 protected:
  friend class Expr;
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  LikePredicate(const TExprNode& node);

 private:
  friend class OpcodeRegistry;

  char escape_char_;
  std::string search_string_;
  StringValue search_string_sv_;
  StringSearch substring_pattern_;
  boost::scoped_ptr<boost::regex> regex_;

  // Convert a LIKE pattern (with embedded % and _) into the corresponding
  // regular expression pattern. Escaped chars are copied verbatim.
  void ConvertLikePattern(const StringValue* pattern, std::string* re_pattern) const;

  // Handling of like predicates that map to strstr
  static void* ConstantSubstringFn(Expr* e, TupleRow* row);

  // Handling of like predicates that can be implemented using strncmp
  static void* ConstantStartsWithFn(Expr* e, TupleRow* row);
  
  // Handling of like predicates that can be implemented using strncmp
  static void* ConstantEndsWithFn(Expr* e, TupleRow* row);

  static void* ConstantRegexFn(Expr* e, TupleRow* row);
  static void* LikeFn(Expr* e, TupleRow* row);
  static void* RegexFn(Expr* e, TupleRow* row);
  static void* RegexMatch(Expr* e, TupleRow* row, bool is_like_pattern);
};

}

#endif
