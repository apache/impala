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


#ifndef IMPALA_EXPRS_IN_PREDICATE_H_
#define IMPALA_EXPRS_IN_PREDICATE_H_

#include <string>
#include "exprs/predicate.h"
#include "udf/udf.h"

namespace impala {

class InPredicate : public Predicate {
 public:
  static impala_udf::BooleanVal In(
      impala_udf::FunctionContext* context, const impala_udf::BooleanVal& val,
      int num_args, const impala_udf::BooleanVal* args);

  static impala_udf::BooleanVal NotIn(
      impala_udf::FunctionContext* context, const impala_udf::BooleanVal& val,
      int num_args, const impala_udf::BooleanVal* args);

  static impala_udf::BooleanVal In(
      impala_udf::FunctionContext* context, const impala_udf::TinyIntVal& val,
      int num_args, const impala_udf::TinyIntVal* args);

  static impala_udf::BooleanVal NotIn(
      impala_udf::FunctionContext* context, const impala_udf::TinyIntVal& val,
      int num_args, const impala_udf::TinyIntVal* args);

  static impala_udf::BooleanVal In(
      impala_udf::FunctionContext* context, const impala_udf::SmallIntVal& val,
      int num_args, const impala_udf::SmallIntVal* args);

  static impala_udf::BooleanVal NotIn(
      impala_udf::FunctionContext* context, const impala_udf::SmallIntVal& val,
      int num_args, const impala_udf::SmallIntVal* args);

  static impala_udf::BooleanVal In(
      impala_udf::FunctionContext* context, const impala_udf::IntVal& val,
      int num_args, const impala_udf::IntVal* args);

  static impala_udf::BooleanVal NotIn(
      impala_udf::FunctionContext* context, const impala_udf::IntVal& val,
      int num_args, const impala_udf::IntVal* args);

  static impala_udf::BooleanVal In(
      impala_udf::FunctionContext* context, const impala_udf::BigIntVal& val,
      int num_args, const impala_udf::BigIntVal* args);

  static impala_udf::BooleanVal NotIn(
      impala_udf::FunctionContext* context, const impala_udf::BigIntVal& val,
      int num_args, const impala_udf::BigIntVal* args);

  static impala_udf::BooleanVal In(
      impala_udf::FunctionContext* context, const impala_udf::FloatVal& val,
      int num_args, const impala_udf::FloatVal* args);

  static impala_udf::BooleanVal NotIn(
      impala_udf::FunctionContext* context, const impala_udf::FloatVal& val,
      int num_args, const impala_udf::FloatVal* args);

  static impala_udf::BooleanVal In(
      impala_udf::FunctionContext* context, const impala_udf::DoubleVal& val,
      int num_args, const impala_udf::DoubleVal* args);

  static impala_udf::BooleanVal NotIn(
      impala_udf::FunctionContext* context, const impala_udf::DoubleVal& val,
      int num_args, const impala_udf::DoubleVal* args);

  static impala_udf::BooleanVal In(
      impala_udf::FunctionContext* context, const impala_udf::StringVal& val,
      int num_args, const impala_udf::StringVal* args);

  static impala_udf::BooleanVal NotIn(
      impala_udf::FunctionContext* context, const impala_udf::StringVal& val,
      int num_args, const impala_udf::StringVal* args);

  static impala_udf::BooleanVal In(
      impala_udf::FunctionContext* context, const impala_udf::TimestampVal& val,
      int num_args, const impala_udf::TimestampVal* args);

  static impala_udf::BooleanVal NotIn(
      impala_udf::FunctionContext* context, const impala_udf::TimestampVal& val,
      int num_args, const impala_udf::TimestampVal* args);

  static impala_udf::BooleanVal In(
      impala_udf::FunctionContext* context, const impala_udf::DecimalVal& val,
      int num_args, const impala_udf::DecimalVal* args);

  static impala_udf::BooleanVal NotIn(
      impala_udf::FunctionContext* context, const impala_udf::DecimalVal& val,
      int num_args, const impala_udf::DecimalVal* args);

 private:
  template<typename T, bool not_in>
  static inline impala_udf::BooleanVal TemplatedIn(
      impala_udf::FunctionContext* context, const T& val, int num_args, const T* args);
};

}

#endif
