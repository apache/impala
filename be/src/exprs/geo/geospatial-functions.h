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

#pragma once

#include <map>

#include "common/status.h"
#include "udf/udf.h"

namespace impala::geo {

using impala_udf::FunctionContext;
using impala_udf::BooleanVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::DoubleVal;
using impala_udf::StringVal;

class Expr;
class OpcodeRegistry;
struct StringValue;
class TupleRow;

class GeospatialFunctions {
 public:
  // Accessors
  static DoubleVal st_X(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_Y(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MinX(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MinY(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MaxX(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MaxY(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_GeometryType(FunctionContext* ctx, const StringVal& geom);
  static IntVal st_Srid(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_SetSrid(FunctionContext* ctx, const StringVal& geom,
      const IntVal& srid);

  // Constructors
  static StringVal st_Point(FunctionContext* ctx, const DoubleVal& x, const DoubleVal& y);

  // Predicates
  static BooleanVal st_EnvIntersects(
      FunctionContext* ctx, const StringVal& lhs,const StringVal& rhs);
};

}// namespace impala
