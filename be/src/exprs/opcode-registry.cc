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

#include "exprs/opcode-registry.h"
#include "exprs/aggregate-functions.h"

using namespace boost;
using namespace std;

namespace impala {

OpcodeRegistry* OpcodeRegistry::instance_ = NULL;
mutex OpcodeRegistry::instance_lock_;

static void InitAggregateBuiltins(OpcodeRegistry::AggregateBuiltins* fns);

OpcodeRegistry::OpcodeRegistry() {
  int num_opcodes = static_cast<int>(TExprOpcode::LAST_OPCODE);
  scalar_builtins_.resize(num_opcodes);
  symbols_.resize(num_opcodes);
  Init();
  InitAggregateBuiltins(&aggregate_builtins_);
}

// TODO: Older versions of gcc have problems resolving template types so use this
// nasty macro to help it.
// This needs to be used if the function is templated.
#define COERCE_CAST1(FUNC, TYPE) ((void*)(void(*)(FunctionContext*, TYPE*))FUNC<TYPE>)
#define COERCE_CAST2_SAME(FUNC, TYPE)\
    ((void*)(void(*)(FunctionContext*, const TYPE&, TYPE*))FUNC<TYPE>)
#define COERCE_CAST2(FUNC, TYPE1, TYPE2)\
    ((void*)(void(*)(FunctionContext*, const TYPE1&, TYPE2*))FUNC<TYPE1, TYPE2>)

static OpcodeRegistry::AggFnDescriptor NullDesc() {
  return OpcodeRegistry::AggFnDescriptor((void*)AggregateFunctions::InitNull);
}

static OpcodeRegistry::AggFnDescriptor CountStarDesc() {
  return OpcodeRegistry::AggFnDescriptor(
      COERCE_CAST1(AggregateFunctions::InitZero, BigIntVal),
      (void*)AggregateFunctions::CountStarUpdate,
      // TODO: this should be Sum<BigInt,BigInt> but there needs to be a planner
      // change. The planner currently manually splits count to count/sum.
      // For count(distinct col1, col2), the plan relies on count on this step.
      (void*)AggregateFunctions::CountStarUpdate);
}

static OpcodeRegistry::AggFnDescriptor CountDesc() {
  return OpcodeRegistry::AggFnDescriptor(
      COERCE_CAST1(AggregateFunctions::InitZero, BigIntVal),
      (void*)AggregateFunctions::CountUpdate,
      (void*)AggregateFunctions::CountUpdate);
}

template<typename INPUT, typename OUTPUT>
static OpcodeRegistry::AggFnDescriptor SumDesc() {
  return OpcodeRegistry::AggFnDescriptor(
      (void*)AggregateFunctions::InitNull,
      COERCE_CAST2(AggregateFunctions::Sum, INPUT, OUTPUT),
      COERCE_CAST2(AggregateFunctions::Sum, OUTPUT, OUTPUT));
}

template<typename T>
static OpcodeRegistry::AggFnDescriptor MinDesc() {
  return OpcodeRegistry::AggFnDescriptor(
      (void*)AggregateFunctions::InitNull,
      COERCE_CAST2_SAME(AggregateFunctions::Min, T),
      COERCE_CAST2_SAME(AggregateFunctions::Min, T));
}

template<typename T>
static OpcodeRegistry::AggFnDescriptor MaxDesc() {
  return OpcodeRegistry::AggFnDescriptor(
      (void*)AggregateFunctions::InitNull,
      COERCE_CAST2_SAME(AggregateFunctions::Max, T),
      COERCE_CAST2_SAME(AggregateFunctions::Max, T));
}

template<> inline
OpcodeRegistry::AggFnDescriptor MinDesc<StringVal>() {
  return OpcodeRegistry::AggFnDescriptor(
      (void*)AggregateFunctions::InitScratch,
      COERCE_CAST2_SAME(AggregateFunctions::Min, StringVal),
      COERCE_CAST2_SAME(AggregateFunctions::Min, StringVal),
      (void*)AggregateFunctions::SerializeScratch,
      (void*)AggregateFunctions::SerializeScratch);
}

template<> inline
OpcodeRegistry::AggFnDescriptor MaxDesc<StringVal>() {
  return OpcodeRegistry::AggFnDescriptor(
      (void*)AggregateFunctions::InitScratch,
      COERCE_CAST2_SAME(AggregateFunctions::Max, StringVal),
      COERCE_CAST2_SAME(AggregateFunctions::Max, StringVal),
      (void*)AggregateFunctions::SerializeScratch,
      (void*)AggregateFunctions::SerializeScratch);
}

static OpcodeRegistry::AggFnDescriptor StringConcatDesc() {
  return OpcodeRegistry::AggFnDescriptor(
      (void*)AggregateFunctions::InitScratch,
      (void*)AggregateFunctions::StringConcat,
      (void*)AggregateFunctions::StringConcat,
      (void*)AggregateFunctions::SerializeScratch,
      (void*)AggregateFunctions::SerializeScratch);
}

template<typename T>
static OpcodeRegistry::AggFnDescriptor DistinctPcDesc() {
  return OpcodeRegistry::AggFnDescriptor(
      (void*)AggregateFunctions::PcInit,
      (void*) (void(*)(FunctionContext*, const T&, StringVal*))
          AggregateFunctions::PcUpdate<T>,
      (void*)AggregateFunctions::PcMerge,
      NULL,
      (void*)AggregateFunctions::PcFinalize);
}

template<typename T>
static OpcodeRegistry::AggFnDescriptor DistinctPcsaDesc() {
  return OpcodeRegistry::AggFnDescriptor(
      (void*)AggregateFunctions::PcInit,
      (void*) (void(*)(FunctionContext*, const T&, StringVal*))
          AggregateFunctions::PcsaUpdate<T>,
      (void*)AggregateFunctions::PcMerge,
      NULL,
      (void*)AggregateFunctions::PcsaFinalize);
}

void InitAggregateBuiltins(OpcodeRegistry::AggregateBuiltins* fns) {
  // Count(*)
  (*fns)[make_pair(TAggregationOp::COUNT, INVALID_TYPE)] = CountStarDesc();

  // Count
  (*fns)[make_pair(TAggregationOp::COUNT, TYPE_NULL)] = NullDesc();
  (*fns)[make_pair(TAggregationOp::COUNT, TYPE_BOOLEAN)] = CountDesc();
  (*fns)[make_pair(TAggregationOp::COUNT, TYPE_TINYINT)] = CountDesc();
  (*fns)[make_pair(TAggregationOp::COUNT, TYPE_SMALLINT)] = CountDesc();
  (*fns)[make_pair(TAggregationOp::COUNT, TYPE_INT)] = CountDesc();
  (*fns)[make_pair(TAggregationOp::COUNT, TYPE_BIGINT)] = CountDesc();
  (*fns)[make_pair(TAggregationOp::COUNT, TYPE_FLOAT)] = CountDesc();
  (*fns)[make_pair(TAggregationOp::COUNT, TYPE_DOUBLE)] = CountDesc();
  (*fns)[make_pair(TAggregationOp::COUNT, TYPE_TIMESTAMP)] = CountDesc();
  (*fns)[make_pair(TAggregationOp::COUNT, TYPE_STRING)] = CountDesc();

  // Sum
  (*fns)[make_pair(TAggregationOp::SUM, TYPE_NULL)] = NullDesc();
  (*fns)[make_pair(TAggregationOp::SUM, TYPE_BOOLEAN)] =
      SumDesc<BooleanVal, BigIntVal>();
  (*fns)[make_pair(TAggregationOp::SUM, TYPE_TINYINT)] =
      SumDesc<TinyIntVal, BigIntVal>();
  (*fns)[make_pair(TAggregationOp::SUM, TYPE_SMALLINT)] =
      SumDesc<SmallIntVal, BigIntVal>();
  (*fns)[make_pair(TAggregationOp::SUM, TYPE_INT)] =
      SumDesc<IntVal, BigIntVal>();
  (*fns)[make_pair(TAggregationOp::SUM, TYPE_BIGINT)] =
      SumDesc<BigIntVal, BigIntVal>();
  (*fns)[make_pair(TAggregationOp::SUM, TYPE_FLOAT)] =
      SumDesc<FloatVal, DoubleVal>();
  (*fns)[make_pair(TAggregationOp::SUM, TYPE_DOUBLE)] =
      SumDesc<DoubleVal, DoubleVal>();

  // Min
  (*fns)[make_pair(TAggregationOp::MIN, TYPE_NULL)] = NullDesc();
  (*fns)[make_pair(TAggregationOp::MIN, TYPE_BOOLEAN)] = MinDesc<BooleanVal>();
  (*fns)[make_pair(TAggregationOp::MIN, TYPE_TINYINT)] = MinDesc<TinyIntVal>();
  (*fns)[make_pair(TAggregationOp::MIN, TYPE_SMALLINT)] = MinDesc<SmallIntVal>();
  (*fns)[make_pair(TAggregationOp::MIN, TYPE_INT)] = MinDesc<IntVal>();
  (*fns)[make_pair(TAggregationOp::MIN, TYPE_BIGINT)] = MinDesc<BigIntVal>();
  (*fns)[make_pair(TAggregationOp::MIN, TYPE_FLOAT)] = MinDesc<FloatVal>();
  (*fns)[make_pair(TAggregationOp::MIN, TYPE_DOUBLE)] = MinDesc<DoubleVal>();
  (*fns)[make_pair(TAggregationOp::MIN, TYPE_TIMESTAMP)] = MinDesc<TimestampVal>();
  (*fns)[make_pair(TAggregationOp::MIN, TYPE_STRING)] = MinDesc<StringVal>();

  // Max
  (*fns)[make_pair(TAggregationOp::MAX, TYPE_NULL)] = NullDesc();
  (*fns)[make_pair(TAggregationOp::MAX, TYPE_BOOLEAN)] = MaxDesc<BooleanVal>();
  (*fns)[make_pair(TAggregationOp::MAX, TYPE_TINYINT)] = MaxDesc<TinyIntVal>();
  (*fns)[make_pair(TAggregationOp::MAX, TYPE_SMALLINT)] = MaxDesc<SmallIntVal>();
  (*fns)[make_pair(TAggregationOp::MAX, TYPE_INT)] = MaxDesc<IntVal>();
  (*fns)[make_pair(TAggregationOp::MAX, TYPE_BIGINT)] = MaxDesc<BigIntVal>();
  (*fns)[make_pair(TAggregationOp::MAX, TYPE_FLOAT)] = MaxDesc<FloatVal>();
  (*fns)[make_pair(TAggregationOp::MAX, TYPE_DOUBLE)] = MaxDesc<DoubleVal>();
  (*fns)[make_pair(TAggregationOp::MAX, TYPE_TIMESTAMP)] = MaxDesc<TimestampVal>();
  (*fns)[make_pair(TAggregationOp::MAX, TYPE_STRING)] = MaxDesc<StringVal>();

  // Group Concat
  (*fns)[make_pair(TAggregationOp::GROUP_CONCAT, TYPE_NULL)] = NullDesc();
  (*fns)[make_pair(TAggregationOp::GROUP_CONCAT, TYPE_STRING)] = StringConcatDesc();

  // Distinct PC
  (*fns)[make_pair(TAggregationOp::DISTINCT_PC, TYPE_NULL)] = NullDesc();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PC, TYPE_BOOLEAN)] =
      DistinctPcDesc<BooleanVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PC, TYPE_TINYINT)] =
      DistinctPcDesc<TinyIntVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PC, TYPE_SMALLINT)] =
      DistinctPcDesc<SmallIntVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PC, TYPE_INT)] =
      DistinctPcDesc<IntVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PC, TYPE_BIGINT)] =
      DistinctPcDesc<BigIntVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PC, TYPE_FLOAT)] =
      DistinctPcDesc<FloatVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PC, TYPE_DOUBLE)] =
      DistinctPcDesc<DoubleVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PC, TYPE_TIMESTAMP)] =
      DistinctPcDesc<TimestampVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PC, TYPE_STRING)] =
      DistinctPcDesc<StringVal>();

  // Distinct PCSA
  (*fns)[make_pair(TAggregationOp::DISTINCT_PCSA, TYPE_NULL)] = NullDesc();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PCSA, TYPE_BOOLEAN)] =
      DistinctPcsaDesc<BooleanVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PCSA, TYPE_TINYINT)] =
      DistinctPcsaDesc<TinyIntVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PCSA, TYPE_SMALLINT)] =
      DistinctPcsaDesc<SmallIntVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PCSA, TYPE_INT)] =
      DistinctPcsaDesc<IntVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PCSA, TYPE_BIGINT)] =
      DistinctPcsaDesc<BigIntVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PCSA, TYPE_FLOAT)] =
      DistinctPcsaDesc<FloatVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PCSA, TYPE_DOUBLE)] =
      DistinctPcsaDesc<DoubleVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PCSA, TYPE_TIMESTAMP)] =
      DistinctPcsaDesc<TimestampVal>();
  (*fns)[make_pair(TAggregationOp::DISTINCT_PCSA, TYPE_STRING)] =
      DistinctPcsaDesc<StringVal>();
}


}

