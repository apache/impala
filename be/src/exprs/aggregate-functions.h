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


#ifndef IMPALA_EXPRS_AGGREGATE_FUNCTIONS_H
#define IMPALA_EXPRS_AGGREGATE_FUNCTIONS_H

#include "udf/udf-internal.h"

using namespace impala_udf;

namespace impala {

// Collection of builtin aggregate functions. Aggregate functions implement
// the various phases of the aggregation: Init(), Update(), Serialize(), Merge(),
// and Finalize(). Not all functions need to implement all of the steps and
// some of the parts can be reused across different aggregate functions.
// This functions are implemented using the UDA interface.
class AggregateFunctions {
 public:
  // Initializes dst to NULL.
  static void InitNull(FunctionContext*, AnyVal* dst);
  // Initializes dst to NULL and sets dst->ptr to NULL.
  static void InitNullString(FunctionContext* c, StringVal* dst);

  // Initializes dst to 0.
  template <typename T>
  static void InitZero(FunctionContext*, T* dst);

  // StringVal Serialize/Finalize function that copies and frees src
  static StringVal StringValSerializeOrFinalize(
      FunctionContext* ctx, const StringVal& src);

  // Implementation of Count and Count(*)
  static void CountUpdate(FunctionContext*, const AnyVal& src, BigIntVal* dst);
  static void CountStarUpdate(FunctionContext*, BigIntVal* dst);
  static void CountMerge(FunctionContext*, const BigIntVal& src, BigIntVal* dst);

  // Implementation of Avg.
  // TODO: Change this to use a fixed-sized BufferVal as intermediate type.
  static void AvgInit(FunctionContext* ctx, StringVal* dst);
  template <typename T>
  static void AvgUpdate(FunctionContext* ctx, const T& src, StringVal* dst);
  static void AvgMerge(FunctionContext* ctx, const StringVal& src, StringVal* dst);
  static DoubleVal AvgFinalize(FunctionContext* ctx, const StringVal& val);

  // Avg for timestamp. Uses AvgInit() and AvgMerge().
  static void TimestampAvgUpdate(FunctionContext* ctx, const TimestampVal& src,
      StringVal* dst);
  static TimestampVal TimestampAvgFinalize(FunctionContext* ctx, const StringVal& val);

  // Avg for decimals.
  static void DecimalAvgInit(FunctionContext* ctx, StringVal* dst);
  static void DecimalAvgUpdate(FunctionContext* ctx, const DecimalVal& src,
      StringVal* dst);
  static void DecimalAvgMerge(FunctionContext* ctx, const StringVal& src,
      StringVal* dst);
  static DecimalVal DecimalAvgFinalize(FunctionContext* ctx, const StringVal& val);

  // SumUpdate, SumMerge
  template <typename SRC_VAL, typename DST_VAL>
  static void Sum(FunctionContext*, const SRC_VAL& src, DST_VAL* dst);

  // Sum for decimals
  static void SumUpdate(FunctionContext*, const DecimalVal& src, DecimalVal* dst);
  static void SumMerge(FunctionContext*, const DecimalVal& src, DecimalVal* dst);

  // MinUpdate/MinMerge
  template <typename T>
  static void Min(FunctionContext*, const T& src, T* dst);

  // MaxUpdate/MaxMerge
  template <typename T>
  static void Max(FunctionContext*, const T& src, T* dst);

  // String concat
  static void StringConcatUpdate(FunctionContext*,
      const StringVal& src, StringVal* result);
  static void StringConcatUpdate(FunctionContext*,
      const StringVal& src, const StringVal& separator, StringVal* result);
  static void StringConcatMerge(FunctionContext*,
      const StringVal& src, StringVal* result);
  static StringVal StringConcatFinalize(FunctionContext*,
      const StringVal& src);

  // Probabilistic Counting (PC), a distinct estimate algorithms.
  // Probabilistic Counting with Stochastic Averaging (PCSA) is a variant
  // of PC that runs faster and usually gets equally accurate results.
  static void PcInit(FunctionContext*, StringVal* slot);

  template <typename T>
  static void PcUpdate(FunctionContext*, const T& src, StringVal* dst);
  template <typename T>
  static void PcsaUpdate(FunctionContext*, const T& src, StringVal* dst);

  static void PcMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal PcFinalize(FunctionContext*, const StringVal& src);
  static StringVal PcsaFinalize(FunctionContext*, const StringVal& src);

  // Reservoir sampling produces a uniform random sample without knowing the total number
  // of items. ReservoirSample{Init, Update, Merge, Serialize} implement distributed
  // reservoir sampling. Samples are first collected locally via reservoir sampling in
  // Update(), and then all local samples are merged together in Merge() using weights
  // proportional to the size of the local input, using 'weighted' reservoir sampling.
  // See the following references for more details:
  // http://dl.acm.org/citation.cfm?id=1138834
  // http://gregable.com/2007/10/reservoir-sampling.html
  template <typename T>
  static void ReservoirSampleInit(FunctionContext*, StringVal* slot);
  template <typename T>
  static void ReservoirSampleUpdate(FunctionContext*, const T& src, StringVal* dst);
  template <typename T>
  static void ReservoirSampleMerge(FunctionContext*, const StringVal& src,
      StringVal* dst);
  template <typename T>
  static const StringVal ReservoirSampleSerialize(FunctionContext*,
      const StringVal& src);

  // Returns 20,000 unsorted samples as a list of comma-separated values.
  template <typename T>
  static StringVal ReservoirSampleFinalize(FunctionContext*, const StringVal& src);

  // Returns an approximate median using reservoir sampling.
  // TODO: Return T when return type does not need to be the intermediate type
  template <typename T>
  static StringVal AppxMedianFinalize(FunctionContext*, const StringVal& src);

  // Returns an equi-depth histogram computed from a sample of data produced via
  // reservoir sampling. The result is a comma-separated list of up to 100 histogram
  // bucket endpoints where each bucket contains the same number of elements. For
  // example, "10, 50, 60, 100" would mean 25% of values are less than 10, 25% are
  // between 10 and 50, etc.
  template <typename T>
  static StringVal HistogramFinalize(FunctionContext*, const StringVal& src);

  // Hyperloglog distinct estimate algorithm.
  // See these papers for more details.
  // 1) Hyperloglog: The analysis of a near-optimal cardinality estimation
  // algorithm (2007)
  // 2) HyperLogLog in Practice (paper from google with some improvements)
  static void HllInit(FunctionContext*, StringVal* slot);
  template <typename T>
  static void HllUpdate(FunctionContext*, const T& src, StringVal* dst);
  static void HllMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal HllFinalize(FunctionContext*, const StringVal& src);

  // Knuth's variance algorithm, more numerically stable than canonical stddev
  // algorithms; reference implementation:
  // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
  static void KnuthVarInit(FunctionContext* context, StringVal* val);
  template <typename T>
  static void KnuthVarUpdate(FunctionContext* context, const T& input, StringVal* val);
  static void KnuthVarMerge(FunctionContext* context, const StringVal& src,
                            StringVal* dst);
  static DoubleVal KnuthVarFinalize(FunctionContext* context, const StringVal& val);

  // Calculates the biased variance, uses KnuthVar Init-Update-Merge functions
  static StringVal KnuthVarPopFinalize(FunctionContext* context, const StringVal& val);

  // Calculates STDDEV, uses KnuthVar Init-Update-Merge functions
  static StringVal KnuthStddevFinalize(FunctionContext* context, const StringVal& val);

  // Calculates the biased STDDEV, uses KnuthVar Init-Update-Merge functions
  static StringVal KnuthStddevPopFinalize(FunctionContext* context, const StringVal& val);


  // ----------------------------- Analytic Functions ---------------------------------
  // Analytic functions implement the UDA interface (except Merge(), Serialize()) and are
  // used internally by the AnalyticEvalNode. Some analytic functions store intermediate
  // state as a StringVal which is needed for multiple calls to Finalize(), so some fns
  // also implement a (private) GetValue() method to just return the value. In that
  // case, Finalize() is only called at the end to clean up.

  // Initializes the state for RANK and DENSE_RANK
  static void RankInit(FunctionContext*, StringVal* slot);

  // Update state for RANK
  static void RankUpdate(FunctionContext*, StringVal* dst);

  // Update state for DENSE_RANK
  static void DenseRankUpdate(FunctionContext*, StringVal* dst);

  // Returns the result for RANK and prepares the state for the next Update().
  static BigIntVal RankGetValue(FunctionContext*, StringVal& src);

  // Returns the result for DENSE_RANK and prepares the state for the next Update().
  // TODO: Implement DENSE_RANK with a single BigIntVal. Requires src can be modified,
  // AggFnEvaluator would need to handle copying the src AnyVal back into the src slot.
  static BigIntVal DenseRankGetValue(FunctionContext*, StringVal& src);

  // Returns the result for RANK and DENSE_RANK and cleans up intermediate state in src.
  static BigIntVal RankFinalize(FunctionContext*, StringVal& src);
};

}
#endif
