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


#ifndef IMPALA_EXPRS_AGGREGATE_FUNCTIONS_H
#define IMPALA_EXPRS_AGGREGATE_FUNCTIONS_H

#include "udf/udf-internal.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::AnyVal;
using impala_udf::BooleanVal;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::FloatVal;
using impala_udf::DoubleVal;
using impala_udf::TimestampVal;
using impala_udf::StringVal;
using impala_udf::DecimalVal;
using impala_udf::DateVal;

static constexpr double FLOATING_POINT_ERROR_THRESHOLD = -1E-8;

/// Collection of builtin aggregate functions. Aggregate functions implement
/// the various phases of the aggregation: Init(), Update(), Serialize(), Merge(),
/// and Finalize(). Not all functions need to implement all of the steps and
/// some of the parts can be reused across different aggregate functions.
/// This functions are implemented using the UDA interface.
class AggregateFunctions {
 public:
  /// Initializes dst to NULL.
  static void InitNull(FunctionContext*, AnyVal* dst);
  /// Initializes dst to NULL and sets dst->ptr to NULL.
  static void InitNullString(FunctionContext* c, StringVal* dst);

  /// Initializes dst to 0.
  template <typename T>
  static void InitZero(FunctionContext*, T* dst);

  // Sets dst's value to src. Handles deallocation if src and dst are StringVals.
  template <typename T>
  static void UpdateVal(FunctionContext*, const T& src, T* dst);

  /// StringVal GetValue() function that returns a copy of src
  static StringVal StringValGetValue(FunctionContext* ctx, const StringVal& src);

  /// StringVal Serialize/Finalize function that copies and frees src
  static StringVal StringValSerializeOrFinalize(
      FunctionContext* ctx, const StringVal& src);

  /// Implementation of Regr_Count()
  static void RegrCountUpdate(FunctionContext*, const DoubleVal& src1,
      const DoubleVal& src2, BigIntVal* dst);
  static void RegrCountRemove(FunctionContext*, const DoubleVal& src1,
      const DoubleVal& src2, BigIntVal* dst);
  static void TimestampRegrCountUpdate(FunctionContext*,
      const TimestampVal& src1, const TimestampVal& src2, BigIntVal* dst);
  static void TimestampRegrCountRemove(FunctionContext*,
      const TimestampVal& src1, const TimestampVal& src2, BigIntVal* dst);

  /// Implementation of regr_slope() and regr_intercept()
  static void RegrSlopeInit(FunctionContext* ctx, StringVal* dst);
  static void RegrSlopeUpdate(FunctionContext* ctx, const DoubleVal& src1,
      const DoubleVal& src2, StringVal* dst);
  static void RegrSlopeRemove(FunctionContext* ctx, const DoubleVal& src1,
      const DoubleVal& src2, StringVal* dst);
  static void TimestampRegrSlopeUpdate(FunctionContext* ctx,
      const TimestampVal& src1, const TimestampVal& src2, StringVal* dst);
  static void TimestampRegrSlopeRemove(FunctionContext* ctx,
      const TimestampVal& src1, const TimestampVal& src2, StringVal* dst);
  static void RegrSlopeMerge(FunctionContext* ctx, const StringVal& src, StringVal* dst);
  static DoubleVal RegrSlopeGetValue(FunctionContext* ctx, const StringVal& src);
  static DoubleVal RegrSlopeFinalize(FunctionContext* ctx, const StringVal& src);
  static DoubleVal RegrInterceptGetValue(FunctionContext* ctx, const StringVal& src);
  static DoubleVal RegrInterceptFinalize(FunctionContext* ctx, const StringVal& src);

  /// Implementation of Corr()
  static void CorrInit(FunctionContext* ctx, StringVal* dst);
  static void CorrUpdate(FunctionContext* ctx, const DoubleVal& src1,
      const DoubleVal& src2, StringVal* dst);
  static void CorrRemove(FunctionContext* ctx, const DoubleVal& src1,
      const DoubleVal& src2, StringVal* dst);
  static void TimestampCorrUpdate(FunctionContext* ctx,
      const TimestampVal& src1, const TimestampVal& src2, StringVal* dst);
  static void TimestampCorrRemove(FunctionContext* ctx,
      const TimestampVal& src1, const TimestampVal& src2, StringVal* dst);
  static void CorrMerge(FunctionContext* ctx, const StringVal& src, StringVal* dst);
  static DoubleVal CorrGetValue(FunctionContext* ctx, const StringVal& src);
  static DoubleVal CorrFinalize(FunctionContext* ctx, const StringVal& src);

  /// Implementation of regr_r2()
  static DoubleVal Regr_r2GetValue(FunctionContext* ctx, const StringVal& src);
  static DoubleVal Regr_r2Finalize(FunctionContext* ctx, const StringVal& src);

  /// Implementation of Covar_samp() and Covar_pop()
  static void CovarInit(FunctionContext* ctx, StringVal* dst);
  static void CovarUpdate(FunctionContext* ctx, const DoubleVal& src1,
      const DoubleVal& src2, StringVal* dst);
  static void CovarRemove(FunctionContext* ctx, const DoubleVal& src1,
      const DoubleVal& src2, StringVal* dst);
  static void TimestampCovarUpdate(FunctionContext* ctx,
      const TimestampVal& src1, const TimestampVal& src2, StringVal* dst);
  static void TimestampCovarRemove(FunctionContext* ctx,
      const TimestampVal& src1, const TimestampVal& src2, StringVal* dst);
  static void CovarMerge(FunctionContext* ctx, const StringVal& src, StringVal* dst);
  static DoubleVal CovarSampleGetValue(FunctionContext* ctx, const StringVal& src);
  static DoubleVal CovarPopulationGetValue(FunctionContext* ctx, const StringVal& src);
  static DoubleVal CovarSampleFinalize(FunctionContext* ctx,
      const StringVal& src);
  static DoubleVal CovarPopulationFinalize(FunctionContext* ctx,
      const StringVal& src);

  /// Implementation of Count and Count(*)
  static void CountUpdate(FunctionContext*, const AnyVal& src, BigIntVal* dst);
  static void CountStarUpdate(FunctionContext*, BigIntVal* dst);
  static void CountRemove(FunctionContext*, const AnyVal& src, BigIntVal* dst);
  static void CountStarRemove(FunctionContext*, BigIntVal* dst);
  static void CountMerge(FunctionContext*, const BigIntVal& src, BigIntVal* dst);

  /// Implementation of Avg.
  /// TODO: Change this to use a fixed-sized BufferVal as intermediate type.
  static void AvgInit(FunctionContext* ctx, StringVal* dst);
  template <typename T>
  static void AvgUpdate(FunctionContext* ctx, const T& src, StringVal* dst);
  template <typename T>
  static void AvgRemove(FunctionContext* ctx, const T& src, StringVal* dst);
  static void AvgMerge(FunctionContext* ctx, const StringVal& src, StringVal* dst);
  static DoubleVal AvgGetValue(FunctionContext* ctx, const StringVal& val);
  static DoubleVal AvgFinalize(FunctionContext* ctx, const StringVal& val);

  /// Avg for timestamp. Uses AvgInit() and AvgMerge().
  static void TimestampAvgUpdate(FunctionContext* ctx, const TimestampVal& src,
      StringVal* dst);
  static void TimestampAvgRemove(FunctionContext* ctx, const TimestampVal& src,
      StringVal* dst);
  static TimestampVal TimestampAvgGetValue(FunctionContext* ctx, const StringVal& val);
  static TimestampVal TimestampAvgFinalize(FunctionContext* ctx, const StringVal& val);

  /// Avg for decimals.
  static void DecimalAvgInit(FunctionContext* ctx, StringVal* dst);
  static void DecimalAvgUpdate(FunctionContext* ctx, const DecimalVal& src,
      StringVal* dst);
  static void DecimalAvgRemove(FunctionContext* ctx, const DecimalVal& src,
      StringVal* dst);
  static void DecimalAvgAddOrRemove(FunctionContext* ctx, const DecimalVal& src,
      StringVal* dst, bool remove = false);
  static void DecimalAvgMerge(FunctionContext* ctx, const StringVal& src,
      StringVal* dst);
  static DecimalVal DecimalAvgGetValue(FunctionContext* ctx, const StringVal& val);
  static DecimalVal DecimalAvgFinalize(FunctionContext* ctx, const StringVal& val);

  /// SumUpdate, SumMerge
  template <typename SRC_VAL, typename DST_VAL>
  static void SumUpdate(FunctionContext*, const SRC_VAL& src, DST_VAL* dst);

  template <typename SRC_VAL, typename DST_VAL>
  static void SumRemove(FunctionContext*, const SRC_VAL& src, DST_VAL* dst);

  /// Sum for decimals
  static void SumDecimalUpdate(FunctionContext*, const DecimalVal& src, DecimalVal* dst);
  static void SumDecimalRemove(FunctionContext*, const DecimalVal& src, DecimalVal* dst);
  static void SumDecimalMerge(FunctionContext*, const DecimalVal& src, DecimalVal* dst);
  /// Adds or or subtracts src from dst. Implements Update() and Remove().
  static void SumDecimalAddOrSubtract(FunctionContext*, const DecimalVal& src,
      DecimalVal* dst, bool subtract = false);

  /// MinUpdate/MinMerge
  template <typename T>
  static void Min(FunctionContext*, const T& src, T* dst);

  /// MaxUpdate/MaxMerge
  template <typename T>
  static void Max(FunctionContext*, const T& src, T* dst);

  /// String concat
  static void StringConcatUpdate(FunctionContext*,
      const StringVal& src, StringVal* result);
  static void StringConcatUpdate(FunctionContext*,
      const StringVal& src, const StringVal& separator, StringVal* result);
  static void StringConcatMerge(FunctionContext*,
      const StringVal& src, StringVal* result);
  static StringVal StringConcatFinalize(FunctionContext*,
      const StringVal& src);

  /// Probabilistic Counting (PC), a distinct estimate algorithms.
  /// Probabilistic Counting with Stochastic Averaging (PCSA) is a variant
  /// of PC that runs faster and usually gets equally accurate results.
  static void PcInit(FunctionContext*, StringVal* slot);

  template <typename T>
  static void PcUpdate(FunctionContext*, const T& src, StringVal* dst);
  template <typename T>
  static void PcsaUpdate(FunctionContext*, const T& src, StringVal* dst);

  static void PcMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static BigIntVal PcFinalize(FunctionContext*, const StringVal& src);
  static BigIntVal PcsaFinalize(FunctionContext*, const StringVal& src);

  /// Reservoir sampling produces a uniform random sample without knowing the total number
  /// of items. ReservoirSample{Init, Update, Merge, Serialize} implement distributed
  /// reservoir sampling. Samples are first collected locally via reservoir sampling in
  /// Update(), and then all local samples are merged together in Merge() using weights
  /// proportional to the size of the local input, using 'weighted' reservoir sampling.
  /// See the following references for more details:
  /// http://dl.acm.org/citation.cfm?id=1138834
  /// http://gregable.com/2007/10/reservoir-sampling.html
  template <typename T>
  static void ReservoirSampleInit(FunctionContext*, StringVal* slot);
  template <typename T>
  static void ReservoirSampleUpdate(FunctionContext*, const T& src, StringVal* dst);
  template <typename T>
  static void ReservoirSampleMerge(FunctionContext*, const StringVal& src,
      StringVal* dst);
  template <typename T>
  static StringVal ReservoirSampleSerialize(FunctionContext*,
      const StringVal& src);

  /// Returns 20,000 unsorted samples as a list of comma-separated values.
  template <typename T>
  static StringVal ReservoirSampleFinalize(FunctionContext*, const StringVal& src);

  /// Returns an approximate median using reservoir sampling.
  template <typename T>
  static T AppxMedianFinalize(FunctionContext*, const StringVal& src);

  /// Returns an equi-depth histogram computed from a sample of data produced via
  /// reservoir sampling. The result is a comma-separated list of up to 100 histogram
  /// bucket endpoints where each bucket contains the same number of elements. For
  /// example, "10, 50, 60, 100" would mean 25% of values are less than 10, 25% are
  /// between 10 and 50, etc.
  template <typename T>
  static StringVal HistogramFinalize(FunctionContext*, const StringVal& src);

  /// Hyperloglog distinct estimate algorithm.
  /// See these papers for more details.
  /// 1) Hyperloglog: The analysis of a near-optimal cardinality estimation
  /// algorithm (2007)
  /// 2) HyperLogLog in Practice (paper from google with some improvements)

  /// This precision is the default precision from the paper. It doesn't seem to matter
  /// very much when between 6 and 12.
  static constexpr int DEFAULT_HLL_PRECISION = 10;

  // Define the min and the max of a precision value that can be indirectly
  // specified by the 2nd argument in NDV(<arg1>, <arg2>).
  static constexpr int MIN_HLL_PRECISION = 9;
  static constexpr int MAX_HLL_PRECISION = 18;

  // DEFAULT_HLL_LEN is the parameter m defined in the paper.
  static constexpr int DEFAULT_HLL_LEN = 1 << DEFAULT_HLL_PRECISION;

  // Define the min and the max of parameter m.
  static constexpr int MIN_HLL_LEN = 1 << MIN_HLL_PRECISION;
  static constexpr int MAX_HLL_LEN = 1 << MAX_HLL_PRECISION;

  static void HllInit(FunctionContext*, StringVal* slot);

  // The implementation for both versions of the update functions below.
  template <typename T>
  static void HllUpdate(FunctionContext*, const T& src, StringVal* dst, int precision);

  // Update function for single input argument version of NDV(),
  // utilizing the default precision for HLL algorithm.
  template <typename T>
  static void HllUpdate(FunctionContext*, const T& src, StringVal* dst);

  // Update function for two input argument version of NDV(),
  // utilizing the precision value indirectly specified through the
  // 2nd argument in NDV().
  template <typename T>
  static void HllUpdate(
      FunctionContext*, const T& src1, const IntVal& src2, StringVal* dst);

  static void HllMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static BigIntVal HllFinalize(FunctionContext*, const StringVal& src);

  /// Utility method to compute the final result of an HLL estimation.
  /// Assumes hll_len number of buckets.
  static uint64_t HllFinalEstimate(
      const uint8_t* buckets, int hll_len = AggregateFunctions::DEFAULT_HLL_LEN);

  /// These functions provide cardinality estimates similarly to ndv() but these use HLL
  /// algorithm from Apache Datasketches.
  static void DsHllInit(FunctionContext*, StringVal* slot);
  template <typename T>
  static void DsHllUpdate(FunctionContext*, const T& src, StringVal* dst);
  static StringVal DsHllSerialize(FunctionContext*, const StringVal& src);
  static void DsHllMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static BigIntVal DsHllFinalize(FunctionContext*, const StringVal& src);
  static StringVal DsHllFinalizeSketch(FunctionContext*, const StringVal& src);

  /// These functions implement ds_hll_union().
  static void DsHllUnionInit(FunctionContext*, StringVal* slot);
  static void DsHllUnionUpdate(FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal DsHllUnionSerialize(FunctionContext*, const StringVal& src);
  static void DsHllUnionMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal DsHllUnionFinalize(FunctionContext*, const StringVal& src);

  /// These functions implement Apache DataSketches CPC support for sketching.
  static void DsCpcInit(FunctionContext*, StringVal* slot);
  template <typename T>
  static void DsCpcUpdate(FunctionContext*, const T& src, StringVal* dst);
  static StringVal DsCpcSerialize(FunctionContext*, const StringVal& src);
  static void DsCpcMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static BigIntVal DsCpcFinalize(FunctionContext*, const StringVal& src);
  static StringVal DsCpcFinalizeSketch(FunctionContext*, const StringVal& src);

  /// These functions implement ds_cpc_union().
  static void DsCpcUnionInit(FunctionContext*, StringVal* dst);
  static void DsCpcUnionUpdate(FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal DsCpcUnionSerialize(FunctionContext*, const StringVal& src);
  static void DsCpcUnionMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal DsCpcUnionFinalize(FunctionContext*, const StringVal& src);

  /// These functions implement Apache DataSketches Theta support for sketching.
  static void DsThetaInit(FunctionContext*, StringVal* slot);
  template <typename T>
  static void DsThetaUpdate(FunctionContext*, const T& src, StringVal* dst);
  static StringVal DsThetaSerialize(FunctionContext*, const StringVal& src);
  static void DsThetaMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static BigIntVal DsThetaFinalize(FunctionContext*, const StringVal& src);
  static StringVal DsThetaFinalizeSketch(FunctionContext*, const StringVal& src);

  /// These functions implement ds_theta_union().
  static void DsThetaUnionInit(FunctionContext*, StringVal* dst);
  static void DsThetaUnionUpdate(FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal DsThetaUnionSerialize(FunctionContext*, const StringVal& src);
  static void DsThetaUnionMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal DsThetaUnionFinalize(FunctionContext*, const StringVal& src);

  /// These functions implement ds_theta_intersect().
  static void DsThetaIntersectInit(FunctionContext*, StringVal* slot);
  static void DsThetaIntersectUpdate(
      FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal DsThetaIntersectSerialize(FunctionContext*, const StringVal& src);
  static void DsThetaIntersectMerge(
      FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal DsThetaIntersectFinalize(FunctionContext*, const StringVal& src);

  /// These functions implement Apache DataSketches KLL support for sketching.
  static void DsKllInit(FunctionContext*, StringVal* slot);
  static void DsKllUpdate(FunctionContext*, const FloatVal& src, StringVal* dst);
  static StringVal DsKllSerialize(FunctionContext*, const StringVal& src);
  static void DsKllMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal DsKllFinalizeSketch(FunctionContext*, const StringVal& src);

  // Helper functions to keep common code for DataSketches KLL sketch and union
  // operations.
  static void DsKllInitHelper(FunctionContext* ctx, StringVal* slot);
  static StringVal DsKllSerializeHelper(FunctionContext* ctx, const StringVal& src);
  static void DsKllMergeHelper(FunctionContext* ctx, const StringVal& src,
      StringVal* dst);
  static StringVal DsKllFinalizeHelper(FunctionContext*, const StringVal& src);

  /// These functions implement ds_kll_union().
  static void DsKllUnionInit(FunctionContext*, StringVal* slot);
  static void DsKllUnionUpdate(FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal DsKllUnionSerialize(FunctionContext*, const StringVal& src);
  static void DsKllUnionMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static StringVal DsKllUnionFinalize(FunctionContext*, const StringVal& src);

  /// Estimates the number of distinct values (NDV) based on a sample of data and the
  /// corresponding sampling rate. The main idea of this function is to collect several
  /// (x,y) data points where x is the number of rows and y is the corresponding NDV
  /// estimate. These data points are used to fit an objective function to the data such
  /// that the true NDV can be extrapolated.
  /// This aggregate function maintains a fixed number of HyperLogLog intermediates.
  /// The Update() phase updates the intermediates in a round-robin fashion.
  /// The Merge() phase combines the corresponding intermediates.
  /// The Finalize() phase generates (x,y) data points, performs curve fitting, and
  /// computes the estimated true NDV.
  static void SampledNdvInit(FunctionContext*, StringVal* dst);
  template <typename T>
  static void SampledNdvUpdate(FunctionContext*, const T& src,
      const DoubleVal& sample_perc, StringVal* dst);
  static void SampledNdvMerge(FunctionContext*, const StringVal& src, StringVal* dst);
  static BigIntVal SampledNdvFinalize(FunctionContext*, const StringVal& src);

  /// The AGGIF(predicate, expr) function returns 'expr' if 'predicate' is true.
  /// It is expected that 'predicate' only returns true for a single row per group.
  /// The predicate must not evaluate to NULL.
  template <typename T>
  static void AggIfUpdate(FunctionContext*, const BooleanVal& cond, const T& src, T* dst);
  template <typename T>
  static void AggIfMerge(FunctionContext*, const T& src, T* dst);
  template <typename T>
  static T AggIfFinalize(FunctionContext*, const T& src);

  /// Knuth's variance algorithm, more numerically stable than canonical stddev
  /// algorithms; reference implementation:
  /// http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
  static void KnuthVarInit(FunctionContext* context, StringVal* val);
  template <typename T>
  static void KnuthVarUpdate(FunctionContext* context, const T& input, StringVal* val);
  static void KnuthVarMerge(FunctionContext* context, const StringVal& src,
                            StringVal* dst);
  static DoubleVal KnuthVarFinalize(FunctionContext* context, const StringVal& val);

  /// Calculates the biased variance, uses KnuthVar Init-Update-Merge functions
  static DoubleVal KnuthVarPopFinalize(FunctionContext* context, const StringVal& val);

  /// Calculates STDDEV, uses KnuthVar Init-Update-Merge functions
  static DoubleVal KnuthStddevFinalize(FunctionContext* context, const StringVal& val);

  /// Calculates the biased STDDEV, uses KnuthVar Init-Update-Merge functions
  static DoubleVal KnuthStddevPopFinalize(FunctionContext* context, const StringVal& val);


  /// ----------------------------- Analytic Functions ---------------------------------
  /// Analytic functions implement the UDA interface (except Merge(), Serialize()) and are
  /// used internally by the AnalyticEvalNode. Some analytic functions store intermediate
  /// state as a StringVal which is needed for multiple calls to Finalize(), so some fns
  /// also implement a (private) GetValue() method to just return the value. In that
  /// case, Finalize() is only called at the end to clean up.

  /// Initializes the state for RANK and DENSE_RANK
  static void RankInit(FunctionContext*, StringVal* slot);

  /// Update state for RANK
  static void RankUpdate(FunctionContext*, StringVal* dst);

  /// Update state for DENSE_RANK
  static void DenseRankUpdate(FunctionContext*, StringVal* dst);

  /// Returns the result for RANK and prepares the state for the next Update().
  static BigIntVal RankGetValue(FunctionContext*, StringVal& src);

  /// Returns the result for DENSE_RANK and prepares the state for the next Update().
  /// TODO: Implement DENSE_RANK with a single BigIntVal. Requires src can be modified,
  /// AggFnEvaluator would need to handle copying the src AnyVal back into the src slot.
  static BigIntVal DenseRankGetValue(FunctionContext*, StringVal& src);

  /// Returns the result for RANK and DENSE_RANK and cleans up intermediate state in src.
  static BigIntVal RankFinalize(FunctionContext*, StringVal& src);

  /// Implements LAST_VALUE.
  template <typename T>
  static void LastValRemove(FunctionContext*, const T& src, T* dst);

  // Implements LAST_VALUE_IGNORE_NULLS
  template <typename T>
  static void LastValIgnoreNullsInit(FunctionContext*, StringVal* dst);
  template <typename T>
  static void LastValIgnoreNullsUpdate(FunctionContext*, const T& src, StringVal* dst);
  template <typename T>
  static void LastValIgnoreNullsRemove(FunctionContext*, const T& src, StringVal* dst);
  template <typename T>
  static T LastValIgnoreNullsGetValue(FunctionContext* ctx, const StringVal& src);
  template <typename T>
  static T LastValIgnoreNullsFinalize(FunctionContext* ctx, const StringVal& src);

  /// Implements FIRST_VALUE. Requires a start bound of UNBOUNDED PRECEDING.
  template <typename T>
  static void FirstValUpdate(FunctionContext*, const T& src, T* dst);
  /// Implements FIRST_VALUE for some windows that require rewrites during planning.
  /// The BigIntVal is unused by FirstValRewriteUpdate() (it is used by the
  /// AnalyticEvalNode).
  template <typename T>
  static void FirstValRewriteUpdate(FunctionContext*, const T& src, const BigIntVal&,
      T* dst);
  /// Implements FIRST_VALUE_IGNORE_NULLS. Requires a start bound of UNBOUNDED PRECEDING.
  template <typename T>
  static void FirstValIgnoreNullsUpdate(FunctionContext*, const T& src, T* dst);

  /// OffsetFn*() implement LAG and LEAD. Init() sets the default value (the last
  /// constant parameter) as dst.
  template <typename T>
  static void OffsetFnInit(FunctionContext*, T* dst);

  /// Update() takes all the parameters to LEAD/LAG, including the integer offset and
  /// the default value, neither which are needed by Update(). (The offset is already
  /// used in the window for the analytic fn evaluation and the default value is set
  /// in Init().
  template <typename T>
  static void OffsetFnUpdate(FunctionContext*, const T& src, const BigIntVal&, const T&,
      T* dst);
};

}
#endif
