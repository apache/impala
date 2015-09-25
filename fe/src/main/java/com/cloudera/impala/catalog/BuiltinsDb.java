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

package com.cloudera.impala.catalog;

import java.util.ArrayList;
import java.util.Map;

import com.cloudera.impala.analysis.ArithmeticExpr;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.CaseExpr;
import com.cloudera.impala.analysis.CastExpr;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.InPredicate;
import com.cloudera.impala.analysis.IsNullPredicate;
import com.cloudera.impala.analysis.LikePredicate;
import com.cloudera.impala.builtins.ScalarBuiltins;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class BuiltinsDb extends Db {
  public BuiltinsDb(String name, Catalog catalog) {
    super(name, catalog, null);
    setIsSystemDb(true);
    initBuiltins();
  }

  /**
   * Initializes all the builtins.
   */
  private void initBuiltins() {
    // Populate all aggregate builtins.
    initAggregateBuiltins();

    // Populate all scalar builtins.
    ArithmeticExpr.initBuiltins(this);
    BinaryPredicate.initBuiltins(this);
    CastExpr.initBuiltins(this);
    CaseExpr.initBuiltins(this);
    CompoundPredicate.initBuiltins(this);
    InPredicate.initBuiltins(this);
    IsNullPredicate.initBuiltins(this);
    LikePredicate.initBuiltins(this);
    ScalarBuiltins.initBuiltins(this);
  }

  private static final Map<Type, String> SAMPLE_INIT_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "19ReservoirSampleInitIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.TINYINT,
            "19ReservoirSampleInitIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.SMALLINT,
            "19ReservoirSampleInitIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.INT,
            "19ReservoirSampleInitIN10impala_udf6IntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.BIGINT,
            "19ReservoirSampleInitIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.FLOAT,
            "19ReservoirSampleInitIN10impala_udf8FloatValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.DOUBLE,
            "19ReservoirSampleInitIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.STRING,
            "19ReservoirSampleInitIN10impala_udf9StringValEEEvPNS2_15FunctionContextEPS3_")
        .put(Type.TIMESTAMP,
            "19ReservoirSampleInitIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.DECIMAL,
            "19ReservoirSampleInitIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .build();

  private static final Map<Type, String> SAMPLE_SERIALIZE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "24ReservoirSampleSerializeIN10impala_udf10BooleanValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.TINYINT,
             "24ReservoirSampleSerializeIN10impala_udf10TinyIntValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.SMALLINT,
             "24ReservoirSampleSerializeIN10impala_udf11SmallIntValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.INT,
             "24ReservoirSampleSerializeIN10impala_udf6IntValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.BIGINT,
             "24ReservoirSampleSerializeIN10impala_udf9BigIntValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.FLOAT,
             "24ReservoirSampleSerializeIN10impala_udf8FloatValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.DOUBLE,
             "24ReservoirSampleSerializeIN10impala_udf9DoubleValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.STRING,
             "24ReservoirSampleSerializeIN10impala_udf9StringValEEEKS3_PNS2_15FunctionContextERS4_")
        .put(Type.TIMESTAMP,
             "24ReservoirSampleSerializeIN10impala_udf12TimestampValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .put(Type.DECIMAL,
             "24ReservoirSampleSerializeIN10impala_udf10DecimalValEEEKNS2_9StringValEPNS2_15FunctionContextERS5_")
        .build();

  private static final Map<Type, String> SAMPLE_MERGE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "20ReservoirSampleMergeIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.TINYINT,
            "20ReservoirSampleMergeIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.SMALLINT,
            "20ReservoirSampleMergeIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.INT,
            "20ReservoirSampleMergeIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.BIGINT,
            "20ReservoirSampleMergeIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.FLOAT,
            "20ReservoirSampleMergeIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.DOUBLE,
            "20ReservoirSampleMergeIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.STRING,
            "20ReservoirSampleMergeIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKS3_PS3_")
        .put(Type.TIMESTAMP,
            "20ReservoirSampleMergeIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .put(Type.DECIMAL,
            "20ReservoirSampleMergeIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
        .build();

  private static final Map<Type, String> SAMPLE_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "21ReservoirSampleUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.TINYINT,
            "21ReservoirSampleUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.SMALLINT,
            "21ReservoirSampleUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "21ReservoirSampleUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "21ReservoirSampleUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "21ReservoirSampleUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "21ReservoirSampleUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.STRING,
            "21ReservoirSampleUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .put(Type.TIMESTAMP,
            "21ReservoirSampleUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DECIMAL,
            "21ReservoirSampleUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .build();

  private static final Map<Type, String> SAMPLE_FINALIZE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "23ReservoirSampleFinalizeIN10impala_udf10BooleanValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.TINYINT,
             "23ReservoirSampleFinalizeIN10impala_udf10TinyIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.SMALLINT,
             "23ReservoirSampleFinalizeIN10impala_udf11SmallIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.INT,
             "23ReservoirSampleFinalizeIN10impala_udf6IntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.BIGINT,
             "23ReservoirSampleFinalizeIN10impala_udf9BigIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.FLOAT,
             "23ReservoirSampleFinalizeIN10impala_udf8FloatValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DOUBLE,
             "23ReservoirSampleFinalizeIN10impala_udf9DoubleValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.STRING,
             "23ReservoirSampleFinalizeIN10impala_udf9StringValEEES3_PNS2_15FunctionContextERKS3_")
        .put(Type.TIMESTAMP,
             "23ReservoirSampleFinalizeIN10impala_udf12TimestampValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DECIMAL,
             "23ReservoirSampleFinalizeIN10impala_udf10DecimalValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .build();

  private static final Map<Type, String> APPX_MEDIAN_FINALIZE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "18AppxMedianFinalizeIN10impala_udf10BooleanValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.DECIMAL,
            "18AppxMedianFinalizeIN10impala_udf10DecimalValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.TINYINT,
            "18AppxMedianFinalizeIN10impala_udf10TinyIntValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.SMALLINT,
            "18AppxMedianFinalizeIN10impala_udf11SmallIntValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.TIMESTAMP,
            "18AppxMedianFinalizeIN10impala_udf12TimestampValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.INT,
            "18AppxMedianFinalizeIN10impala_udf6IntValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.FLOAT,
            "18AppxMedianFinalizeIN10impala_udf8FloatValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.BIGINT,
            "18AppxMedianFinalizeIN10impala_udf9BigIntValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.DOUBLE,
            "18AppxMedianFinalizeIN10impala_udf9DoubleValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.STRING,
            "18AppxMedianFinalizeIN10impala_udf9StringValEEET_PNS2_15FunctionContextERKS3_")
        .build();

  private static final Map<Type, String> HISTOGRAM_FINALIZE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "17HistogramFinalizeIN10impala_udf10BooleanValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.TINYINT,
             "17HistogramFinalizeIN10impala_udf10TinyIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.SMALLINT,
             "17HistogramFinalizeIN10impala_udf11SmallIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.INT,
             "17HistogramFinalizeIN10impala_udf6IntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.BIGINT,
             "17HistogramFinalizeIN10impala_udf9BigIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.FLOAT,
             "17HistogramFinalizeIN10impala_udf8FloatValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DOUBLE,
             "17HistogramFinalizeIN10impala_udf9DoubleValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.STRING,
             "17HistogramFinalizeIN10impala_udf9StringValEEES3_PNS2_15FunctionContextERKS3_")
        .put(Type.TIMESTAMP,
             "17HistogramFinalizeIN10impala_udf12TimestampValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DECIMAL,
             "17HistogramFinalizeIN10impala_udf10DecimalValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .build();

  private static final Map<Type, String> HLL_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "9HllUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.TINYINT,
            "9HllUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.SMALLINT,
            "9HllUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "9HllUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "9HllUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "9HllUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "9HllUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.STRING,
            "9HllUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .put(Type.TIMESTAMP,
            "9HllUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DECIMAL,
            "9HllUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .build();

  private static final Map<Type, String> PC_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "8PcUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.TINYINT,
            "8PcUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.SMALLINT,
            "8PcUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "8PcUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "8PcUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "8PcUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "8PcUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.STRING,
            "8PcUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .put(Type.TIMESTAMP,
            "8PcUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
         .put(Type.DECIMAL,
            "8PcUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .build();

    private static final Map<Type, String> PCSA_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
          .put(Type.BOOLEAN,
              "10PcsaUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.TINYINT,
              "10PcsaUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.SMALLINT,
              "10PcsaUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.INT,
              "10PcsaUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.BIGINT,
              "10PcsaUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.FLOAT,
              "10PcsaUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.DOUBLE,
              "10PcsaUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.STRING,
              "10PcsaUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
          .put(Type.TIMESTAMP,
              "10PcsaUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(Type.DECIMAL,
              "10PcsaUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .build();

  private static final Map<Type, String> MIN_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "3MinIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TINYINT,
            "3MinIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.SMALLINT,
            "3MinIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.INT,
            "3MinIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.BIGINT,
            "3MinIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.FLOAT,
            "3MinIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DOUBLE,
            "3MinIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.STRING,
            "3MinIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TIMESTAMP,
            "3MinIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DECIMAL,
            "3MinIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .build();

  private static final Map<Type, String> MAX_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "3MaxIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TINYINT,
            "3MaxIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.SMALLINT,
            "3MaxIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.INT,
            "3MaxIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.BIGINT,
            "3MaxIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.FLOAT,
            "3MaxIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DOUBLE,
            "3MaxIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.STRING,
            "3MaxIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TIMESTAMP,
            "3MaxIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DECIMAL,
            "3MaxIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .build();

  private static final Map<Type, String> STDDEV_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.TINYINT,
            "14KnuthVarUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.SMALLINT,
            "14KnuthVarUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "14KnuthVarUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "14KnuthVarUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "14KnuthVarUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "14KnuthVarUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .build();
  private static final Map<Type, String> OFFSET_FN_INIT_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "12OffsetFnInitIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextEPT_")
        .put(Type.DECIMAL,
             "12OffsetFnInitIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextEPT_")
        .put(Type.TINYINT,
             "12OffsetFnInitIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextEPT_")
        .put(Type.SMALLINT,
             "12OffsetFnInitIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextEPT_")
        .put(Type.TIMESTAMP,
             "12OffsetFnInitIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextEPT_")
        .put(Type.INT,
             "12OffsetFnInitIN10impala_udf6IntValEEEvPNS2_15FunctionContextEPT_")
        .put(Type.FLOAT,
             "12OffsetFnInitIN10impala_udf8FloatValEEEvPNS2_15FunctionContextEPT_")
        .put(Type.BIGINT,
             "12OffsetFnInitIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_")
        .put(Type.DOUBLE,
             "12OffsetFnInitIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextEPT_")
        .put(Type.STRING,
             "12OffsetFnInitIN10impala_udf9StringValEEEvPNS2_15FunctionContextEPT_")
        .build();

  private static final Map<Type, String> OFFSET_FN_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "14OffsetFnUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
        .put(Type.DECIMAL,
             "14OffsetFnUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
        .put(Type.TINYINT,
             "14OffsetFnUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
        .put(Type.SMALLINT,
             "14OffsetFnUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
        .put(Type.TIMESTAMP,
             "14OffsetFnUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
        .put(Type.INT,
             "14OffsetFnUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
        .put(Type.FLOAT,
             "14OffsetFnUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
        .put(Type.BIGINT,
             "14OffsetFnUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_RKS3_S8_PS6_")
        .put(Type.DOUBLE,
             "14OffsetFnUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
        .put(Type.STRING,
             "14OffsetFnUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
        .build();

  private static final Map<Type, String> LAST_VALUE_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "13LastValUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DECIMAL,
             "13LastValUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TINYINT,
             "13LastValUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.SMALLINT,
             "13LastValUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TIMESTAMP,
             "13LastValUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.INT,
             "13LastValUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.FLOAT,
             "13LastValUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.BIGINT,
             "13LastValUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DOUBLE,
             "13LastValUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.STRING,
             "13LastValUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .build();

  private static final Map<Type, String> FIRST_VALUE_REWRITE_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "21FirstValRewriteUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
        .put(Type.DECIMAL,
             "21FirstValRewriteUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
        .put(Type.TINYINT,
             "21FirstValRewriteUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
        .put(Type.SMALLINT,
             "21FirstValRewriteUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
        .put(Type.TIMESTAMP,
             "21FirstValRewriteUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
        .put(Type.INT,
             "21FirstValRewriteUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
        .put(Type.FLOAT,
             "21FirstValRewriteUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
        .put(Type.BIGINT,
             "21FirstValRewriteUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_RKS3_PS6_")
        .put(Type.DOUBLE,
             "21FirstValRewriteUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
        .put(Type.STRING,
             "21FirstValRewriteUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
        .build();

  private static final Map<Type, String> LAST_VALUE_REMOVE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "13LastValRemoveIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DECIMAL,
             "13LastValRemoveIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TINYINT,
             "13LastValRemoveIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.SMALLINT,
             "13LastValRemoveIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TIMESTAMP,
             "13LastValRemoveIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.INT,
             "13LastValRemoveIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.FLOAT,
             "13LastValRemoveIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.BIGINT,
             "13LastValRemoveIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DOUBLE,
             "13LastValRemoveIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.STRING,
             "13LastValRemoveIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .build();

  private static final Map<Type, String> FIRST_VALUE_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "14FirstValUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DECIMAL,
             "14FirstValUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TINYINT,
             "14FirstValUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.SMALLINT,
             "14FirstValUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TIMESTAMP,
             "14FirstValUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.INT,
             "14FirstValUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.FLOAT,
             "14FirstValUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.BIGINT,
             "14FirstValUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DOUBLE,
             "14FirstValUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.STRING,
             "14FirstValUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .build();

  // Populate all the aggregate builtins in the catalog.
  // null symbols indicate the function does not need that step of the evaluation.
  // An empty symbol indicates a TODO for the BE to implement the function.
  // TODO: We could also generate this in python but I'm not sure that is easier.
  private void initAggregateBuiltins() {
    final String prefix = "_ZN6impala18AggregateFunctions";
    final String initNullString = prefix +
        "14InitNullStringEPN10impala_udf15FunctionContextEPNS1_9StringValE";
    final String initNull = prefix +
        "8InitNullEPN10impala_udf15FunctionContextEPNS1_6AnyValE";
    final String stringValSerializeOrFinalize = prefix +
        "28StringValSerializeOrFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE";
    final String stringValGetValue = prefix +
        "17StringValGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE";

    Db db = this;
    // Count (*)
    db.addBuiltin(AggregateFunction.createBuiltin(db, "count",
        new ArrayList<Type>(), Type.BIGINT, Type.BIGINT,
        prefix + "8InitZeroIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
        prefix + "15CountStarUpdateEPN10impala_udf15FunctionContextEPNS1_9BigIntValE",
        prefix + "10CountMergeEPN10impala_udf15FunctionContextERKNS1_9BigIntValEPS4_",
        null, null,
        prefix + "15CountStarRemoveEPN10impala_udf15FunctionContextEPNS1_9BigIntValE",
        null, false, true, true));

    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue; // NULL is handled through type promotion.
      if (t.isScalarType(PrimitiveType.CHAR)) continue; // promoted to STRING
      if (t.isScalarType(PrimitiveType.VARCHAR)) continue; // promoted to STRING
      // Count
      db.addBuiltin(AggregateFunction.createBuiltin(db, "count",
          Lists.newArrayList(t), Type.BIGINT, Type.BIGINT,
          prefix + "8InitZeroIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
          prefix + "11CountUpdateEPN10impala_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
          prefix + "10CountMergeEPN10impala_udf15FunctionContextERKNS1_9BigIntValEPS4_",
          null, null,
          prefix + "11CountRemoveEPN10impala_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
          null, false, true, true));

      // Min
      String minMaxInit = t.isStringType() ? initNullString : initNull;
      String minMaxSerializeOrFinalize = t.isStringType() ?
          stringValSerializeOrFinalize : null;
      String minMaxGetValue = t.isStringType() ? stringValGetValue : null;
      db.addBuiltin(AggregateFunction.createBuiltin(db, "min",
          Lists.newArrayList(t), t, t, minMaxInit,
          prefix + MIN_UPDATE_SYMBOL.get(t),
          prefix + MIN_UPDATE_SYMBOL.get(t),
          minMaxSerializeOrFinalize, minMaxGetValue,
          null, minMaxSerializeOrFinalize, true, true, false));
      // Max
      db.addBuiltin(AggregateFunction.createBuiltin(db, "max",
          Lists.newArrayList(t), t, t, minMaxInit,
          prefix + MAX_UPDATE_SYMBOL.get(t),
          prefix + MAX_UPDATE_SYMBOL.get(t),
          minMaxSerializeOrFinalize, minMaxGetValue,
          null, minMaxSerializeOrFinalize, true, true, false));
      // Sample
      db.addBuiltin(AggregateFunction.createBuiltin(db, "sample",
          Lists.newArrayList(t), Type.STRING, Type.STRING,
          prefix + SAMPLE_INIT_SYMBOL.get(t),
          prefix + SAMPLE_UPDATE_SYMBOL.get(t),
          prefix + SAMPLE_MERGE_SYMBOL.get(t),
          prefix + SAMPLE_SERIALIZE_SYMBOL.get(t),
          prefix + SAMPLE_FINALIZE_SYMBOL.get(t),
          false, false, true));

      // Approximate median
      db.addBuiltin(AggregateFunction.createBuiltin(db, "appx_median",
          Lists.newArrayList(t), t, Type.STRING,
          prefix + SAMPLE_INIT_SYMBOL.get(t),
          prefix + SAMPLE_UPDATE_SYMBOL.get(t),
          prefix + SAMPLE_MERGE_SYMBOL.get(t),
          prefix + SAMPLE_SERIALIZE_SYMBOL.get(t),
          prefix + APPX_MEDIAN_FINALIZE_SYMBOL.get(t),
          false, false, true));

      // Histogram
      db.addBuiltin(AggregateFunction.createBuiltin(db, "histogram",
          Lists.newArrayList(t), Type.STRING, Type.STRING,
          prefix + SAMPLE_INIT_SYMBOL.get(t),
          prefix + SAMPLE_UPDATE_SYMBOL.get(t),
          prefix + SAMPLE_MERGE_SYMBOL.get(t),
          prefix + SAMPLE_SERIALIZE_SYMBOL.get(t),
          prefix + HISTOGRAM_FINALIZE_SYMBOL.get(t),
          false, false, true));

      // NDV
      // TODO: this needs to switch to CHAR(64) as the intermediate type
      db.addBuiltin(AggregateFunction.createBuiltin(db, "ndv",
          Lists.newArrayList(t), Type.BIGINT, Type.STRING,
          prefix + "7HllInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + HLL_UPDATE_SYMBOL.get(t),
          prefix + "8HllMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          prefix + "11HllFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          true, false, true));

      db.addBuiltin(AggregateFunction.createBuiltin(db, "ndv_no_finalize",
          Lists.newArrayList(t), Type.STRING, Type.STRING,
          prefix + "7HllInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + HLL_UPDATE_SYMBOL.get(t),
          prefix + "8HllMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          "_Z20IncrementNdvFinalizePN10impala_udf15FunctionContextERKNS_9StringValE",
          true, false, true));

      // distinctpc
      // TODO: this needs to switch to CHAR(64) as the intermediate type
      db.addBuiltin(AggregateFunction.createBuiltin(db, "distinctpc",
          Lists.newArrayList(t), Type.BIGINT, Type.STRING,
          prefix + "6PcInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + PC_UPDATE_SYMBOL.get(t),
          prefix + "7PcMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          prefix + "10PcFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          false, false, true));

      // distinctpcsa
      // TODO: this needs to switch to CHAR(64) as the intermediate type
      db.addBuiltin(AggregateFunction.createBuiltin(db, "distinctpcsa",
          Lists.newArrayList(t), Type.BIGINT, Type.STRING,
          prefix + "6PcInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + PCSA_UPDATE_SYMBOL.get(t),
          prefix + "7PcMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          prefix + "12PcsaFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          false, false, true));

      if (STDDEV_UPDATE_SYMBOL.containsKey(t)) {
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev",
            Lists.newArrayList(t), Type.DOUBLE, ScalarType.createCharType(24),
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "19KnuthStddevFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev_samp",
            Lists.newArrayList(t), Type.DOUBLE, ScalarType.createCharType(24),
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "19KnuthStddevFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev_pop",
            Lists.newArrayList(t), Type.DOUBLE, ScalarType.createCharType(24),
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "22KnuthStddevPopFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance",
            Lists.newArrayList(t), Type.DOUBLE, ScalarType.createCharType(24),
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "16KnuthVarFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance_samp",
            Lists.newArrayList(t), Type.DOUBLE, ScalarType.createCharType(24),
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "16KnuthVarFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "var_samp",
            Lists.newArrayList(t), Type.DOUBLE, ScalarType.createCharType(24),
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "16KnuthVarFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance_pop",
            Lists.newArrayList(t), Type.DOUBLE, ScalarType.createCharType(24),
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "19KnuthVarPopFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "var_pop",
            Lists.newArrayList(t), Type.DOUBLE, ScalarType.createCharType(24),
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "19KnuthVarPopFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
      }
    }

    // Sum
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum",
        Lists.<Type>newArrayList(Type.BIGINT), Type.BIGINT, Type.BIGINT, initNull,
        prefix + "9SumUpdateIN10impala_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        prefix + "9SumUpdateIN10impala_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        null, null,
        prefix + "9SumRemoveIN10impala_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        null, false, true, false));
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum",
        Lists.<Type>newArrayList(Type.DOUBLE), Type.DOUBLE, Type.DOUBLE, initNull,
        prefix + "9SumUpdateIN10impala_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        prefix + "9SumUpdateIN10impala_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        null, null,
        prefix + "9SumRemoveIN10impala_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        null, false, true, false));
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum",
        Lists.<Type>newArrayList(Type.DECIMAL), Type.DECIMAL, Type.DECIMAL, initNull,
        prefix + "16SumDecimalUpdateEPN10impala_udf15FunctionContextERKNS1_10DecimalValEPS4_",
        prefix + "15SumDecimalMergeEPN10impala_udf15FunctionContextERKNS1_10DecimalValEPS4_",
        null, null,
        prefix + "16SumDecimalRemoveEPN10impala_udf15FunctionContextERKNS1_10DecimalValEPS4_",
        null, false, true, false));

    // Avg
    // TODO: switch to CHAR(sizeof(AvgIntermediateType) when that becomes available
    db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
        Lists.<Type>newArrayList(Type.BIGINT), Type.DOUBLE, Type.STRING,
        prefix + "7AvgInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "9AvgUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
        prefix + "8AvgMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "11AvgGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "9AvgRemoveIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
        prefix + "11AvgFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));
    db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
        Lists.<Type>newArrayList(Type.DOUBLE), Type.DOUBLE, Type.STRING,
        prefix + "7AvgInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "9AvgUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
        prefix + "8AvgMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "11AvgGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "9AvgRemoveIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
        prefix + "11AvgFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));
    db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
        Lists.<Type>newArrayList(Type.DECIMAL), Type.DECIMAL, Type.STRING,
        prefix + "14DecimalAvgInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "16DecimalAvgUpdateEPN10impala_udf15FunctionContextERKNS1_10DecimalValEPNS1_9StringValE",
        prefix + "15DecimalAvgMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "18DecimalAvgGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "16DecimalAvgRemoveEPN10impala_udf15FunctionContextERKNS1_10DecimalValEPNS1_9StringValE",
        prefix + "18DecimalAvgFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));
    // Avg(Timestamp)
    db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
        Lists.<Type>newArrayList(Type.TIMESTAMP), Type.TIMESTAMP, Type.STRING,
        prefix + "7AvgInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "18TimestampAvgUpdateEPN10impala_udf15FunctionContextERKNS1_12TimestampValEPNS1_9StringValE",
        prefix + "8AvgMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "20TimestampAvgGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "18TimestampAvgRemoveEPN10impala_udf15FunctionContextERKNS1_12TimestampValEPNS1_9StringValE",
        prefix + "20TimestampAvgFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    // Group_concat(string)
    db.addBuiltin(AggregateFunction.createBuiltin(db, "group_concat",
        Lists.<Type>newArrayList(Type.STRING), Type.STRING, Type.STRING, initNullString,
        prefix +
            "18StringConcatUpdateEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix +
            "17StringConcatMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix +
            "20StringConcatFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, false, false));
    // Group_concat(string, string)
    db.addBuiltin(AggregateFunction.createBuiltin(db, "group_concat",
        Lists.<Type>newArrayList(Type.STRING, Type.STRING), Type.STRING, Type.STRING,
        initNullString,
        prefix +
            "18StringConcatUpdateEPN10impala_udf15FunctionContextERKNS1_9StringValES6_PS4_",
        prefix +
            "17StringConcatMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix +
            "20StringConcatFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, false, false));

    // analytic functions
    // Rank
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(db, "rank",
        Lists.<Type>newArrayList(), Type.BIGINT, Type.STRING,
        prefix + "8RankInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "10RankUpdateEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        null,
        prefix + "12RankGetValueEPN10impala_udf15FunctionContextERNS1_9StringValE",
        prefix + "12RankFinalizeEPN10impala_udf15FunctionContextERNS1_9StringValE"));
    // Dense rank
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(db, "dense_rank",
        Lists.<Type>newArrayList(), Type.BIGINT, Type.STRING,
        prefix + "8RankInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "15DenseRankUpdateEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        null,
        prefix + "17DenseRankGetValueEPN10impala_udf15FunctionContextERNS1_9StringValE",
        prefix + "12RankFinalizeEPN10impala_udf15FunctionContextERNS1_9StringValE"));
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
        db, "row_number", new ArrayList<Type>(), Type.BIGINT, Type.BIGINT,
        prefix + "8InitZeroIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
        prefix + "15CountStarUpdateEPN10impala_udf15FunctionContextEPNS1_9BigIntValE",
        prefix + "10CountMergeEPN10impala_udf15FunctionContextERKNS1_9BigIntValEPS4_",
        null, null));

    // The following 3 functions are never directly executed because they get rewritten
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
        db, "percent_rank", Lists.<Type>newArrayList(), Type.DOUBLE, Type.STRING));
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
        db, "cume_dist", Lists.<Type>newArrayList(), Type.DOUBLE, Type.STRING));
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
        db, "ntile", Lists.<Type>newArrayList(Type.BIGINT), Type.BIGINT, Type.STRING));

    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue; // NULL is handled through type promotion.
      if (t.isScalarType(PrimitiveType.CHAR)) continue; // promoted to STRING
      if (t.isScalarType(PrimitiveType.VARCHAR)) continue; // promoted to STRING
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
          db, "first_value", Lists.newArrayList(t), t, t,
          t.isStringType() ? initNullString : initNull,
          prefix + FIRST_VALUE_UPDATE_SYMBOL.get(t),
          null,
          t == Type.STRING ? stringValGetValue : null,
          t == Type.STRING ? stringValSerializeOrFinalize : null));
      // Implements FIRST_VALUE for some windows that require rewrites during planning.
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
          db, "first_value_rewrite", Lists.newArrayList(t, Type.BIGINT), t, t,
          t.isStringType() ? initNullString : initNull,
          prefix + FIRST_VALUE_REWRITE_UPDATE_SYMBOL.get(t),
          null,
          t == Type.STRING ? stringValGetValue : null,
          t == Type.STRING ? stringValSerializeOrFinalize : null,
          false));

      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
          db, "last_value", Lists.newArrayList(t), t, t,
          t.isStringType() ? initNullString : initNull,
          prefix + LAST_VALUE_UPDATE_SYMBOL.get(t),
          prefix + LAST_VALUE_REMOVE_SYMBOL.get(t),
          t == Type.STRING ? stringValGetValue : null,
          t == Type.STRING ? stringValSerializeOrFinalize : null));

      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
          db, "lag", Lists.newArrayList(t, Type.BIGINT, t), t, t,
          prefix + OFFSET_FN_INIT_SYMBOL.get(t),
          prefix + OFFSET_FN_UPDATE_SYMBOL.get(t),
          null, null, null));
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
          db, "lead", Lists.newArrayList(t, Type.BIGINT, t), t, t,
          prefix + OFFSET_FN_INIT_SYMBOL.get(t),
          prefix + OFFSET_FN_UPDATE_SYMBOL.get(t),
          null, null, null));

      // lead() and lag() the default offset and the default value should be
      // rewritten to call the overrides that take all parameters.
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
            db, "lag", Lists.newArrayList(t), t, t));
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
            db, "lag", Lists.newArrayList(t, Type.BIGINT), t, t));
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
            db, "lead", Lists.newArrayList(t), t, t));
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
            db, "lead", Lists.newArrayList(t, Type.BIGINT), t, t));
    }
  }
}
