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

package org.apache.impala.catalog;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.impala.analysis.ArithmeticExpr;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CaseExpr;
import org.apache.impala.analysis.CastExpr;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.LikePredicate;
import org.apache.impala.builtins.ScalarBuiltins;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.base.Preconditions;
import org.apache.impala.compat.HiveEsriGeospatialBuiltins;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TGeospatialLibrary;

public class BuiltinsDb extends Db {
  // Size in bytes of AvgState used for integer, floating point, and timestamp avg().
  private static final int AVG_INTERMEDIATE_SIZE = 16;

  // Size in bytes of DecimalAvgState used for decimal avg().
  private static final int DECIMAL_AVG_INTERMEDIATE_SIZE = 24;

  // Size in bytes of KnuthVarianceState used for stddev(), variance(), etc.
  private static final int STDDEV_INTERMEDIATE_SIZE = 24;

  // Size in bytes of probabilistic counting bitmap, used for distinctpc(), etc.
  // Must match PC_INTERMEDIATE_BYTES in aggregate-functions-ir.cc.
  private static final int PC_INTERMEDIATE_SIZE = 256;

  // Size in bytes of the default Hyperloglog intermediate value used for ndv().
  // Must match DEFAULT_HLL_LEN in aggregate-functions-ir.cc.
  private static final int HLL_INTERMEDIATE_SIZE = 1024;

  // Sizes in bytes of all supported HyperLogLog intermediate value used for NDV(),
  // corresponding to precision 9, 10, 11, 12, 13, 14, 15, 16, 17 and 18, respectively
  private static final int[] hll_intermediate_sizes = {
      512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144};

  // A map of the AggregateFunction templates with all known/supported
  // intermediate data types.
  private final Map<Type, List<AggregateFunction>> builtinNDVs_ =
      new HashMap<>();

  // Size in bytes of RankState used for rank() and dense_rank().
  private static final int RANK_INTERMEDIATE_SIZE = 16;

  private static Db INSTANCE;

  public static final String NAME = "_impala_builtins";

  public static synchronized Db getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new BuiltinsDb();
    }
    return INSTANCE;
  }

  public static synchronized Db getInstance(BuiltinsDbLoader loader) {
    if (INSTANCE == null) {
      INSTANCE = loader.getBuiltinsDbInstance();
    }
    return INSTANCE;
  }

  private BuiltinsDb() {
    super(NAME, createMetastoreDb(NAME));
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

    if (BackendConfig.INSTANCE.getGeospatialLibrary().equals(
            TGeospatialLibrary.HIVE_ESRI)) {
      HiveEsriGeospatialBuiltins.initBuiltins(this);
    }
  }

  private static final String BUILTINS_DB_COMMENT = "System database for Impala builtin functions";

  private static Database createMetastoreDb(String name) {
    return new org.apache.hadoop.hive.metastore.api.Database(name,
        BUILTINS_DB_COMMENT, "", Collections.<String,String>emptyMap());
  }

  @Override // FeDb
  public String getOwnerUser() { return null; }

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
        .put(Type.DATE,
            "19ReservoirSampleInitIN10impala_udf7DateValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .build();

  private static final Map<Type, String> SAMPLE_SERIALIZE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "24ReservoirSampleSerializeIN10impala_udf10BooleanValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.TINYINT,
             "24ReservoirSampleSerializeIN10impala_udf10TinyIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.SMALLINT,
             "24ReservoirSampleSerializeIN10impala_udf11SmallIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.INT,
             "24ReservoirSampleSerializeIN10impala_udf6IntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.BIGINT,
             "24ReservoirSampleSerializeIN10impala_udf9BigIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.FLOAT,
             "24ReservoirSampleSerializeIN10impala_udf8FloatValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DOUBLE,
             "24ReservoirSampleSerializeIN10impala_udf9DoubleValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.STRING,
             "24ReservoirSampleSerializeIN10impala_udf9StringValEEES3_PNS2_15FunctionContextERKS3_")
        .put(Type.TIMESTAMP,
             "24ReservoirSampleSerializeIN10impala_udf12TimestampValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DECIMAL,
             "24ReservoirSampleSerializeIN10impala_udf10DecimalValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .put(Type.DATE,
             "24ReservoirSampleSerializeIN10impala_udf7DateValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
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
        .put(Type.DATE,
            "20ReservoirSampleMergeIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKNS2_9StringValEPS6_")
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
        .put(Type.DATE,
            "21ReservoirSampleUpdateIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
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
        .put(Type.DATE,
             "23ReservoirSampleFinalizeIN10impala_udf7DateValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
        .build();

  private static final Map<Type, String> UPDATE_VAL_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "9UpdateValIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DATE,
             "9UpdateValIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DECIMAL,
             "9UpdateValIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TINYINT,
             "9UpdateValIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.SMALLINT,
             "9UpdateValIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TIMESTAMP,
             "9UpdateValIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.INT,
             "9UpdateValIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.FLOAT,
             "9UpdateValIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.BIGINT,
             "9UpdateValIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DOUBLE,
             "9UpdateValIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.STRING,
             "9UpdateValIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .build();

  private static final Map<Type, String> APPX_MEDIAN_FINALIZE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "18AppxMedianFinalizeIN10impala_udf10BooleanValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.DATE,
            "18AppxMedianFinalizeIN10impala_udf7DateValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
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
        .put(Type.DATE,
             "17HistogramFinalizeIN10impala_udf7DateValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
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
        .put(Type.DATE,
            "9HllUpdateIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .build();

  // For ndv() with two input arguments
  private static final Map<Type, String> HLL_UPDATE_SYMBOL_WITH_PRECISION =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "9HllUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
        .put(Type.TINYINT,
             "9HllUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
        .put(Type.SMALLINT,
             "9HllUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
        .put(Type.INT,
             "9HllUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_RKS3_PNS2_9StringValE")
        .put(Type.BIGINT,
             "9HllUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
        .put(Type.FLOAT,
             "9HllUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
        .put(Type.DOUBLE,
             "9HllUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
        .put(Type.STRING,
             "9HllUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPS3_")
        .put(Type.TIMESTAMP,
             "9HllUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
        .put(Type.DECIMAL,
             "9HllUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
        .put(Type.DATE,
             "9HllUpdateIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_RKNS2_6IntValEPNS2_9StringValE")
        .build();

    private static final Map<Type, String> DS_HLL_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.TINYINT,
            "11DsHllUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "11DsHllUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "11DsHllUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "11DsHllUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "11DsHllUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
         // Hive's Datasketches implementation currently uses different hash for STRING and BINARY.
         // Impala only supports it for STRING, but treats it as Hive treats BINARY.
         // See IMPALA-9939 for more details.
        .put(Type.STRING,
            "11DsHllUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .build();

  private static final Map<Type, String> DS_CPC_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.TINYINT,
            "11DsCpcUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "11DsCpcUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "11DsCpcUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "11DsCpcUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "11DsCpcUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.STRING,
            "11DsCpcUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .build();

  private static final Map<Type, String> DS_THETA_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.TINYINT,
            "13DsThetaUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "13DsThetaUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "13DsThetaUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "13DsThetaUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "13DsThetaUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.STRING,
            "13DsThetaUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .build();

  private static final Map<Type, String> SAMPLED_NDV_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "16SampledNdvUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_RKNS2_9DoubleValEPNS2_9StringValE")
        .put(Type.TINYINT,
            "16SampledNdvUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_RKNS2_9DoubleValEPNS2_9StringValE")
        .put(Type.SMALLINT,
            "16SampledNdvUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_RKNS2_9DoubleValEPNS2_9StringValE")
        .put(Type.INT,
            "16SampledNdvUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_RKNS2_9DoubleValEPNS2_9StringValE")
        .put(Type.BIGINT,
            "16SampledNdvUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_RKNS2_9DoubleValEPNS2_9StringValE")
        .put(Type.FLOAT,
            "16SampledNdvUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_RKNS2_9DoubleValEPNS2_9StringValE")
        .put(Type.DOUBLE,
            "16SampledNdvUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKS3_PNS2_9StringValE")
        .put(Type.STRING,
            "16SampledNdvUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_9DoubleValEPS3_")
        .put(Type.TIMESTAMP,
            "16SampledNdvUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_RKNS2_9DoubleValEPNS2_9StringValE")
        .put(Type.DECIMAL,
            "16SampledNdvUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_RKNS2_9DoubleValEPNS2_9StringValE")
        .put(Type.DATE,
            "16SampledNdvUpdateIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_RKNS2_9DoubleValEPNS2_9StringValE")
        .build();

  private static final Map<Type, String> AGGIF_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "11AggIfUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKS3_RKT_PS8_")
        .put(Type.TINYINT,
            "11AggIfUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKNS2_10BooleanValERKT_PS9_")
        .put(Type.SMALLINT,
            "11AggIfUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKNS2_10BooleanValERKT_PS9_")
        .put(Type.INT,
            "11AggIfUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKNS2_10BooleanValERKT_PS9_")
        .put(Type.BIGINT,
            "11AggIfUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKNS2_10BooleanValERKT_PS9_")
        .put(Type.FLOAT,
            "11AggIfUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKNS2_10BooleanValERKT_PS9_")
        .put(Type.DOUBLE,
            "11AggIfUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKNS2_10BooleanValERKT_PS9_")
        .put(Type.STRING,
            "11AggIfUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKNS2_10BooleanValERKT_PS9_")
        .put(Type.TIMESTAMP,
            "11AggIfUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKNS2_10BooleanValERKT_PS9_")
        .put(Type.DECIMAL,
            "11AggIfUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKNS2_10BooleanValERKT_PS9_")
        .put(Type.DATE,
            "11AggIfUpdateIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKNS2_10BooleanValERKT_PS9_")
        .build();

  private static final Map<Type, String> AGGIF_MERGE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "10AggIfMergeIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TINYINT,
            "10AggIfMergeIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.SMALLINT,
            "10AggIfMergeIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.INT,
            "10AggIfMergeIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.BIGINT,
            "10AggIfMergeIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.FLOAT,
            "10AggIfMergeIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DOUBLE,
            "10AggIfMergeIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.STRING,
            "10AggIfMergeIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TIMESTAMP,
            "10AggIfMergeIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DECIMAL,
            "10AggIfMergeIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DATE,
            "10AggIfMergeIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PS6_")
        .build();

  private static final Map<Type, String> AGGIF_FINALIZE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "13AggIfFinalizeIN10impala_udf10BooleanValEEET_PNS2_15FunctionContextERKS4_")
        .put(Type.TINYINT,
            "13AggIfFinalizeIN10impala_udf10TinyIntValEEET_PNS2_15FunctionContextERKS4_")
        .put(Type.SMALLINT,
            "13AggIfFinalizeIN10impala_udf11SmallIntValEEET_PNS2_15FunctionContextERKS4_")
        .put(Type.INT,
            "13AggIfFinalizeIN10impala_udf6IntValEEET_PNS2_15FunctionContextERKS4_")
        .put(Type.BIGINT,
            "13AggIfFinalizeIN10impala_udf9BigIntValEEET_PNS2_15FunctionContextERKS4_")
        .put(Type.FLOAT,
            "13AggIfFinalizeIN10impala_udf8FloatValEEET_PNS2_15FunctionContextERKS4_")
        .put(Type.DOUBLE,
            "13AggIfFinalizeIN10impala_udf9DoubleValEEET_PNS2_15FunctionContextERKS4_")
        .put(Type.STRING,
            "13AggIfFinalizeIN10impala_udf9StringValEEET_PNS2_15FunctionContextERKS4_")
        .put(Type.TIMESTAMP,
            "13AggIfFinalizeIN10impala_udf12TimestampValEEET_PNS2_15FunctionContextERKS4_")
        .put(Type.DECIMAL,
            "13AggIfFinalizeIN10impala_udf10DecimalValEEET_PNS2_15FunctionContextERKS4_")
        .put(Type.DATE,
            "13AggIfFinalizeIN10impala_udf7DateValEEET_PNS2_15FunctionContextERKS4_")
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
         .put(Type.DATE,
            "8PcUpdateIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
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
          .put(Type.DATE,
              "10PcsaUpdateIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
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
        .put(Type.BINARY,
            "3MinIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TIMESTAMP,
            "3MinIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DECIMAL,
            "3MinIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DATE,
            "3MinIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PS6_")
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
        .put(Type.BINARY,
            "3MaxIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TIMESTAMP,
            "3MaxIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DECIMAL,
            "3MaxIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DATE,
            "3MaxIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PS6_")
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
        .put(Type.DATE,
             "12OffsetFnInitIN10impala_udf7DateValEEEvPNS2_15FunctionContextEPT_")
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
        .put(Type.DATE,
             "14OffsetFnUpdateIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
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

  private static final Map<Type, String> FIRST_VALUE_REWRITE_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "21FirstValRewriteUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
        .put(Type.DATE,
             "21FirstValRewriteUpdateIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
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
        .put(Type.DATE,
             "13LastValRemoveIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PS6_")
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

  private static final Map<Type, String> LAST_VALUE_IGNORE_NULLS_INIT_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "22LastValIgnoreNullsInitIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.TINYINT,
            "22LastValIgnoreNullsInitIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.SMALLINT,
            "22LastValIgnoreNullsInitIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.INT,
            "22LastValIgnoreNullsInitIN10impala_udf6IntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.BIGINT,
            "22LastValIgnoreNullsInitIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.FLOAT,
            "22LastValIgnoreNullsInitIN10impala_udf8FloatValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.DOUBLE,
            "22LastValIgnoreNullsInitIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.STRING,
            "22LastValIgnoreNullsInitIN10impala_udf9StringValEEEvPNS2_15FunctionContextEPS3_")
        .put(Type.TIMESTAMP,
            "22LastValIgnoreNullsInitIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.DECIMAL,
            "22LastValIgnoreNullsInitIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .put(Type.DATE,
            "22LastValIgnoreNullsInitIN10impala_udf7DateValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
        .build();

  private static final Map<Type, String> LAST_VALUE_IGNORE_NULLS_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "24LastValIgnoreNullsUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DATE,
            "24LastValIgnoreNullsUpdateIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DECIMAL,
            "24LastValIgnoreNullsUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.TINYINT,
            "24LastValIgnoreNullsUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.SMALLINT,
            "24LastValIgnoreNullsUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.TIMESTAMP,
            "24LastValIgnoreNullsUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "24LastValIgnoreNullsUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "24LastValIgnoreNullsUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "24LastValIgnoreNullsUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "24LastValIgnoreNullsUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.STRING,
            "24LastValIgnoreNullsUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .build();

  private static final Map<Type, String> LAST_VALUE_IGNORE_NULLS_REMOVE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "24LastValIgnoreNullsRemoveIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DATE,
            "24LastValIgnoreNullsRemoveIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DECIMAL,
            "24LastValIgnoreNullsRemoveIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.TINYINT,
            "24LastValIgnoreNullsRemoveIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.SMALLINT,
            "24LastValIgnoreNullsRemoveIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.TIMESTAMP,
            "24LastValIgnoreNullsRemoveIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.INT,
            "24LastValIgnoreNullsRemoveIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.FLOAT,
            "24LastValIgnoreNullsRemoveIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.BIGINT,
            "24LastValIgnoreNullsRemoveIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.DOUBLE,
            "24LastValIgnoreNullsRemoveIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(Type.STRING,
            "24LastValIgnoreNullsRemoveIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .build();

  private static final Map<Type, String> LAST_VALUE_IGNORE_NULLS_GET_VALUE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "26LastValIgnoreNullsGetValueIN10impala_udf10BooleanValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.TINYINT,
            "26LastValIgnoreNullsGetValueIN10impala_udf10TinyIntValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.SMALLINT,
            "26LastValIgnoreNullsGetValueIN10impala_udf11SmallIntValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.INT,
            "26LastValIgnoreNullsGetValueIN10impala_udf6IntValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.BIGINT,
            "26LastValIgnoreNullsGetValueIN10impala_udf9BigIntValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.FLOAT,
            "26LastValIgnoreNullsGetValueIN10impala_udf8FloatValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.DOUBLE,
            "26LastValIgnoreNullsGetValueIN10impala_udf9DoubleValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.STRING,
            "26LastValIgnoreNullsGetValueIN10impala_udf9StringValEEET_PNS2_15FunctionContextERKS3_")
        .put(Type.TIMESTAMP,
            "26LastValIgnoreNullsGetValueIN10impala_udf12TimestampValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.DECIMAL,
            "26LastValIgnoreNullsGetValueIN10impala_udf10DecimalValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.DATE,
            "26LastValIgnoreNullsGetValueIN10impala_udf7DateValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .build();

  private static final Map<Type, String> LAST_VALUE_IGNORE_NULLS_FINALIZE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "26LastValIgnoreNullsFinalizeIN10impala_udf10BooleanValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.TINYINT,
            "26LastValIgnoreNullsFinalizeIN10impala_udf10TinyIntValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.SMALLINT,
            "26LastValIgnoreNullsFinalizeIN10impala_udf11SmallIntValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.INT,
            "26LastValIgnoreNullsFinalizeIN10impala_udf6IntValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.BIGINT,
            "26LastValIgnoreNullsFinalizeIN10impala_udf9BigIntValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.FLOAT,
            "26LastValIgnoreNullsFinalizeIN10impala_udf8FloatValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.DOUBLE,
            "26LastValIgnoreNullsFinalizeIN10impala_udf9DoubleValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.STRING,
            "26LastValIgnoreNullsFinalizeIN10impala_udf9StringValEEET_PNS2_15FunctionContextERKS3_")
        .put(Type.TIMESTAMP,
            "26LastValIgnoreNullsFinalizeIN10impala_udf12TimestampValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.DECIMAL,
            "26LastValIgnoreNullsFinalizeIN10impala_udf10DecimalValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .put(Type.DATE,
            "26LastValIgnoreNullsFinalizeIN10impala_udf7DateValEEET_PNS2_15FunctionContextERKNS2_9StringValE")
        .build();

  private static final Map<Type, String> FIRST_VALUE_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
             "14FirstValUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DATE,
             "14FirstValUpdateIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PS6_")
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

  private static final Map<Type, String> FIRST_VALUE_IGNORE_NULLS_UPDATE_SYMBOL =
      ImmutableMap.<Type, String>builder()
        .put(Type.BOOLEAN,
            "25FirstValIgnoreNullsUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DATE,
            "25FirstValIgnoreNullsUpdateIN10impala_udf7DateValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DECIMAL,
            "25FirstValIgnoreNullsUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TINYINT,
            "25FirstValIgnoreNullsUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.SMALLINT,
            "25FirstValIgnoreNullsUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.TIMESTAMP,
            "25FirstValIgnoreNullsUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.INT,
            "25FirstValIgnoreNullsUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.FLOAT,
            "25FirstValIgnoreNullsUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.BIGINT,
            "25FirstValIgnoreNullsUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.DOUBLE,
            "25FirstValIgnoreNullsUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(Type.STRING,
            "25FirstValIgnoreNullsUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .build();

  // A helper function to return a template aggregation function for NDV with 2 arguments,
  // for data type t. Inputs:
  //  db: the db;
  //  prefix: the prefix string for each function call symbol in BE;
  //  t: the type to build the template for;
  //  hllIntermediateType: the intermediate data type.
  private static AggregateFunction createTemplateAggregateFunctionForNDVWith2Args(
      Db db, String prefix, Type t, Type hllIntermediateType) {
    return AggregateFunction.createBuiltin(db, "ndv", Lists.newArrayList(t, Type.INT),
        Type.BIGINT, hllIntermediateType,
        prefix + "7HllInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + HLL_UPDATE_SYMBOL_WITH_PRECISION.get(t),
        prefix + "8HllMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_", null,
        prefix + "11HllFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE", true,
        false, true);
  }

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
        new ArrayList<>(), Type.BIGINT, Type.BIGINT,
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
    }

    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue; // NULL is handled through type promotion.
      if (t.isScalarType(PrimitiveType.CHAR)) continue; // promoted to STRING
      if (t.isScalarType(PrimitiveType.VARCHAR)) continue; // promoted to STRING
      if (t.isBinary()) continue; // Only supported for count/min/max

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
      // Setup the intermediate type based on the default precision in the template
      // function in the db. This type is useful when the default precision is all
      // needed in the ndv().
      Type defaultHllIntermediateType =
          ScalarType.createFixedUdaIntermediateType(HLL_INTERMEDIATE_SIZE);

      // Single input argument version
      db.addBuiltin(AggregateFunction.createBuiltin(db, "ndv", Lists.newArrayList(t),
          Type.BIGINT, defaultHllIntermediateType,
          prefix + "7HllInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + HLL_UPDATE_SYMBOL.get(t),
          prefix + "8HllMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          null,
          prefix + "11HllFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          true, false, true));

      // Double input argument version, with the unique HllUpdate function symbols.
      // Take an intermediate data type with the default length for now. During the
      // analysis phase, the data type will be resolved to the correct template, based
      // on the the value in the 2nd argument.
      db.addBuiltin(createTemplateAggregateFunctionForNDVWith2Args(
          db, prefix, t, defaultHllIntermediateType));

      // For each type t, populate the hash map of AggregateFunctions with
      // all known intermediate data types.
      List<AggregateFunction> ndvList = new ArrayList<AggregateFunction>();
      for (int size : hll_intermediate_sizes) {
        Type hllIntermediateType = ScalarType.createFixedUdaIntermediateType(size);
        ndvList.add(createTemplateAggregateFunctionForNDVWith2Args(
            db, prefix, t, hllIntermediateType));
      }
      builtinNDVs_.put(t, ndvList);

      // Used in stats computation. Will take a single input argument only.
      db.addBuiltin(AggregateFunction.createBuiltin(db, "ndv_no_finalize",
          Lists.newArrayList(t), Type.STRING, defaultHllIntermediateType,
          prefix + "7HllInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + HLL_UPDATE_SYMBOL.get(t),
          prefix + "8HllMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          null,
          "_Z20IncrementNdvFinalizePN10impala_udf15FunctionContextERKNS_9StringValE",
          true, false, true));

      // DataSketches HLL
      if (DS_HLL_UPDATE_SYMBOL.containsKey(t)) {
        db.addBuiltin(AggregateFunction.createBuiltin(db, "ds_hll_sketch_and_estimate",
            Lists.newArrayList(t), Type.BIGINT, Type.STRING,
            prefix + "9DsHllInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + DS_HLL_UPDATE_SYMBOL.get(t),
            prefix + "10DsHllMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            prefix + "14DsHllSerializeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            prefix + "13DsHllFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true, false, true));

        db.addBuiltin(AggregateFunction.createBuiltin(db, "ds_hll_sketch",
            Lists.newArrayList(t), Type.STRING, Type.STRING,
            prefix + "9DsHllInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + DS_HLL_UPDATE_SYMBOL.get(t),
            prefix + "10DsHllMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            prefix + "14DsHllSerializeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            prefix + "19DsHllFinalizeSketchEPN10impala_udf15FunctionContextERKNS1_" +
                "9StringValE", true, false, true));
      } else {
        db.addBuiltin(AggregateFunction.createUnsupportedBuiltin(db,
            "ds_hll_sketch_and_estimate", Lists.newArrayList(t), Type.STRING,
            Type.STRING));
        db.addBuiltin(AggregateFunction.createUnsupportedBuiltin(db, "ds_hll_sketch",
            Lists.newArrayList(t), Type.STRING, Type.STRING));
      }

      // DataSketches CPC
      if (DS_CPC_UPDATE_SYMBOL.containsKey(t)) {
        db.addBuiltin(AggregateFunction.createBuiltin(db, "ds_cpc_sketch_and_estimate",
            Lists.newArrayList(t), Type.BIGINT, Type.STRING,
            prefix + "9DsCpcInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + DS_CPC_UPDATE_SYMBOL.get(t),
            prefix + "10DsCpcMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            prefix + "14DsCpcSerializeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            prefix + "13DsCpcFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true, false, true));

        db.addBuiltin(AggregateFunction.createBuiltin(db, "ds_cpc_sketch",
            Lists.newArrayList(t), Type.STRING, Type.STRING,
            prefix + "9DsCpcInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + DS_CPC_UPDATE_SYMBOL.get(t),
            prefix + "10DsCpcMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            prefix + "14DsCpcSerializeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            prefix + "19DsCpcFinalizeSketchEPN10impala_udf15FunctionContextERKNS1_" +
                "9StringValE", true, false, true));
      } else {
        db.addBuiltin(AggregateFunction.createUnsupportedBuiltin(db,
            "ds_cpc_sketch_and_estimate", Lists.newArrayList(t), Type.STRING,
            Type.STRING));
        db.addBuiltin(AggregateFunction.createUnsupportedBuiltin(db, "ds_cpc_sketch",
                Lists.newArrayList(t), Type.STRING, Type.STRING));
      }

      // DataSketches Theta
      if (DS_THETA_UPDATE_SYMBOL.containsKey(t)) {
        db.addBuiltin(AggregateFunction.createBuiltin(db, "ds_theta_sketch_and_estimate",
                Lists.newArrayList(t), Type.BIGINT, Type.STRING,
                prefix + "11DsThetaInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
                prefix + DS_THETA_UPDATE_SYMBOL.get(t),
                prefix + "12DsThetaMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "16DsThetaSerializeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
                prefix + "15DsThetaFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
                true, false, true));

        db.addBuiltin(AggregateFunction.createBuiltin(db, "ds_theta_sketch",
                Lists.newArrayList(t), Type.STRING, Type.STRING,
                prefix + "11DsThetaInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
                prefix + DS_THETA_UPDATE_SYMBOL.get(t),
                prefix + "12DsThetaMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "16DsThetaSerializeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
                prefix + "21DsThetaFinalizeSketchEPN10impala_udf15FunctionContextERKNS1_" +
                        "9StringValE", true, false, true));
      } else {
        db.addBuiltin(AggregateFunction.createUnsupportedBuiltin(db,
                "ds_theta_sketch_and_estimate", Lists.newArrayList(t), Type.STRING,
                Type.STRING));
        db.addBuiltin(AggregateFunction.createUnsupportedBuiltin(db, "ds_theta_sketch",
                Lists.newArrayList(t), Type.STRING, Type.STRING));
      }

      // SAMPLED_NDV.
      // Size needs to be kept in sync with SampledNdvState in the BE.
      int NUM_HLL_BUCKETS = 32;
      int size = 16 + NUM_HLL_BUCKETS * (8 + HLL_INTERMEDIATE_SIZE);
      Type sampledIntermediateType = ScalarType.createFixedUdaIntermediateType(size);
      db.addBuiltin(AggregateFunction.createBuiltin(db, "sampled_ndv",
          Lists.newArrayList(t, Type.DOUBLE), Type.BIGINT, sampledIntermediateType,
          prefix + "14SampledNdvInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + SAMPLED_NDV_UPDATE_SYMBOL.get(t),
          prefix + "15SampledNdvMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          null,
          prefix + "18SampledNdvFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          true, false, true));

      db.addBuiltin(AggregateFunction.createBuiltin(db, "aggif",
          Lists.newArrayList(ScalarType.BOOLEAN, t), t, t,
          initNull,
          prefix + AGGIF_UPDATE_SYMBOL.get(t),
          prefix + AGGIF_MERGE_SYMBOL.get(t),
          null,
          prefix + AGGIF_FINALIZE_SYMBOL.get(t),
          true, false, true));

      Type pcIntermediateType =
          ScalarType.createFixedUdaIntermediateType(PC_INTERMEDIATE_SIZE);
      // distinctpc
      db.addBuiltin(AggregateFunction.createBuiltin(db, "distinctpc",
          Lists.newArrayList(t), Type.BIGINT, pcIntermediateType,
          prefix + "6PcInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + PC_UPDATE_SYMBOL.get(t),
          prefix + "7PcMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          null,
          prefix + "10PcFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          false, false, true));

      // distinctpcsa
      db.addBuiltin(AggregateFunction.createBuiltin(db, "distinctpcsa",
          Lists.newArrayList(t), Type.BIGINT, pcIntermediateType,
          prefix + "6PcInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + PCSA_UPDATE_SYMBOL.get(t),
          prefix + "7PcMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          null,
          prefix + "12PcsaFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          false, false, true));

      if (STDDEV_UPDATE_SYMBOL.containsKey(t)) {
        Type stddevIntermediateType =
            ScalarType.createFixedUdaIntermediateType(STDDEV_INTERMEDIATE_SIZE);
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev",
            Lists.newArrayList(t), Type.DOUBLE, stddevIntermediateType,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "19KnuthStddevFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev_samp",
            Lists.newArrayList(t), Type.DOUBLE, stddevIntermediateType,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "19KnuthStddevFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev_pop",
            Lists.newArrayList(t), Type.DOUBLE, stddevIntermediateType,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "22KnuthStddevPopFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance",
            Lists.newArrayList(t), Type.DOUBLE, stddevIntermediateType,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "16KnuthVarFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance_samp",
            Lists.newArrayList(t), Type.DOUBLE, stddevIntermediateType,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "16KnuthVarFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "var_samp",
            Lists.newArrayList(t), Type.DOUBLE, stddevIntermediateType,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "16KnuthVarFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance_pop",
            Lists.newArrayList(t), Type.DOUBLE, stddevIntermediateType,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            null,
            prefix + "19KnuthVarPopFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            false, false, false));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "var_pop",
            Lists.newArrayList(t), Type.DOUBLE, stddevIntermediateType,
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

    // Sum that returns zero on an empty input.
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum_init_zero",
        Lists.<Type>newArrayList(Type.BIGINT), Type.BIGINT, Type.BIGINT,
        prefix + "8InitZeroIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
        prefix + "9SumUpdateIN10impala_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        prefix + "9SumUpdateIN10impala_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        null, null,
        prefix + "9SumRemoveIN10impala_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        null, false, true, true));

    // Corr()
    db.addBuiltin(AggregateFunction.createBuiltin(db, "corr",
        Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.STRING,
        prefix + "8CorrInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "10CorrUpdateEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9StringValE",
        prefix + "9CorrMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "12CorrGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "10CorrRemoveEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9StringValE",
        prefix + "12CorrFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    db.addBuiltin(AggregateFunction.createBuiltin(db, "corr",
        Lists.<Type>newArrayList(Type.TIMESTAMP, Type.TIMESTAMP), Type.DOUBLE, Type.STRING,
        prefix + "8CorrInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix +
                "19TimestampCorrUpdateEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9StringValE",
        prefix + "9CorrMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "12CorrGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix +
                "19TimestampCorrRemoveEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9StringValE",
        prefix + "12CorrFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    // Regr_count()
    db.addBuiltin(AggregateFunction.createBuiltin(db, "regr_count",
        Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE), Type.BIGINT, Type.BIGINT,
        prefix + "8InitZeroIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
        prefix + "15RegrCountUpdateEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9BigIntValE",
        prefix + "10CountMergeEPN10impala_udf15FunctionContextERKNS1_9BigIntValEPS4_",
        null, null,
        prefix + "15RegrCountRemoveEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9BigIntValE",
        null, false, true, true));

    db.addBuiltin(AggregateFunction.createBuiltin(db, "regr_count",
        Lists.<Type>newArrayList(Type.TIMESTAMP, Type.TIMESTAMP), Type.BIGINT, Type.BIGINT,
        prefix + "8InitZeroIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
        prefix + "24TimestampRegrCountUpdateEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9BigIntValE",
        prefix + "10CountMergeEPN10impala_udf15FunctionContextERKNS1_9BigIntValEPS4_",
        null, null,
        prefix + "24TimestampRegrCountRemoveEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9BigIntValE",
        null, false, true, true));

    // Regr_r2()
    db.addBuiltin(AggregateFunction.createBuiltin(db, "regr_r2",
        Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.STRING,
        prefix + "8CorrInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "10CorrUpdateEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9StringValE",
        prefix + "9CorrMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "15Regr_r2GetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "10CorrRemoveEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9StringValE",
        prefix + "15Regr_r2FinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    db.addBuiltin(AggregateFunction.createBuiltin(db, "regr_r2",
        Lists.<Type>newArrayList(Type.TIMESTAMP, Type.TIMESTAMP), Type.DOUBLE, Type.STRING,
        prefix + "8CorrInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix +
                "19TimestampCorrUpdateEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9StringValE",
        prefix + "9CorrMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "15Regr_r2GetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix +
                "19TimestampCorrRemoveEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9StringValE",
        prefix + "15Regr_r2FinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    //Regr_slope()
    db.addBuiltin(AggregateFunction.createBuiltin(db, "regr_slope",
        Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.STRING,
        prefix + "13RegrSlopeInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "15RegrSlopeUpdateEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9StringValE",
        prefix + "14RegrSlopeMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "17RegrSlopeGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "15RegrSlopeRemoveEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9StringValE",
        prefix + "17RegrSlopeFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    db.addBuiltin(AggregateFunction.createBuiltin(db, "regr_slope",
        Lists.<Type>newArrayList(Type.TIMESTAMP, Type.TIMESTAMP), Type.DOUBLE, Type.STRING,
        prefix + "13RegrSlopeInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "24TimestampRegrSlopeUpdateEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9StringValE",
        prefix + "14RegrSlopeMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "17RegrSlopeGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "24TimestampRegrSlopeRemoveEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9StringValE",
        prefix + "17RegrSlopeFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    // Regr_intercept()
    db.addBuiltin(AggregateFunction.createBuiltin(db, "regr_intercept",
        Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.STRING,
        prefix + "13RegrSlopeInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "15RegrSlopeUpdateEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9StringValE",
        prefix + "14RegrSlopeMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "21RegrInterceptGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "15RegrSlopeRemoveEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9StringValE",
        prefix + "21RegrInterceptFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    db.addBuiltin(AggregateFunction.createBuiltin(db, "regr_intercept",
        Lists.<Type>newArrayList(Type.TIMESTAMP, Type.TIMESTAMP), Type.DOUBLE, Type.STRING,
        prefix + "13RegrSlopeInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "24TimestampRegrSlopeUpdateEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9StringValE",
        prefix + "14RegrSlopeMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "21RegrInterceptGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "24TimestampRegrSlopeRemoveEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9StringValE",
        prefix + "21RegrInterceptFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    // Covar_samp()
    db.addBuiltin(AggregateFunction.createBuiltin(db, "covar_samp",
        Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.STRING,
        prefix + "9CovarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "11CovarUpdateEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9StringValE",
        prefix + "10CovarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "19CovarSampleGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "11CovarRemoveEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9StringValE",
        prefix + "19CovarSampleFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    db.addBuiltin(AggregateFunction.createBuiltin(db, "covar_samp",
        Lists.<Type>newArrayList(Type.TIMESTAMP, Type.TIMESTAMP), Type.DOUBLE, Type.STRING,
        prefix + "9CovarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix +
                "20TimestampCovarUpdateEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9StringValE",
        prefix + "10CovarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "19CovarSampleGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix +
                "20TimestampCovarRemoveEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9StringValE",
        prefix + "19CovarSampleFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    // Covar_pop()
    db.addBuiltin(AggregateFunction.createBuiltin(db, "covar_pop",
        Lists.<Type>newArrayList(Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.STRING,
        prefix + "9CovarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "11CovarUpdateEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9StringValE",
        prefix + "10CovarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "23CovarPopulationGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "11CovarRemoveEPN10impala_udf15FunctionContextERKNS1_9DoubleValES6_PNS1_9StringValE",
        prefix + "23CovarPopulationFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    db.addBuiltin(AggregateFunction.createBuiltin(db, "covar_pop",
        Lists.<Type>newArrayList(Type.TIMESTAMP, Type.TIMESTAMP), Type.DOUBLE, Type.STRING,
        prefix + "9CovarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix +
                "20TimestampCovarUpdateEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9StringValE",
        prefix + "10CovarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize,
        prefix + "23CovarPopulationGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix +
                "20TimestampCovarRemoveEPN10impala_udf15FunctionContextERKNS1_12TimestampValES6_PNS1_9StringValE",
        prefix + "23CovarPopulationFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));

    // Avg
    Type avgIntermediateType =
        ScalarType.createFixedUdaIntermediateType(AVG_INTERMEDIATE_SIZE);
    Type decimalAvgIntermediateType =
        ScalarType.createFixedUdaIntermediateType(DECIMAL_AVG_INTERMEDIATE_SIZE);
    db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
        Lists.<Type>newArrayList(Type.BIGINT), Type.DOUBLE, avgIntermediateType,
        prefix + "7AvgInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "9AvgUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
        prefix + "8AvgMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        null,
        prefix + "11AvgGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "9AvgRemoveIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
        prefix + "11AvgFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));
    db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
        Lists.<Type>newArrayList(Type.DOUBLE), Type.DOUBLE, avgIntermediateType,
        prefix + "7AvgInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "9AvgUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
        prefix + "8AvgMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        null,
        prefix + "11AvgGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "9AvgRemoveIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
        prefix + "11AvgFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));
    db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
        Lists.<Type>newArrayList(Type.DECIMAL), Type.DECIMAL, decimalAvgIntermediateType,
        prefix + "14DecimalAvgInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "16DecimalAvgUpdateEPN10impala_udf15FunctionContextERKNS1_10DecimalValEPNS1_9StringValE",
        prefix + "15DecimalAvgMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        null,
        prefix + "18DecimalAvgGetValueEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "16DecimalAvgRemoveEPN10impala_udf15FunctionContextERKNS1_10DecimalValEPNS1_9StringValE",
        prefix + "18DecimalAvgFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        false, true, false));
    // Avg(Timestamp)
    db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
        Lists.<Type>newArrayList(Type.TIMESTAMP), Type.TIMESTAMP, avgIntermediateType,
        prefix + "7AvgInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "18TimestampAvgUpdateEPN10impala_udf15FunctionContextERKNS1_12TimestampValEPNS1_9StringValE",
        prefix + "8AvgMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        null,
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
    Type rankIntermediateType =
        ScalarType.createFixedUdaIntermediateType(RANK_INTERMEDIATE_SIZE);
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(db, "rank",
        Lists.<Type>newArrayList(), Type.BIGINT, rankIntermediateType,
        prefix + "8RankInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "10RankUpdateEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        null,
        prefix + "12RankGetValueEPN10impala_udf15FunctionContextERNS1_9StringValE",
        prefix + "12RankFinalizeEPN10impala_udf15FunctionContextERNS1_9StringValE"));
    // Dense rank
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(db, "dense_rank",
        Lists.<Type>newArrayList(), Type.BIGINT, rankIntermediateType,
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

    // DataSketches HLL sketch
    db.addBuiltin(AggregateFunction.createBuiltin(db, "ds_kll_sketch",
        Lists.<Type>newArrayList(Type.FLOAT), Type.STRING, Type.STRING,
        prefix + "9DsKllInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix + "11DsKllUpdateEPN10impala_udf15FunctionContextERKNS1_8FloatValEPNS1_" +
            "9StringValE",
        prefix + "10DsKllMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix + "14DsKllSerializeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix + "19DsKllFinalizeSketchEPN10impala_udf15FunctionContextERKNS1_" +
            "9StringValE", true, false, true));

    // DataSketches KLL union
    db.addBuiltin(AggregateFunction.createBuiltin(db, "ds_kll_union",
        Lists.<Type>newArrayList(Type.STRING), Type.STRING, Type.STRING,
        prefix + "14DsKllUnionInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix +
            "16DsKllUnionUpdateEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix +
            "15DsKllUnionMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix +
            "19DsKllUnionSerializeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix +
            "18DsKllUnionFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        true, false, true));

    // The following 3 functions are never directly executed because they get rewritten
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
        db, "percent_rank", Lists.<Type>newArrayList(), Type.DOUBLE, Type.STRING));
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
        db, "cume_dist", Lists.<Type>newArrayList(), Type.DOUBLE, Type.STRING));
    db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
        db, "ntile", Lists.<Type>newArrayList(Type.BIGINT), Type.BIGINT, Type.STRING));

    // DataSketches HLL union
    db.addBuiltin(AggregateFunction.createBuiltin(db, "ds_hll_union",
        Lists.<Type>newArrayList(Type.STRING), Type.STRING, Type.STRING,
        prefix + "14DsHllUnionInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix +
            "16DsHllUnionUpdateEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix +
            "15DsHllUnionMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix +
            "19DsHllUnionSerializeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix +
            "18DsHllUnionFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        true, false, true));

    // DataSketches CPC union
    db.addBuiltin(AggregateFunction.createBuiltin(db, "ds_cpc_union",
        Lists.<Type>newArrayList(Type.STRING), Type.STRING, Type.STRING,
        prefix + "14DsCpcUnionInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix +
            "16DsCpcUnionUpdateEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix +
            "15DsCpcUnionMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix +
            "19DsCpcUnionSerializeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix +
            "18DsCpcUnionFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        true, false, true));

    // DataSketches Theta union
    db.addBuiltin(AggregateFunction.createBuiltin(db, "ds_theta_union",
        Lists.<Type>newArrayList(Type.STRING), Type.STRING, Type.STRING,
        prefix + "16DsThetaUnionInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix +
            "18DsThetaUnionUpdateEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix +
            "17DsThetaUnionMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix +
            "21DsThetaUnionSerializeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix +
            "20DsThetaUnionFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        true, false, true));

    // DataSketches Theta intersect
    db.addBuiltin(AggregateFunction.createBuiltin(db, "ds_theta_intersect",
        Lists.<Type>newArrayList(Type.STRING), Type.STRING, Type.STRING,
        prefix + "20DsThetaIntersectInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
        prefix +
            "22DsThetaIntersectUpdateEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix +
            "21DsThetaIntersectMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix +
            "25DsThetaIntersectSerializeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        prefix +
            "24DsThetaIntersectFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
        true, false, true));

    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue; // NULL is handled through type promotion.
      if (t.isScalarType(PrimitiveType.CHAR)) continue; // promoted to STRING
      if (t.isScalarType(PrimitiveType.VARCHAR)) continue; // promoted to STRING
      // BINARY is not supported for analytic functions.
      if (t.isBinary()) continue;
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
          db, "first_value_ignore_nulls", Lists.newArrayList(t), t, t,
          t.isStringType() ? initNullString : initNull,
          prefix + FIRST_VALUE_IGNORE_NULLS_UPDATE_SYMBOL.get(t),
          null,
          t == Type.STRING ? stringValGetValue : null,
          t == Type.STRING ? stringValSerializeOrFinalize : null,
          false));

      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
          db, "last_value", Lists.newArrayList(t), t, t,
          t.isStringType() ? initNullString : initNull,
          prefix + UPDATE_VAL_SYMBOL.get(t),
          prefix + LAST_VALUE_REMOVE_SYMBOL.get(t),
          t == Type.STRING ? stringValGetValue : null,
          t == Type.STRING ? stringValSerializeOrFinalize : null));

      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
          db, "last_value_ignore_nulls", Lists.newArrayList(t), t, Type.STRING,
          prefix + LAST_VALUE_IGNORE_NULLS_INIT_SYMBOL.get(t),
          prefix + LAST_VALUE_IGNORE_NULLS_UPDATE_SYMBOL.get(t),
          prefix + LAST_VALUE_IGNORE_NULLS_REMOVE_SYMBOL.get(t),
          prefix + LAST_VALUE_IGNORE_NULLS_GET_VALUE_SYMBOL.get(t),
          prefix + LAST_VALUE_IGNORE_NULLS_FINALIZE_SYMBOL.get(t),
          false));

      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
          db, "lag", Lists.newArrayList(t, Type.BIGINT, t), t, t,
          prefix + OFFSET_FN_INIT_SYMBOL.get(t),
          prefix + OFFSET_FN_UPDATE_SYMBOL.get(t),
          null,
          t == Type.STRING ? stringValGetValue : null,
          t == Type.STRING ? stringValSerializeOrFinalize : null));
      db.addBuiltin(AggregateFunction.createAnalyticBuiltin(
          db, "lead", Lists.newArrayList(t, Type.BIGINT, t), t, t,
          prefix + OFFSET_FN_INIT_SYMBOL.get(t),
          prefix + OFFSET_FN_UPDATE_SYMBOL.get(t),
          null,
          t == Type.STRING ? stringValGetValue : null,
          t == Type.STRING ? stringValSerializeOrFinalize : null));

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

    // Grouping ID functions for grouping sets. These are rewritten during analysis.
    db.addBuiltin(AggregateFunction.createRewrittenBuiltin(db, "grouping_id",
        Collections.<Type>emptyList(), Type.BIGINT, /*ignoresDistinct=*/ true,
        /*isAnalyticFn=*/ false, /*returnsNonNullOnEmpty=*/ true));
    // grouping() can take any grouping expression as input.
    for (Type t: Type.getSupportedTypes()) {
      db.addBuiltin(AggregateFunction.createRewrittenBuiltin(db, "grouping",
          Lists.newArrayList(t), Type.TINYINT, /*ignoresDistinct=*/ true,
          /*isAnalyticFn=*/ false, /*returnsNonNullOnEmpty=*/ true));
    }
  }

  // Resolve the intermediate data type by searching for one in the
  // map builtinNDVs_ that has the desired length.
  public AggregateFunction resolveNdvIntermediateType(
      AggregateFunction func, int length) {
    Preconditions.checkState(func.getNumArgs() >= 1);
    List<AggregateFunction> list = builtinNDVs_.get(func.getArgs()[0]);
    for (AggregateFunction aggF : list) {
      ScalarType sType = (ScalarType) aggF.getIntermediateType();
      if (sType.getLength() == length) return aggF;
    }
    return null;
  }

  /**
   * BuiltinsDbLoader allows a third party extension to create their own BuiltinsDb.
   */
  public interface BuiltinsDbLoader {
    Db getBuiltinsDbInstance();
  }
}
