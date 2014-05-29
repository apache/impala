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
import com.cloudera.impala.analysis.LikePredicate;
import com.cloudera.impala.builtins.ScalarBuiltins;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class BuiltinsDb extends Db {
  public BuiltinsDb(String name, Catalog catalog) {
    super(name, catalog);
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
    LikePredicate.initBuiltins(this);
    ScalarBuiltins.initBuiltins(this);
  }

  private static final Map<ColumnType, String> HLL_UPDATE_SYMBOL =
      ImmutableMap.<ColumnType, String>builder()
        .put(ColumnType.BOOLEAN,
            "9HllUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.TINYINT,
            "9HllUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.SMALLINT,
            "9HllUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.INT,
            "9HllUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.BIGINT,
            "9HllUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.FLOAT,
            "9HllUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.DOUBLE,
            "9HllUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.STRING,
            "9HllUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .put(ColumnType.TIMESTAMP,
            "9HllUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.DECIMAL,
            "9HllUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .build();

  private static final Map<ColumnType, String> PC_UPDATE_SYMBOL =
      ImmutableMap.<ColumnType, String>builder()
        .put(ColumnType.BOOLEAN,
            "8PcUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.TINYINT,
            "8PcUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.SMALLINT,
            "8PcUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.INT,
            "8PcUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.BIGINT,
            "8PcUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.FLOAT,
            "8PcUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.DOUBLE,
            "8PcUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.STRING,
            "8PcUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .put(ColumnType.TIMESTAMP,
            "8PcUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
         .put(ColumnType.DECIMAL,
            "8PcUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .build();

    private static final Map<ColumnType, String> PCSA_UPDATE_SYMBOL =
      ImmutableMap.<ColumnType, String>builder()
          .put(ColumnType.BOOLEAN,
              "10PcsaUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.TINYINT,
              "10PcsaUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.SMALLINT,
              "10PcsaUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.INT,
              "10PcsaUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.BIGINT,
              "10PcsaUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.FLOAT,
              "10PcsaUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.DOUBLE,
              "10PcsaUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.STRING,
              "10PcsaUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
          .put(ColumnType.TIMESTAMP,
              "10PcsaUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.DECIMAL,
              "10PcsaUpdateIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .build();

  private static final Map<ColumnType, String> MIN_UPDATE_SYMBOL =
      ImmutableMap.<ColumnType, String>builder()
        .put(ColumnType.BOOLEAN,
            "3MinIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.TINYINT,
            "3MinIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.SMALLINT,
            "3MinIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.INT,
            "3MinIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.BIGINT,
            "3MinIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.FLOAT,
            "3MinIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.DOUBLE,
            "3MinIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.STRING,
            "3MinIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.TIMESTAMP,
            "3MinIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.DECIMAL,
            "3MinIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .build();

  private static final Map<ColumnType, String> MAX_UPDATE_SYMBOL =
      ImmutableMap.<ColumnType, String>builder()
        .put(ColumnType.BOOLEAN,
            "3MaxIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.TINYINT,
            "3MaxIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.SMALLINT,
            "3MaxIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.INT,
            "3MaxIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.BIGINT,
            "3MaxIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.FLOAT,
            "3MaxIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.DOUBLE,
            "3MaxIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.STRING,
            "3MaxIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.TIMESTAMP,
            "3MaxIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.DECIMAL,
            "3MaxIN10impala_udf10DecimalValEEEvPNS2_15FunctionContextERKT_PS6_")
        .build();

  private static final Map<ColumnType, String> STDDEV_UPDATE_SYMBOL =
      ImmutableMap.<ColumnType, String>builder()
        .put(ColumnType.TINYINT,
            "14KnuthVarUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.SMALLINT,
            "14KnuthVarUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.INT,
            "14KnuthVarUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.BIGINT,
            "14KnuthVarUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.FLOAT,
            "14KnuthVarUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.DOUBLE,
            "14KnuthVarUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
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

    Db db = this;
    // Count (*)
    // TODO: the merge function should be Sum but the way we rewrite distincts
    // makes that not work.
    db.addBuiltin(AggregateFunction.createBuiltin(db, "count",
        new ArrayList<ColumnType>(),
        ColumnType.BIGINT, ColumnType.BIGINT,
        prefix + "8InitZeroIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
        prefix + "15CountStarUpdateEPN10impala_udf15FunctionContextEPNS1_9BigIntValE",
        prefix + "15CountStarUpdateEPN10impala_udf15FunctionContextEPNS1_9BigIntValE",
        null, null, false));

    for (ColumnType t : ColumnType.getSupportedTypes()) {
      if (t.isNull()) continue; // NULL is handled through type promotion.
      // Count
      db.addBuiltin(AggregateFunction.createBuiltin(db, "count",
          Lists.newArrayList(t), ColumnType.BIGINT, ColumnType.BIGINT,
          prefix + "8InitZeroIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
          prefix + "11CountUpdateEPN10impala_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
          prefix + "11CountUpdateEPN10impala_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
          null, null, false));
      // Min
      String minMaxInit = t.isStringType() ? initNullString : initNull;
      String minMaxSerializeOrFinalize = t.isStringType() ?
          stringValSerializeOrFinalize : null;
      db.addBuiltin(AggregateFunction.createBuiltin(db, "min",
          Lists.newArrayList(t), t, t, minMaxInit,
          prefix + MIN_UPDATE_SYMBOL.get(t),
          prefix + MIN_UPDATE_SYMBOL.get(t),
          minMaxSerializeOrFinalize, minMaxSerializeOrFinalize, true));
      // Max
      db.addBuiltin(AggregateFunction.createBuiltin(db, "max",
          Lists.newArrayList(t), t, t, minMaxInit,
          prefix + MAX_UPDATE_SYMBOL.get(t),
          prefix + MAX_UPDATE_SYMBOL.get(t),
          minMaxSerializeOrFinalize, minMaxSerializeOrFinalize, true));
      // NDV
      // TODO: this needs to switch to CHAR(64) as the intermediate type
      db.addBuiltin(AggregateFunction.createBuiltin(db, "ndv",
          Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
          prefix + "7HllInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + HLL_UPDATE_SYMBOL.get(t),
          prefix + "8HllMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          prefix + "11HllFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          true));

      // distinctpc
      // TODO: this needs to switch to CHAR(64) as the intermediate type
      db.addBuiltin(AggregateFunction.createBuiltin(db, "distinctpc",
          Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
          prefix + "6PcInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + PC_UPDATE_SYMBOL.get(t),
          prefix + "7PcMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          prefix + "10PcFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          true));

      // distinctpcsa
      // TODO: this needs to switch to CHAR(64) as the intermediate type
      db.addBuiltin(AggregateFunction.createBuiltin(db, "distinctpcsa",
          Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
          prefix + "6PcInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + PCSA_UPDATE_SYMBOL.get(t),
          prefix + "7PcMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          prefix + "12PcsaFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          true));

      if (STDDEV_UPDATE_SYMBOL.containsKey(t)) {
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev",
            Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "19KnuthStddevFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev_samp",
            Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "19KnuthStddevFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev_pop",
            Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "22KnuthStddevPopFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance",
            Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "16KnuthVarFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance_samp",
            Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "16KnuthVarFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance_pop",
            Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "19KnuthVarPopFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
      }
    }

    // Sum
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum",
        Lists.newArrayList(ColumnType.BIGINT), ColumnType.BIGINT, ColumnType.BIGINT,
        initNull,
        prefix + "3SumIN10impala_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        prefix + "3SumIN10impala_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        null, null, false));
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum",
        Lists.newArrayList(ColumnType.DOUBLE), ColumnType.DOUBLE, ColumnType.DOUBLE,
        initNull,
        prefix + "3SumIN10impala_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        prefix + "3SumIN10impala_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        null, null, false));
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum",
        Lists.newArrayList(ColumnType.DECIMAL), ColumnType.DECIMAL, ColumnType.DECIMAL,
        initNull,
        prefix + "9SumUpdateEPN10impala_udf15FunctionContextERKNS1_10DecimalValEPS4_",
        prefix + "8SumMergeEPN10impala_udf15FunctionContextERKNS1_10DecimalValEPS4_",
        null, null, false));

    for (ColumnType t: ColumnType.getNumericTypes()) {
      // Avg
      // TODO: because of avg rewrite, BE doesn't implement it yet.
      db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
          Lists.newArrayList(t), ColumnType.DOUBLE, ColumnType.DOUBLE,
          "", "", "", null, "", false));
    }
    // Avg(Timestamp)
    // TODO: why does this make sense? Avg(timestamp) returns a double.
    db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
        Lists.newArrayList(ColumnType.TIMESTAMP),
        ColumnType.DOUBLE, ColumnType.DOUBLE,
        "", "", "", null, "", false));

    // Group_concat(string)
    db.addBuiltin(AggregateFunction.createBuiltin(db, "group_concat",
        Lists.newArrayList(ColumnType.STRING),
        ColumnType.STRING, ColumnType.STRING,
        initNullString,
        prefix + "12StringConcatEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix + "12StringConcatEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize, stringValSerializeOrFinalize, false));
    // Group_concat(string, string)
    db.addBuiltin(AggregateFunction.createBuiltin(db, "group_concat",
        Lists.newArrayList(ColumnType.STRING, ColumnType.STRING),
        ColumnType.STRING, ColumnType.STRING,
        initNullString,
        prefix +
            "12StringConcatEPN10impala_udf15FunctionContextERKNS1_9StringValES6_PS4_",
        prefix + "12StringConcatEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize, stringValSerializeOrFinalize, false));
  }
}
