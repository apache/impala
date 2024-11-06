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

package org.apache.impala.calcite.functions;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The FunctionResolver is a wrapper around the Impala Function Resolver (via the
 * (Db.getFunction() method).
 */
public class FunctionResolver {
  protected static final Logger LOG =
      LoggerFactory.getLogger(FunctionResolver.class.getName());

  // Map of the Calcite Kind to an Impala function name
  // Only functions that do not map directly from Calcite to Impala need
  // to be in this map. For instance, Calcite maps "%" to the name "mod"
  // and since "mod" is the function name in Impala, it does not need
  // to be in this map.
  public static Map<SqlKind, String> CALCITE_KIND_TO_IMPALA_FUNC =
      ImmutableMap.<SqlKind, String> builder()
      .put(SqlKind.EQUALS, "eq")
      .put(SqlKind.IS_FALSE, "isfalse")
      .put(SqlKind.IS_NOT_FALSE, "isnotfalse")
      .put(SqlKind.IS_NOT_NULL, "is_not_null_pred")
      .put(SqlKind.IS_NOT_TRUE, "isnottrue")
      .put(SqlKind.IS_NULL, "is_null_pred")
      .put(SqlKind.IS_TRUE, "istrue")
      .put(SqlKind.GREATER_THAN, "gt")
      .put(SqlKind.GREATER_THAN_OR_EQUAL, "ge")
      .put(SqlKind.LESS_THAN, "lt")
      .put(SqlKind.LESS_THAN_OR_EQUAL, "le")
      .put(SqlKind.NOT_EQUALS, "ne")
      .put(SqlKind.PLUS, "add")
      .put(SqlKind.MINUS, "subtract")
      .put(SqlKind.MINUS_PREFIX, "negative")
      .put(SqlKind.TIMES, "multiply")
      .put(SqlKind.DIVIDE, "divide")
      .put(SqlKind.SUM0, "sum_init_zero")
      .put(SqlKind.POSIX_REGEX_CASE_SENSITIVE, "regexp")
      .put(SqlKind.POSIX_REGEX_CASE_INSENSITIVE, "iregexp")
      .put(SqlKind.IS_NOT_DISTINCT_FROM, "notdistinct")
      .put(SqlKind.IS_DISTINCT_FROM, "distinctfrom")
      .build();

  // Map of Calcite names to an Impala function name when the names are different
  public static Map<String, String> CALCITE_NAME_TO_IMPALA_FUNC =
      ImmutableMap.<String, String> builder()
      .put("explicit_cast", "cast")
      .put("||", "concat")
      .build();

  public static Set<SqlKind> ARITHMETIC_TYPES =
      ImmutableSet.<SqlKind> builder()
      .add(SqlKind.PLUS)
      .add(SqlKind.MINUS)
      .add(SqlKind.TIMES)
      .add(SqlKind.DIVIDE)
      .add(SqlKind.MOD)
      .build();

  public static Set<String> SPECIAL_PROCESSING_FUNCTIONS =
      ImmutableSet.<String> builder()
      .add("grouping_id")
      .add("count")
      .build();

  public static Function getSupertypeFunction(RexCall call) {
    // For arithmetic types, we use separate logic. Calcite has already
    // calculated the proper return type at validation time. So an
    // (INT + INT) function already has the correct calculated return type
    // of BIGINT. If we just used the operands, the function resolver would
    // return the prototype of INT +(INT, INT) which is not what we want.
    // So we pass in the operands based on the already calculated return
    // type (call.getType()).
    List<RelDataType> argTypes = ARITHMETIC_TYPES.contains(call.getKind())
        ? Lists.newArrayList(call.getType(), call.getType())
        : Lists.transform(call.getOperands(), RexNode::getType);
    return getFunction(call.getOperator().getName(), call.getKind(), argTypes, false);
  }

  public static Function getSupertypeFunction(String name, SqlKind kind,
      List<RelDataType> argTypes) {
    return getFunction(name, kind, argTypes, false);
  }

  public static Function getExactFunction(String name, SqlKind kind,
      List<RelDataType> argTypes) {
    return getFunction(name, kind, argTypes, true);
  }

  public static Function getExactFunction(String name, List<RelDataType> argTypes) {
    return getFunction(name, argTypes, true);
  }

  public static Function getSupertypeFunction(String name, List<RelDataType> argTypes) {
    return getFunction(name, argTypes, false);
  }

  private static Function getFunction(String name, SqlKind kind,
      List<RelDataType> argTypes, boolean exactMatch) {

    // Some names in Calcite don't map exactly to their corresponding Impala
    // functions, so we check to see if the mapping exists
    return getFunction(getMappedName(name, kind, argTypes), argTypes, exactMatch);
  }

  /**
   * For most Calcite operators, the function name within Calcite matches the
   * Impala function name. This method handles the exceptions to that rule.
   */
  private static String getMappedName(String name, SqlKind kind,
      List<RelDataType> argTypes) {
    // First check if any special mappings exist from Calcite SqlKinds to
    // Impala functions.
    String mappedName = CALCITE_KIND_TO_IMPALA_FUNC.get(kind);
    if (mappedName != null) {
      // IMPALA-13435: for sum_init_zero, there is support for BIGINT arguments,
      // but not for DECIMAL or FLOAT.
      if (mappedName.equals("sum_init_zero")) {
        if (!argTypes.get(0).getSqlTypeName().equals(SqlTypeName.BIGINT)) {
          return "sum";
        }
      }
      return mappedName;
    }

    String lowercaseName = name.toLowerCase();
    // Next check if there are any names in Calcite that do not match
    // the Impala function name.
    mappedName = CALCITE_NAME_TO_IMPALA_FUNC.get(lowercaseName);
    if (mappedName != null) {
      return mappedName;
    }

    // If reached here, use the function name as given, no special mapping needed.
    return lowercaseName;
  }

  private static Function getFunction(String name, List<RelDataType> argTypes,
      boolean exactMatch) {
    String lowercaseName = name.toLowerCase();

    List<Type> impalaArgTypes = getArgTypes(lowercaseName, argTypes, exactMatch);

    return SPECIAL_PROCESSING_FUNCTIONS.contains(lowercaseName)
        ? getSpecialProcessingFunction(lowercaseName, impalaArgTypes, exactMatch)
        : getImpalaFunction(lowercaseName, impalaArgTypes, exactMatch);
  }

  private static Function getSpecialProcessingFunction(String lowercaseName,
      List<Type> impalaArgTypes, boolean exactMatch) {
    if (lowercaseName.equals("grouping_id")) {
      return AggregateFunction.createRewrittenBuiltin(BuiltinsDb.getInstance(),
          lowercaseName, impalaArgTypes, Type.BIGINT, true, false, true);
    }

    // Hack.  The count function can have more than one parameter when it is
    // of the form "count(distinct c1, c2)"  However, the function resolver only
    // contains count functions with one parameter. It is only later on in
    // the compilation that the AggregateInfo class changes the multiple
    // parameters into one parameter. But we still have to deal with resolving
    // count here.  So we just grab the first parameter so that it resolves
    // properly.
    if (lowercaseName.equals("count")) {
      if (impalaArgTypes.size() > 1) {
        impalaArgTypes = Lists.newArrayList(impalaArgTypes.get(0));
      }
      return getImpalaFunction(lowercaseName, impalaArgTypes, exactMatch);
    }

    throw new RuntimeException("Special function not found: " + lowercaseName);
  }

  private static Function getImpalaFunction(String lowercaseName,
      List<Type> impalaArgTypes, boolean exactMatch) {
    Function searchDesc = new Function(new FunctionName(BuiltinsDb.NAME, lowercaseName),
        impalaArgTypes, Type.INVALID, false);

    Function.CompareMode compareMode = exactMatch
        ? Function.CompareMode.IS_INDISTINGUISHABLE
        : Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;

    Function fn = BuiltinsDb.getInstance().getFunction(searchDesc, compareMode);

    if (fn == null) {
      LOG.debug("Failed to find function " + lowercaseName);
    }

    return fn;
  }

  private static List<Type> getArgTypes(String name, List<RelDataType> argTypes,
      boolean exactMatch) {
    // Case statement is special because the function signature only contains the
    // return value as its only operand. This operand needs to be calculated based
    // on its parameters (see getCaseArgs method comment) if we are looking for a
    // non-exact match (the best case function that would work if casting the operands).
    // The decode statement is also special in that the 3rd parameter is going to
    // contain the return value
    int caseOperandNum = getCaseOperandNum(name);
    if (caseOperandNum != -1) {
      return exactMatch
          ? Lists.newArrayList(ImpalaTypeConverter.createImpalaType(
              argTypes.get(caseOperandNum)))
          : getCaseArgs(name, argTypes);
    }

    return ImpalaTypeConverter.getNormalizedImpalaTypes(argTypes);
  }

  private static int getCaseOperandNum(String name) {
    if (name.equals("case")) {
      return 1;
    }
    if (name.equals("decode")) {
      return 2;
    }
    return -1;
  }

  /**
   * getCaseArgs needs to calculate the proper operand. It walks through
   * all the "then" clauses (and the default clause) and calculated the
   * most compatible type which it will use as its return value.
   */
  private static List<Type> getCaseArgs(String name, List<RelDataType> argTypes) {
    // Only handles the "case" function, not the "decode".
    Preconditions.checkState(name.equals("case"));

    int numOperands = argTypes.size();
    Type compatibleType = Type.NULL;
    for (int i = 0; i < numOperands; ++i) {
      if (!shouldSkipOperandForCase(numOperands, i)) {
        Type impalaType = ImpalaTypeConverter.createImpalaType(argTypes.get(i));
        compatibleType = Type.getAssignmentCompatibleType(compatibleType, impalaType,
            TypeCompatibility.DEFAULT);
      }
    }

    return Lists.newArrayList(compatibleType);
  }

  /**
   * shouldSkipOperand checks to see if we should skip this operand from casting.
   * Currently it only checks for CASE.  CASE will have some boolean operands which
   * will never need casting.
   */
  public static boolean shouldSkipOperandForCase(int numOperands, int i) {
    // The even number parameters are the boolean checks, but the last parameter
    // needs to be checked if there is an "else" term. Note: Though there are 2
    // flavors of the CASE command (one with a term before the first WHEN clause),
    // Calcite merges these 2 flavors in the validation step which ensures that the
    // check here will succeed.
    return (i % 2 == 0) && (numOperands - 1 != i);
  }
}
