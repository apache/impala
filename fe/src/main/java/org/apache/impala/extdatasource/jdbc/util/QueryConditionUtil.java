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

package org.apache.impala.extdatasource.jdbc.util;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.extdatasource.jdbc.dao.DatabaseAccessor;
import org.apache.impala.extdatasource.thrift.TBinaryPredicate;
import org.apache.impala.extdatasource.thrift.TComparisonOp;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TPrimitiveType;
import org.apache.impala.thrift.TTypeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Translates the impala query predicates into a condition that can be run on the
 * underlying database
 */
public class QueryConditionUtil {

  private final static Logger LOG = LoggerFactory.getLogger(QueryConditionUtil.class);

  public static String buildCondition(List<List<TBinaryPredicate>> predicates,
      Map<String, String> columnMapping, DatabaseAccessor dbAccessor_) {
    List<String> condition = Lists.newArrayList();
    for (List<TBinaryPredicate> tBinaryPredicates : predicates) {
      StringJoiner joiner = new StringJoiner(" OR ", "(", ")");
      for (TBinaryPredicate predicate : tBinaryPredicates) {
        String name = predicate.getCol().getName();
        name = columnMapping.getOrDefault(name, name);
        String op = converse(predicate.getOp());
        String value = getTColumnValueAsString(predicate.getValue(),
            predicate.getCol().getType().getTypes().get(0), dbAccessor_);
        joiner.add(String.format("%s %s %s", name, op, value));
      }
      condition.add(joiner.toString());
    }
    return Joiner.on(" AND ").join(condition);
  }

  /**
   * Return the value of a defined field as a string. If the "value" is null or the type
   * is not supported, an exception is thrown.
   *
   * @see org.apache.impala.planner.DataSourceScanNode#literalToColumnValue
   */
  public static String getTColumnValueAsString(TColumnValue value, TTypeNode node,
      DatabaseAccessor dbAccessor_) {
    Preconditions.checkState(value != null);
    StringBuilder sb = new StringBuilder();
    if (value.isSetBool_val()) {
      sb.append(value.bool_val);
    } else if (value.isSetByte_val()) {
      sb.append(value.byte_val);
    } else if (value.isSetShort_val()) {
      sb.append(value.short_val);
    } else if (value.isSetInt_val()) {
      sb.append(value.int_val);
    } else if (value.isSetLong_val()) {
      sb.append(value.long_val);
    } else if (value.isSetDouble_val()) {
      sb.append(value.double_val);
    } else if (value.isSetString_val()) {
      // DECIMAL and TIMESTAMP types of predicates are represented as string.
      if (node.getScalar_type().getType() == TPrimitiveType.DECIMAL) {
        // Check column data type and don't add quotes for decimal string.
        sb.append(String.format("%s", value.string_val));
      } else {
        sb.append(String.format("'%s'", value.string_val));
      }
    } else if (value.isSetDate_val()) {
      sb.append(String.format("'%s'", dbAccessor_.getDateString(value.date_val)));
    } else {
      // TODO: Support data types of binary for predicates.
      // Keep in-sync with DataSourceScanNode.literalToColumnValue().
      throw new IllegalArgumentException("Unsupported data type.");
    }
    return sb.toString();
  }

  /**
   * @see BinaryPredicate.Operator
   */
  public static String converse(TComparisonOp op) {
    for (Operator operator : BinaryPredicate.Operator.values()) {
      if (operator.getThriftOp() == op) {
        return operator.toString();
      }
    }
    return null;
  }
}
