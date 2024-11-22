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

package org.apache.impala.calcite.type;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.util.Static.RESOURCE;
import static java.util.Objects.requireNonNull;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ImpalaSqlIntervalQualifier is an extension of the Calcite SqlIntervalQualifier
 * class. The Calcite version does not handle sub-millisecond parts (the other parts
 * are still processed in the super class). In order to handle these parts, some
 * private methods had to be copied from the base class.
 */
public class ImpalaSqlIntervalQualifier extends SqlIntervalQualifier {

  public ImpalaSqlIntervalQualifier(
      TimeUnit startUnit,
      int startPrecision,
      TimeUnit endUnit,
      int fractionalSecondPrecision,
      SqlParserPos pos) {
    super(startUnit, startPrecision, endUnit, fractionalSecondPrecision, pos);
  }

  public ImpalaSqlIntervalQualifier(
      TimeUnit startUnit,
      TimeUnit endUnit,
      SqlParserPos pos) {
    super(startUnit, endUnit, pos);
  }

  public ImpalaSqlIntervalQualifier(String timeFrameName,
      SqlParserPos pos) {
    super(timeFrameName, pos);
  }

  @Override
  public int[] evaluateIntervalLiteral(String value, SqlParserPos pos,
      RelDataTypeSystem typeSystem) {

    switch(timeUnitRange) {
      case MILLISECOND:
      case MICROSECOND:
      case NANOSECOND:
        break;
      default:
        return super.evaluateIntervalLiteral(value, pos, typeSystem);
    }

    // save original value for if we have to throw
    final String value0 = value;

    // First strip off any leading whitespace
    value = value.trim();

    // check if the sign was explicitly specified.  Record
    // the explicit or implicit sign, and strip it off to
    // simplify pattern matching later.
    final int sign = getIntervalSign(value);
    value = stripLeadingSign(value);

    // If we have an empty or null literal at this point,
    // it's illegal.  Complain and bail out.
    if (Util.isNullOrEmpty(value)) {
      throw invalidValueException(pos, value0);
    }

    return evaluateIntervalLiteralAsSubSeconds(typeSystem, sign, value, value0,
        pos);
  }

  private int[] evaluateIntervalLiteralAsSubSeconds(
      RelDataTypeSystem typeSystem, int sign,
      String value,
      String originalValue,
      SqlParserPos pos) {
    BigDecimal subseconds;

    // validate the integer decimal pattern used for subseconds
    String intervalPattern = "(\\d+)";

    Matcher m = Pattern.compile(intervalPattern).matcher(value);
    if (m.matches()) {
      // Break out field values
      try {
        subseconds = parseField(m, 1);
      } catch (NumberFormatException e) {
        throw invalidValueException(pos, originalValue);
      }

      // package values up for return
      return fillIntervalValueArray(sign, subseconds);
    } else {
      throw invalidValueException(pos, originalValue);
    }
  }

  private static String stripLeadingSign(String value) {
    String unsignedValue = value;

    if (!Util.isNullOrEmpty(value)) {
      if (('-' == value.charAt(0)) || ('+' == value.charAt(0))) {
        unsignedValue = value.substring(1);
      }
    }

    return unsignedValue;
  }

  private static BigDecimal parseField(Matcher m, int i) {
    return new BigDecimal(castNonNull(m.group(i)));
  }

  private CalciteContextException invalidValueException(SqlParserPos pos,
      String value) {
    return SqlUtil.newContextException(pos,
        RESOURCE.unsupportedIntervalLiteral(
            "'" + value + "'", "INTERVAL " + toString()));
  }

  private static int[] fillIntervalValueArray(
      int sign,
      BigDecimal subseconds) {
    int[] ret = new int[3];

    ret[0] = sign;
    ret[1] = subseconds.intValue();
    ret[2] = 0;

    return ret;
  }
}
