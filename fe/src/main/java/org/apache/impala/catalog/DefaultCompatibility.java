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

import static org.apache.impala.catalog.Type.BIGINT;
import static org.apache.impala.catalog.Type.BOOLEAN;
import static org.apache.impala.catalog.Type.CHAR;
import static org.apache.impala.catalog.Type.DATE;
import static org.apache.impala.catalog.Type.DATETIME;
import static org.apache.impala.catalog.Type.DECIMAL;
import static org.apache.impala.catalog.Type.DOUBLE;
import static org.apache.impala.catalog.Type.FLOAT;
import static org.apache.impala.catalog.Type.INT;
import static org.apache.impala.catalog.Type.SMALLINT;
import static org.apache.impala.catalog.Type.STRING;
import static org.apache.impala.catalog.Type.TIMESTAMP;
import static org.apache.impala.catalog.Type.TINYINT;
import static org.apache.impala.catalog.Type.VARCHAR;

public class DefaultCompatibility implements CompatibilityRule {
  @Override
  public void apply(PrimitiveType[][] matrix) {
    matrix[BOOLEAN.ordinal()][TINYINT.ordinal()] = PrimitiveType.TINYINT;
    matrix[BOOLEAN.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
    matrix[BOOLEAN.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    matrix[BOOLEAN.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    matrix[BOOLEAN.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    matrix[BOOLEAN.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    matrix[BOOLEAN.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[BOOLEAN.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[BOOLEAN.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[BOOLEAN.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[BOOLEAN.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[BOOLEAN.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[BOOLEAN.ordinal()][DECIMAL.ordinal()] = PrimitiveType.INVALID_TYPE;

    matrix[TINYINT.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
    matrix[TINYINT.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    matrix[TINYINT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    // 8 bit integer fits in mantissa of both float and double.
    matrix[TINYINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    matrix[TINYINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    matrix[TINYINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[TINYINT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[TINYINT.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[TINYINT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[TINYINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[TINYINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[TINYINT.ordinal()][DECIMAL.ordinal()] = PrimitiveType.DECIMAL;

    matrix[SMALLINT.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    matrix[SMALLINT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    // 16 bit integer fits in mantissa of both float and double.
    matrix[SMALLINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    matrix[SMALLINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    matrix[SMALLINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[SMALLINT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[SMALLINT.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[SMALLINT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[SMALLINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[SMALLINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[SMALLINT.ordinal()][DECIMAL.ordinal()] = PrimitiveType.DECIMAL;

    matrix[INT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    // 32 bit integer fits only in the mantissa of double.
    // TODO: arguably we should promote INT + FLOAT to DOUBLE to avoid loss of precision,
    // but we depend on it remaining FLOAT for some use cases, e.g.
    // "insert into tbl (float_col) select int_col + float_col from ..."
    matrix[INT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    matrix[INT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    matrix[INT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[INT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[INT.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[INT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[INT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[INT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[INT.ordinal()][DECIMAL.ordinal()] = PrimitiveType.DECIMAL;

    // 64 bit integer does not fit in the mantissa of double or float.
    // TODO: arguably we should always promote BIGINT + FLOAT to double here to keep as
    // much precision as possible, but we depend on this implicit cast for some use
    // cases, similarly to INT + FLOAT.
    matrix[BIGINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    // TODO: we're breaking the definition of strict compatibility for BIGINT + DOUBLE,
    // but this forces function overloading to consider the DOUBLE overload first.
    matrix[BIGINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    matrix[BIGINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[BIGINT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[BIGINT.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[BIGINT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[BIGINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[BIGINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[BIGINT.ordinal()][DECIMAL.ordinal()] = PrimitiveType.DECIMAL;

    matrix[FLOAT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
    matrix[FLOAT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[FLOAT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[FLOAT.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[FLOAT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[FLOAT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[FLOAT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[FLOAT.ordinal()][DECIMAL.ordinal()] = PrimitiveType.FLOAT;

    matrix[DOUBLE.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DOUBLE.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DOUBLE.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DOUBLE.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DOUBLE.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DOUBLE.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DOUBLE.ordinal()][DECIMAL.ordinal()] = PrimitiveType.DOUBLE;

    // We can convert some but not all string values to date.
    matrix[DATE.ordinal()][STRING.ordinal()] = PrimitiveType.DATE;
    matrix[DATE.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DATE.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DATE.ordinal()][DECIMAL.ordinal()] = PrimitiveType.INVALID_TYPE;

    matrix[DATETIME.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.TIMESTAMP;
    matrix[DATETIME.ordinal()][DATE.ordinal()] = PrimitiveType.DATETIME;
    matrix[DATETIME.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DATETIME.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DATETIME.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DATETIME.ordinal()][DECIMAL.ordinal()] = PrimitiveType.INVALID_TYPE;

    // We can convert some but not all date values to timestamps.
    matrix[TIMESTAMP.ordinal()][DATE.ordinal()] = PrimitiveType.TIMESTAMP;
    // We can convert some but not all string values to timestamps.
    matrix[TIMESTAMP.ordinal()][STRING.ordinal()] = PrimitiveType.TIMESTAMP;
    matrix[TIMESTAMP.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[TIMESTAMP.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[TIMESTAMP.ordinal()][DECIMAL.ordinal()] = PrimitiveType.INVALID_TYPE;

    matrix[STRING.ordinal()][VARCHAR.ordinal()] = PrimitiveType.STRING;
    matrix[STRING.ordinal()][CHAR.ordinal()] = PrimitiveType.STRING;
    matrix[STRING.ordinal()][DECIMAL.ordinal()] = PrimitiveType.INVALID_TYPE;

    matrix[VARCHAR.ordinal()][CHAR.ordinal()] = PrimitiveType.VARCHAR;
    matrix[VARCHAR.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
    matrix[VARCHAR.ordinal()][DECIMAL.ordinal()] = PrimitiveType.INVALID_TYPE;

    matrix[CHAR.ordinal()][VARCHAR.ordinal()] = PrimitiveType.VARCHAR;

    matrix[DECIMAL.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DECIMAL.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DECIMAL.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DECIMAL.ordinal()][TIMESTAMP.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DECIMAL.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[DECIMAL.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
  }
}
