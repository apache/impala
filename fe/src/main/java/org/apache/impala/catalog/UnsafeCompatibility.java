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
import static org.apache.impala.catalog.Type.CHAR;
import static org.apache.impala.catalog.Type.DOUBLE;
import static org.apache.impala.catalog.Type.FLOAT;
import static org.apache.impala.catalog.Type.INT;
import static org.apache.impala.catalog.Type.SMALLINT;
import static org.apache.impala.catalog.Type.STRING;
import static org.apache.impala.catalog.Type.TINYINT;
import static org.apache.impala.catalog.Type.VARCHAR;

public class UnsafeCompatibility implements CompatibilityRule {
  @Override
  public void apply(PrimitiveType[][] matrix) {
    // Numeric types to STRING
    matrix[TINYINT.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
    matrix[SMALLINT.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
    matrix[INT.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
    matrix[BIGINT.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
    matrix[FLOAT.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
    matrix[DOUBLE.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;

    // STRING to numeric types
    matrix[STRING.ordinal()][TINYINT.ordinal()] = PrimitiveType.TINYINT;
    matrix[STRING.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
    matrix[STRING.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    matrix[STRING.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    matrix[STRING.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    matrix[STRING.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;

    // Numeric types to VARCHAR
    matrix[TINYINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.VARCHAR;
    matrix[SMALLINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.VARCHAR;
    matrix[INT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.VARCHAR;
    matrix[BIGINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.VARCHAR;
    matrix[FLOAT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.VARCHAR;
    matrix[DOUBLE.ordinal()][VARCHAR.ordinal()] = PrimitiveType.VARCHAR;

    // VARCHAR to numeric types
    matrix[VARCHAR.ordinal()][TINYINT.ordinal()] = PrimitiveType.TINYINT;
    matrix[VARCHAR.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
    matrix[VARCHAR.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    matrix[VARCHAR.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    matrix[VARCHAR.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    matrix[VARCHAR.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;

    // Numeric types to CHAR
    matrix[TINYINT.ordinal()][CHAR.ordinal()] = PrimitiveType.CHAR;
    matrix[SMALLINT.ordinal()][CHAR.ordinal()] = PrimitiveType.CHAR;
    matrix[INT.ordinal()][CHAR.ordinal()] = PrimitiveType.CHAR;
    matrix[BIGINT.ordinal()][CHAR.ordinal()] = PrimitiveType.CHAR;
    matrix[FLOAT.ordinal()][CHAR.ordinal()] = PrimitiveType.CHAR;
    matrix[DOUBLE.ordinal()][CHAR.ordinal()] = PrimitiveType.CHAR;

    // CHAR to numeric types
    matrix[CHAR.ordinal()][TINYINT.ordinal()] = PrimitiveType.TINYINT;
    matrix[CHAR.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
    matrix[CHAR.ordinal()][INT.ordinal()] = PrimitiveType.INT;
    matrix[CHAR.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
    matrix[CHAR.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
    matrix[CHAR.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;

    // CHAR, VARCHAR to STRING
    matrix[CHAR.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
    matrix[VARCHAR.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
  }
}
