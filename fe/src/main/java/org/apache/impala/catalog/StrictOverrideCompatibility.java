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
import static org.apache.impala.catalog.Type.DATE;
import static org.apache.impala.catalog.Type.DECIMAL;
import static org.apache.impala.catalog.Type.DOUBLE;
import static org.apache.impala.catalog.Type.FLOAT;
import static org.apache.impala.catalog.Type.INT;
import static org.apache.impala.catalog.Type.STRING;
import static org.apache.impala.catalog.Type.TIMESTAMP;

public class StrictOverrideCompatibility implements CompatibilityRule {
  @Override
  public void apply(PrimitiveType[][] matrix) {
    matrix[INT.ordinal()][FLOAT.ordinal()] = PrimitiveType.DOUBLE;
    matrix[BIGINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.DOUBLE;
    matrix[DATE.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[TIMESTAMP.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[TIMESTAMP.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
    // The case of decimal and float/double must be handled carefully. There are two
    // modes: strict and non-strict. In non-strict mode, we convert to the floating
    // point type, since it can contain a larger range of values than any decimal (but
    // has lower precision in some parts of its range), so it is generally better.
    // In strict mode, we avoid conversion in either direction because there are also
    // decimal values (e.g. 0.1) that cannot be exactly represented in binary
    // floating point.
    // TODO: it might make sense to promote to double in many cases, but this would
    // require more work elsewhere to avoid breaking things, e.g. inserting decimal
    // literals into float columns.
    matrix[DOUBLE.ordinal()][DECIMAL.ordinal()] = PrimitiveType.INVALID_TYPE;
    matrix[FLOAT.ordinal()][DECIMAL.ordinal()] = PrimitiveType.INVALID_TYPE;
  }
}
