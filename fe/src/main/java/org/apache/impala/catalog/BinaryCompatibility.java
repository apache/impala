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

import static org.apache.impala.catalog.Type.BINARY;
import static org.apache.impala.catalog.Type.STRING;
import static org.apache.impala.catalog.Type.VARCHAR;

// BINARY can be only cast to STRING.
public class BinaryCompatibility implements CompatibilityRule {
  @Override
  public void apply(PrimitiveType[][] matrix) {
    for (int i = 0; i < PrimitiveType.values().length; ++i) {
      if (i != BINARY.ordinal() && i != STRING.ordinal()) {
        matrix[BINARY.ordinal()][i] = PrimitiveType.INVALID_TYPE;
        matrix[i][BINARY.ordinal()] = PrimitiveType.INVALID_TYPE;
      }
    }
    // STRING -> BINARY conversion is not lossy, but implicit cast is not allowed to
    // enforce exact type match in function calls.
    matrix[STRING.ordinal()][BINARY.ordinal()] = PrimitiveType.INVALID_TYPE;
  }
}
