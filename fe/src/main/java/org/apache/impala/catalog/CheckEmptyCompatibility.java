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

import com.google.common.base.Preconditions;

public class CheckEmptyCompatibility implements CompatibilityRule {
  @Override
  public void apply(PrimitiveType[][] matrix) {
    // Check all the necessary entries that should be filled.
    for (int i = 0; i < PrimitiveType.values().length; ++i) {
      for (int j = i; j < PrimitiveType.values().length; ++j) {
        PrimitiveType t1 = PrimitiveType.values()[i];
        PrimitiveType t2 = PrimitiveType.values()[j];
        // INVALID_TYPE and NULL_TYPE are handled separately.
        if (t1 == PrimitiveType.INVALID_TYPE || t2 == PrimitiveType.INVALID_TYPE) {
          continue;
        }
        if (t1 == PrimitiveType.NULL_TYPE || t2 == PrimitiveType.NULL_TYPE) {
          continue;
        }
        Preconditions.checkNotNull(matrix[i][j]);
      }
    }
  }
}
