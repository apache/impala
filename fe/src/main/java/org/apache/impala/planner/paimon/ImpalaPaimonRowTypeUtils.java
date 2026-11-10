/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.planner.paimon;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

/**
 * Row type adjustments for Paimon predicate conversion, aligned with paimon-spark scan
 * builders.
 */
public final class ImpalaPaimonRowTypeUtils {
  private ImpalaPaimonRowTypeUtils() {}

  /** Returns the row type used to build {@code PredicateBuilder} for pushdown. */
  public static RowType predicateRowType(Table table) {
    RowType rowType = table.rowType();
    CoreOptions coreOptions = CoreOptions.fromMap(table.options());
    if (coreOptions.rowTrackingEnabled() && coreOptions.dataEvolutionEnabled()
        && !rowType.containsField(SpecialFields.ROW_ID.name())) {
      return SpecialFields.rowTypeWithRowTracking(rowType);
    }
    return rowType;
  }
}
