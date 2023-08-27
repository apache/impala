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

package org.apache.impala.catalog.monitor;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.impala.thrift.TOperationUsageCounter;
import org.apache.impala.thrift.TTableName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Base class for metric counters.
 * It keeps the possible values of a metric in the outer static Map and the
 * dynamic values in the inner one.
 *
 * This class is thread-safe.
 */
public abstract class CatalogOperationCounter {
  // Lookup table for the counters
  protected Map<String, HashMap<String, MutableLong>> counters_;

  /**
   * Increments the operation counter for a specific catalog table name.
   */
  protected synchronized void incrementCounter(String operation, String tTableName) {
    counters_.get(operation).putIfAbsent(tTableName, new MutableLong(0L));
    counters_.get(operation).get(tTableName).increment();
  }

  /**
   * Decrements the operation counter for a specific catalog table name.
   */
  protected synchronized void decrementCounter(String operation, String tTableName) {
    counters_.get(operation).get(tTableName).decrement();
    if (counters_.get(operation).get(tTableName).longValue() == 0L) {
      counters_.get(operation).remove(tTableName);
    }
  }

  /**
   * Extracts the Catalog operation metrics summary and transforms it to
   * TOperationUsageCounter.
   */
  public synchronized List<TOperationUsageCounter> getOperationUsage() {
    List<TOperationUsageCounter> tOperationUsageCounters = new ArrayList<>();
    counters_.forEach((operation, tableCounterMap) -> {
      if (tableCounterMap.size() > 0) {
        List<TOperationUsageCounter> tOperationUsageCounter =
            createTOperationUsageCounter(operation, tableCounterMap);
        tOperationUsageCounters.addAll(tOperationUsageCounter);
      }
    });
    return tOperationUsageCounters;
  }

  /**
   * A util method that takes a catalog operation and the related table map with
   * counters as the parameter and summarizes the input to a
   * TTableCatalogOpMetrics object.
   */
  protected List<TOperationUsageCounter> createTOperationUsageCounter(
      String operation, HashMap<String, MutableLong> tableCounterMap) {
    List<TOperationUsageCounter> tOperationUsageCounters = new ArrayList();
    tableCounterMap.forEach((table, counter) -> {
      TOperationUsageCounter tOperationUsageCounter = new TOperationUsageCounter();
      tOperationUsageCounter.setCatalog_op_name(operation);
      tOperationUsageCounter.setTable_name(table);
      tOperationUsageCounter.setOp_counter(counter.longValue());
      tOperationUsageCounters.add(tOperationUsageCounter);
    });
    return tOperationUsageCounters;
  }

  /**
   * A util method to extract the table name from the TTableName object.
   */
  protected String getTableName(Optional<TTableName> tTableName) {
    if (tTableName.isPresent()) {
      if (tTableName.get().table_name.isEmpty()) return tTableName.get().db_name;
      return tTableName.get().db_name + "." + tTableName.get().table_name;
    } else {
      return "Not available";
    }
  }
}