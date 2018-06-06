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

import java.util.List;

import org.apache.impala.analysis.QueryStmt;

/**
 * Frontend interface for interacting with a view.
 */
public interface FeView extends FeTable {

  /**
   * @return true if this is a local view (i.e. one defined in a WITH clause)
   */
  boolean isLocalView();

  /**
   * @return the query statement that was parsed to create this view
   */
  QueryStmt getQueryStmt();

  /**
   * @return the explicit column labels for this view, or null if they need to be derived
   * entirely from the underlying query statement. The returned list has at least as many
   * elements as the number of column labels in the query statement.
   */
  List<String> getColLabels();

}
