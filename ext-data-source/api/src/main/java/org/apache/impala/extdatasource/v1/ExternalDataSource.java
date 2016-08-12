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

package org.apache.impala.extdatasource.v1;

import org.apache.impala.extdatasource.thrift.TCloseParams;
import org.apache.impala.extdatasource.thrift.TCloseResult;
import org.apache.impala.extdatasource.thrift.TGetNextParams;
import org.apache.impala.extdatasource.thrift.TGetNextResult;
import org.apache.impala.extdatasource.thrift.TOpenParams;
import org.apache.impala.extdatasource.thrift.TOpenResult;
import org.apache.impala.extdatasource.thrift.TPrepareParams;
import org.apache.impala.extdatasource.thrift.TPrepareResult;

/**
 * Defines an external data source. Called by Impala during planning (prepare() only)
 * and during query execution (open(), getNext(), and close()).
 * TODO: Add javadocs
 */
public interface ExternalDataSource {

  /**
   * Called during the planning phase and serves two purposes:
   *  1) to pass information to the query planner for a specific scan operation (right
   *     now only an estimate of the number of rows returned).
   *  2) to accept or reject predicates that are present in the query; accepted
   *     predicates are then handed over to the library when the scan is initiated with
   *     the Open() call.
   * If prepare() fails, query planning will return with an error.
   */
  TPrepareResult prepare(TPrepareParams params);

  /**
   * Starts a scan. Called during query execution before any calls to getNext().
   */
  TOpenResult open(TOpenParams params);

  /**
   * Gets the next row batch of the scan.
   */
  TGetNextResult getNext(TGetNextParams params);

  /**
   * Ends the scan. After this call Impala will not make any more getNext() calls for
   * this same handle and the implementation is free to release all related resources.
   * Can be called at any point after open() has been called, even if the scan itself
   * hasn't finished (TGetNextResult.eos was not set to true).
   * Should always be called once unless prepare() fails.
   */
  TCloseResult close(TCloseParams params);
}
