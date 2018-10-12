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

package org.apache.impala.analysis;

/**
 * Options to configure how SQL should be outputted by toSql() and related calls.
 */
public enum ToSqlOptions {
  /**
   * The default way of displaying the original SQL query without rewrites.
   */
  DEFAULT(false, false),

  /**
   * Show rewritten query if it exists
   */
  REWRITTEN(true, false),

  /**
   * Show Implicit Casts.
   * To see implicit casts we must also show rewrites as otherwise we see original SQL.
   * This does have the consequence that the sql with implict casts may possibly fail
   * to parse if resubmitted as, for example, EXISTS queries that are rewritten as
   * semi-joins are not legal SQL.
   */
  SHOW_IMPLICIT_CASTS(true, true);

  private boolean rewritten_;

  private boolean implictCasts_;

  /**
   * Show rewritten form of Sql
   */
  public boolean showRewritten() { return rewritten_; }

  /**
   * Show Implicit Casts in Sql
   */
  public boolean showImplictCasts() { return implictCasts_; }

  ToSqlOptions(boolean rewritten, boolean implictCasts) {
    rewritten_ = rewritten;
    implictCasts_ = implictCasts;
  }
}
