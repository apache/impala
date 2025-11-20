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

/// For plain count(*) queries against Iceberg V2 tables we don't need to calculate
/// the number of records in data files without corresponding delete files, as this
/// can be retrieved from metadata. The backend will only calculate the number of records
/// in data files that have corresponding delete files, then this expression simply
/// adds the two numbers together.
public class IcebergV2CountStarAccumulator extends ArithmeticExpr {
  public IcebergV2CountStarAccumulator(Expr expr,
      long numRowsInDataFilesWithoutDeletes) {
    super(Operator.ADD, expr, NumericLiteral.create(
        numRowsInDataFilesWithoutDeletes));
  }
}