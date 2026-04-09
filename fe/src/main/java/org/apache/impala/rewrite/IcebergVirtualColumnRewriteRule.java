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

package org.apache.impala.rewrite;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TVirtualColumnType;
import org.apache.impala.util.IcebergUtil;

/**
 * Rewrites references to the syntactic-sugar virtual columns of Iceberg V3 tables
 * into their equivalent COALESCE expressions:
 *   _row_id ->
 *       COALESCE(_file_row_id, ICEBERG__FIRST__ROW__ID + FILE__POSITION)
 *   _last_updated_sequence_number ->
 *       COALESCE(_file_last_updated_sequence_number,
 *           ICEBERG__DATA__SEQUENCE__NUMBER)
 *
 * This rule runs as a mandatory rewrite so that these virtual columns are never
 * visible to the planner or backend.
 */
public class IcebergVirtualColumnRewriteRule implements ExprRewriteRule {
  public static final ExprRewriteRule INSTANCE = new IcebergVirtualColumnRewriteRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    if (!(expr instanceof SlotRef)) return expr;
    SlotRef slotRef = (SlotRef) expr;
    if (!slotRef.isAnalyzed()) return expr;
    SlotDescriptor desc = slotRef.getDesc();
    if (desc == null) return expr;

    String tableAlias = desc.getParent().getAlias();
    if (tableAlias == null) return expr;

    TVirtualColumnType colType = desc.getVirtualColumnType();
    switch (colType) {
      case ICEBERG_ROW_ID:
        return IcebergUtil.buildRowIdExpr(tableAlias);
      case ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER:
        return IcebergUtil.buildLastUpdatedSeqNoExpr(tableAlias);
      default:
        return expr;
    }
  }
}
