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

package org.apache.impala.calcite.schema;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.impala.analysis.StmtMetadataLoader;
import org.apache.impala.thrift.TQueryCtx;

import java.util.List;

public class ImpalaCalciteCatalogReader extends CalciteCatalogReader {
  private final TQueryCtx queryCtx_;
  private final StmtMetadataLoader.StmtTableCache stmtTableCache_;

  public ImpalaCalciteCatalogReader(CalciteSchema rootSchema, List<String> defaultSchema,
      RelDataTypeFactory typeFactory, CalciteConnectionConfig config, TQueryCtx queryCtx,
      StmtMetadataLoader.StmtTableCache stmtTableCache) {
    super(rootSchema, defaultSchema, typeFactory, config);
    this.queryCtx_ = queryCtx;
    this.stmtTableCache_ = stmtTableCache;
  }

  public TQueryCtx getTQueryCtx() {
    return queryCtx_;
  }

  public StmtMetadataLoader.StmtTableCache getStmtTableCache() {
    return stmtTableCache_;
  }
}
