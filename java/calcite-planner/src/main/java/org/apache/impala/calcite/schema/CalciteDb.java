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

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.View;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.UnsupportedFeatureException;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class CalciteDb extends AbstractSchema {

  private final Map<String, Table> tableMap_;

  private CalciteDb(Map<String, Table> tableMap) {
    this.tableMap_ = tableMap;
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tableMap_;
  }

  public static class Builder {
    private CalciteCatalogReader reader_;

    private final Map<String, Table> tableMap_ = new HashMap<>();

    public Builder(CalciteCatalogReader reader) {
      this.reader_ = reader;
    }

    public Builder addTable(String tableName, FeTable table,
        Analyzer analyzer) throws ImpalaException {
      if (tableMap_.containsKey(tableName)) return this;

      if (table instanceof HdfsTable) {
          tableMap_.put(tableName.toLowerCase(),
              new CalciteTable(table, reader_, analyzer));
          return this;
      }

      if (table instanceof View) {
          tableMap_.put(tableName.toLowerCase(), createViewTable(table));
          return this;
      }

      throw new UnsupportedFeatureException(
          "Table " + table.getFullName() + " has unsupported type " +
              table.getClass().getSimpleName() + ". The Calcite planner only supports " +
              "HdfsTable's and View's.");
    }

    private static ViewTable createViewTable(FeTable feTable) throws ImpalaException {
      RelDataType rowType = CalciteTable.buildColumnsForRelDataType(feTable);
      JavaTypeFactory typeFactory = (JavaTypeFactory) ImpalaTypeSystemImpl.TYPE_FACTORY;
      Type elementType = typeFactory.getJavaClass(rowType);
      return new ViewTable(elementType,
          RelDataTypeImpl.proto(rowType), ((View) feTable).getQueryStmt().toSql(),
          /* schemaPath */ ImmutableList.of(),
          /* viewPath */ ImmutableList.of(feTable.getDb().getName().toLowerCase(),
          feTable.getName().toLowerCase()));
    }

    public CalciteDb build() {
      return new CalciteDb(ImmutableMap.copyOf(tableMap_));
    }
  }
}
