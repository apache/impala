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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.impala.calcite.rules.ImpalaMQContext;
import org.apache.impala.calcite.service.CalciteRelNodeConverter;
import org.apache.impala.catalog.FeView;

import java.lang.reflect.Type;
import java.util.List;

public class ImpalaViewTable extends ViewTable {

  private final FeView table_;

  // The SqlNode tree that has been analyzed and validated.
  private SqlNode validatedNode_;

  public ImpalaViewTable(Type elementType, RelProtoDataType rowType,
      List<String> schemaPath, List<String> viewPath, FeView feTable) {
    super(elementType, rowType, feTable.getQueryStmt().toSql(), schemaPath, viewPath);
    this.table_ = feTable;
  }

  public FeView getFeView() {
    return table_;
  }

  public void setValidatedNode(SqlNode validatedNode) {
    validatedNode_ = validatedNode;
  }

  /**
   * The default implementation of toRel immediately calls ViewTable.expandView()
   * which expands the view into a RelNode tree. However, some of the configuration
   * variables specific to Impala are not configurable in the parent class. This
   * overridden method calls the Impala version of convertQuery to get the RelRoot
   * of the view tree.
   *
   * There is a little bit of work to do to clean up the RelNode after the view
   * has been expanded, including handling recursive views. Unfortunately, the
   * parent ViewTable.expandView() contains these portions and the method is private.
   * So some code had to be copied from ViewTable.expandView() to handle this.
   */
  @Override
  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    RelOptCluster cluster = context.getCluster();
    ImpalaMQContext converterContext =
        (ImpalaMQContext) cluster.getPlanner().getContext();
    CalciteRelNodeConverter relNodeConverter = converterContext.relNodeConverter_;

    try {
      final RelRoot root = relNodeConverter.convertQuery(validatedNode_);
      // Start code extracted from ViewTable.expandView().
      final RelNode rel =
          RelOptUtil.createCastRel(root.rel, relOptTable.getRowType(), true);
      // Expand any views
      final RelNode rel2 =
          rel.accept(new RelShuttleImpl() {
            @Override public RelNode visit(TableScan scan) {
              final RelOptTable table = scan.getTable();
              final TranslatableTable translatableTable =
                  table.unwrap(TranslatableTable.class);
              if (translatableTable != null) {
                return translatableTable.toRel(context, table);
              }
              return super.visit(scan);
            }
          });
      return root.withRel(rel2).rel;
      // End code extracted from ViewTable.expandView().
    } catch (Exception e) {
      throw new RuntimeException("Error analyzing view " + getViewSql() + ": " + e);
    }
  }
}
