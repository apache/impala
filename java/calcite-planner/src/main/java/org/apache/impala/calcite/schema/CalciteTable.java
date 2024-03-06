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
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BaseTableRef;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.Path;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.planner.HdfsPartitionPruner;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;


public class CalciteTable extends RelOptAbstractTable
    implements Table, Prepare.PreparingTable {

  private final HdfsTable table_;


  private final List<String> qualifiedTableName_;

  public CalciteTable(FeTable table, CalciteCatalogReader reader) {
    super(reader, table.getName(), buildColumnsForRelDataType(table));
    this.table_ = (HdfsTable) table;
    this.qualifiedTableName_ = table.getTableName().toPath();
  }

  private static RelDataType buildColumnsForRelDataType(FeTable table) {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl());

    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
    // skip clustering columns, save them for the end
    for (Column column : table.getColumnsInHiveOrder()) {
      RelDataType type =
          ImpalaTypeConverter.createRelDataType(typeFactory, column.getType());
      builder.add(column.getName(), type);
    }
    return builder.build();
  }

  public BaseTableRef createBaseTableRef(Analyzer analyzer
      ) throws ImpalaException {

    TableRef tblRef = new TableRef(qualifiedTableName_, null);

    Path resolvedPath = analyzer.resolvePath(tblRef.getPath(), Path.PathType.TABLE_REF);

    BaseTableRef baseTblRef = new BaseTableRef(tblRef, resolvedPath);
    baseTblRef.analyze(analyzer);
    return baseTblRef;
  }

  // Create tuple and slot descriptors for this base table
  public TupleDescriptor createTupleAndSlotDesc(BaseTableRef baseTblRef,
      List<String> fieldNames, Analyzer analyzer) throws ImpalaException {
    // create the slot descriptors corresponding to this tuple descriptor
    // by supplying the field names from Calcite's output schema for this node
    for (int i = 0; i < fieldNames.size(); i++) {
      String fieldName = fieldNames.get(i);
      SlotRef slotref =
          new SlotRef(Path.createRawPath(baseTblRef.getUniqueAlias(), fieldName));
      slotref.analyze(analyzer);
      SlotDescriptor slotDesc = slotref.getDesc();
      if (slotDesc.getType().isCollectionType()) {
        throw new AnalysisException(String.format(fieldName + " "
            + "is a complex type (array/map/struct) column. "
            + "This is not currently supported."));
      }
      slotDesc.setIsMaterialized(true);
    }
    TupleDescriptor tupleDesc = baseTblRef.getDesc();
    return tupleDesc;
  }

  /**
   * Return the pruned partitions
   * TODO: Currently all partitions are returned since filters aren't yet supported.
   */
  public List<? extends FeFsPartition> getPrunedPartitions(Analyzer analyzer,
      TupleDescriptor tupleDesc) throws ImpalaException {
    HdfsPartitionPruner pruner = new HdfsPartitionPruner(tupleDesc);
    // TODO: pass in the conjuncts needed. An empty conjunct will return all partitions.
    List<Expr> conjuncts = new ArrayList<>();
    Pair<List<? extends FeFsPartition>, List<Expr>> impalaPair =
        pruner.prunePartitions(analyzer, conjuncts, true,
            null);
    return impalaPair.first;
  }

  public HdfsTable getHdfsTable() {
    return table_;
  }

  @Override
  public List<String> getQualifiedName() {
    return qualifiedTableName_;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(String column,
      SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
    return true;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }

  @Override
  public Statistic getStatistic() {
    return null;
  }

  @Override
  public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
    return getRowType();
  }

  @Override
  public <T> T unwrap(Class<T> arg0) {
    // Generic unwrap needed by the Calcite framework to process the table.
    return arg0.isInstance(this) ? arg0.cast(this) : null;
  }

  @Override
  public boolean columnHasDefaultValue(RelDataType rowType, int ordinal,
      InitializerContext initializerContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isTemporal() {
    return false;
  }

  @Override
  public boolean supportsModality(SqlModality modality) {
    return true;
  }

  @Override
  public SqlAccessType getAllowedAccess() {
    return SqlAccessType.ALL;
  }

  @Override
  public SqlMonotonicity getMonotonicity(String columnName) {
    return SqlMonotonicity.NOT_MONOTONIC;
  }
}
