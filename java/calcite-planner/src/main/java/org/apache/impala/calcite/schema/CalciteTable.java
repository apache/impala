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
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.calcite.rel.util.ImpalaBaseTableRef;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.calcite.type.ImpalaTypeSystemImpl;
import org.apache.impala.calcite.util.SimplifiedAnalyzer;
import org.apache.impala.planner.HdfsPartitionPruner;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.UnsupportedFeatureException;
import org.apache.impala.util.AcidUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class CalciteTable extends RelOptAbstractTable
    implements Table, Prepare.PreparingTable {

  private final HdfsTable table_;

  private final Map<Integer, Integer> impalaPositionMap_;

  private final List<String> qualifiedTableName_;

  public CalciteTable(FeTable table, CalciteCatalogReader reader)
      throws ImpalaException {
    super(reader, table.getName(), buildColumnsForRelDataType(table));
    this.table_ = (HdfsTable) table;
    this.qualifiedTableName_ = table.getTableName().toPath();
    this.impalaPositionMap_ = buildPositionMap();

    checkIfTableIsSupported(table);
  }

  public static RelDataType buildColumnsForRelDataType(FeTable table)
      throws ImpalaException {
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(new ImpalaTypeSystemImpl());

    RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

    // skip clustering columns, save them for the end
    for (Column column : table.getColumnsInHiveOrder()) {
      if (column.getType().isComplexType()) {
        throw new UnsupportedFeatureException(
            "Calcite does not support complex types yet.");
      }
      RelDataType type =
          ImpalaTypeConverter.createRelDataType(typeFactory, column.getType());
      builder.add(column.getName(), type);
    }
    return builder.build();
  }

  private void checkIfTableIsSupported(FeTable table) throws ImpalaException {
    if (table instanceof FeView) {
      throw new UnsupportedFeatureException("Views are not supported yet.");
    }

    if (!(table instanceof HdfsTable)) {
      String tableType = table.getClass().getSimpleName().replace("Table", "");
      throw new UnsupportedFeatureException(tableType + " tables are not supported yet.");
    }

  }

  public BaseTableRef createBaseTableRef(SimplifiedAnalyzer analyzer
      ) throws ImpalaException {

    TableRef tblRef = new TableRef(qualifiedTableName_, null);

    Path resolvedPath = analyzer.resolvePath(tblRef.getPath(), Path.PathType.TABLE_REF);

    BaseTableRef baseTblRef = new ImpalaBaseTableRef(tblRef, resolvedPath, analyzer);
    baseTblRef.analyze(analyzer);
    return baseTblRef;
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

  @Override
  public double getRowCount() {
    return (double) table_.getNumRows();
  }

  /**
   * Returns a position map from Impala column numbers to Calcite
   * column numbers. Calcite places the columns in "Hive Order"
   * so that a 'select *' will return the column list in the
   * right order, but this map is needed because sometimes we
   * only have the "Column.getPosition()" column number which
   * is the Impala column number.
   */
  private Map<Integer, Integer> buildPositionMap() {

    Map<Integer, Integer> impalaPositionMap = new HashMap<>();
    // skip clustering columns, save them for the end
    int i = 0;
    for (Column column : table_.getColumnsInHiveOrder()) {
      impalaPositionMap.put(column.getPosition(), i);
      i++;
    }

    return impalaPositionMap;
  }

  public int getCalcitePosition(int impalaPosition) {
    return impalaPositionMap_.get(impalaPosition);
  }

  public int getNumberColumnsIncludingAcid() {
    return impalaPositionMap_.keySet().size();
  }

  /**
   * Returns true if the conditions on the table meet the requirements
   * needed to apply the count star optimization.
   */
  public boolean canApplyCountStarOptimization(List<String> fieldNames) {
    Set<HdfsFileFormat> fileFormats = table_.getFileFormats();
    if (fileFormats.size() != 1) {
      return false;
    }
    if (!fileFormats.contains(HdfsFileFormat.ORC) &&
        !fileFormats.contains(HdfsFileFormat.PARQUET) &&
        !fileFormats.contains(HdfsFileFormat.HUDI_PARQUET)) {
      return false;
    }
    if (AcidUtils.isFullAcidTable(table_.getMetaStoreTable().getParameters())) {
      return false;
    }
    return isOnlyClusteredCols(fieldNames);
  }

  public boolean isOnlyClusteredCols(List<String> fieldNames) {
    for (int i = 0; i < fieldNames.size(); i++) {
      if (!table_.isClusteringColumn(table_.getColumn(fieldNames.get(i)))) {
        return false;
      }
    }
    return true;
  }
}
