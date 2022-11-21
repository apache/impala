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

package org.apache.impala.planner;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.InPredicate;
import org.apache.impala.analysis.IsNullPredicate;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.Column;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.DateLiteral;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TimeTravelSpec;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.IcebergContentFileStore;
import org.apache.impala.catalog.IcebergPositionDeleteTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.VirtualColumn;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.planner.JoinNode.DistributionMode;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TVirtualColumnType;
import org.apache.impala.util.ExprUtil;
import org.apache.impala.util.IcebergUtil;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scanning Iceberg tables is not as simple as scanning legacy Hive tables. The
 * complexity comes from handling delete files and time travel. We also want to push
 * down Impala conjuncts to Iceberg to filter out partitions and data files. This
 * class deals with such complexities.
 */
public class IcebergScanPlanner {
  private final static Logger LOG = LoggerFactory.getLogger(IcebergScanPlanner.class);

  private Analyzer analyzer_;
  private PlannerContext ctx_;
  private TableRef tblRef_;
  private List<Expr> conjuncts_;
  private MultiAggregateInfo aggInfo_;

  // Exprs in icebergConjuncts_ converted to Expression.
  private final List<Expression> icebergPredicates_ = new ArrayList<>();

  private List<FileDescriptor> dataFilesWithoutDeletes_ = new ArrayList<>();
  private List<FileDescriptor> dataFilesWithDeletes_ = new ArrayList<>();
  private Set<FileDescriptor> deleteFiles_ = new HashSet<>();

  // Statistics about the data and delete files. Useful for memory estimates of the
  // ANTI JOIN
  private long deletesRecordCount_ = 0;
  private long dataFilesWithDeletesSumPaths_ = 0;
  private long dataFilesWithDeletesMaxPath_ = 0;

  public IcebergScanPlanner(Analyzer analyzer, PlannerContext ctx,
      TableRef iceTblRef, List<Expr> conjuncts, MultiAggregateInfo aggInfo)
      throws ImpalaException {
    Preconditions.checkState(iceTblRef.getTable() instanceof FeIcebergTable);
    analyzer_ = analyzer;
    ctx_ = ctx;
    tblRef_ = iceTblRef;
    conjuncts_ = conjuncts;
    aggInfo_ = aggInfo;
    extractIcebergConjuncts();
  }

  public PlanNode createIcebergScanPlan() throws ImpalaException {
    analyzer_.materializeSlots(conjuncts_);

    if (!needIcebergForPlanning()) {
      return planWithoutIceberg();
    }

    filterFileDescriptors();

    PlanNode ret;
    if (deleteFiles_.isEmpty()) {
      // If there are no delete files we can just create a single SCAN node.
      Preconditions.checkState(dataFilesWithDeletes_.isEmpty());
      ret = new IcebergScanNode(ctx_.getNextNodeId(), tblRef_, conjuncts_,
          aggInfo_, dataFilesWithoutDeletes_);
      ret.init(analyzer_);
    } else {
      // Let's create a bit more complex plan in the presence of delete files.
      ret = createComplexIcebergScanPlan();
    }
    return ret;
  }

  /**
   * We can avoid calling Iceberg's expensive planFiles() API when the followings are
   * true:
   *  - we don't push down predicates
   *  - no time travel
   *  - no delete files
   * TODO: we should still avoid calling planFiles() if there are delete files. To do that
   * we either need to track which delete files have corresponding data files so we
   * can create the UNION ALL node. Or, if we have an optimized, Iceberg-specific
   * ANTI-JOIN operator, then it wouldn't hurt too much to transfer all rows through it.
   */
  private boolean needIcebergForPlanning() {
    return
        !icebergPredicates_.isEmpty() ||
        tblRef_.getTimeTravelSpec() != null ||
        !getIceTable().getContentFileStore().getDeleteFiles().isEmpty();
  }

  private PlanNode planWithoutIceberg() throws ImpalaException {
    PlanNode ret = new IcebergScanNode(ctx_.getNextNodeId(), tblRef_, conjuncts_,
        aggInfo_, getIceTable().getContentFileStore().getDataFiles());
    ret.init(analyzer_);
    return ret;
  }

  private PlanNode createComplexIcebergScanPlan() throws ImpalaException {
    PlanNode joinNode = createPositionJoinNode();
    if (dataFilesWithoutDeletes_.isEmpty()) {
      // All data files has corresponding delete files, so we just return an ANTI JOIN
      // between all data files and all delete files.
      return joinNode;
    }
    // If there are data files without corresponding delete files to be applied, we
    // can just create a SCAN node for these and do a UNION ALL with the ANTI JOIN.
    IcebergScanNode dataScanNode = new IcebergScanNode(
      ctx_.getNextNodeId(), tblRef_, conjuncts_, aggInfo_, dataFilesWithoutDeletes_);
    dataScanNode.init(analyzer_);
    List<Expr> outputExprs = tblRef_.getDesc().getSlots().stream().map(
        entry -> new SlotRef(entry)).collect(Collectors.toList());
    UnionNode unionNode = new UnionNode(ctx_.getNextNodeId(), tblRef_.getId(),
        outputExprs, false);
    unionNode.addChild(dataScanNode, outputExprs);
    unionNode.addChild(joinNode, outputExprs);
    unionNode.init(analyzer_);
    // Verify that the children are passed through.
    Preconditions.checkState(unionNode.getChildCount() == 2);
    Preconditions.checkState(unionNode.getFirstNonPassthroughChildIndex() == 2);
    return unionNode;
  }

  private PlanNode createPositionJoinNode() throws ImpalaException {
    // The followings just create separate scan nodes for data files and position delete
    // files, plus adds a LEFT ANTI HASH JOIN above them.
    PlanNodeId dataScanNodeId = ctx_.getNextNodeId();
    PlanNodeId deleteScanNodeId = ctx_.getNextNodeId();
    IcebergPositionDeleteTable deleteTable = new IcebergPositionDeleteTable(getIceTable(),
        getIceTable().getName() + "-POSITION-DELETE-" + deleteScanNodeId.toString(),
        deleteFiles_, deletesRecordCount_, getFilePathStats());
    analyzer_.addVirtualTable(deleteTable);
    TableRef deleteDeltaRef = TableRef.newTableRef(analyzer_,
        Arrays.asList(deleteTable.getDb().getName(), deleteTable.getName()),
        tblRef_.getUniqueAlias() + "-position-delete");
    addDataVirtualPositionSlots(tblRef_);
    addDeletePositionSlots(deleteDeltaRef);
    IcebergScanNode dataScanNode = new IcebergScanNode(
      dataScanNodeId, tblRef_, conjuncts_, aggInfo_, dataFilesWithDeletes_);
    dataScanNode.init(analyzer_);
    IcebergScanNode deleteScanNode = new IcebergScanNode(
        deleteScanNodeId, deleteDeltaRef, /*conjuncts=*/Collections.emptyList(),
        aggInfo_, Lists.newArrayList(deleteFiles_));
    deleteScanNode.init(analyzer_);
    deleteScanNode.setCardinality(deletesRecordCount_);

    // Now let's create the JOIN node
    List<BinaryPredicate> positionJoinConjuncts = createPositionJoinConjuncts(
            analyzer_, tblRef_.getDesc(), deleteDeltaRef.getDesc());

    JoinNode joinNode = new HashJoinNode(dataScanNode, deleteScanNode,
        /*straight_join=*/true, DistributionMode.NONE, JoinOperator.LEFT_ANTI_JOIN,
        positionJoinConjuncts, /*otherJoinConjuncts=*/Collections.emptyList());
    joinNode.setId(ctx_.getNextNodeId());
    joinNode.init(analyzer_);
    joinNode.setIsDeleteRowsJoin();
    return joinNode;
  }

  private void addDataVirtualPositionSlots(TableRef tblRef) throws AnalysisException {
    List<String> rawPath = new ArrayList<>();
    rawPath.add(tblRef.getUniqueAlias());
    // Add slot refs for position delete fields;
    String[] posFields = {VirtualColumn.INPUT_FILE_NAME.getName(),
                          VirtualColumn.FILE_POSITION.getName()};
    for (String posField : posFields) {
      rawPath.add(posField);
      SingleNodePlanner.addSlotRefToDesc(analyzer_, rawPath);
      rawPath.remove(rawPath.size() - 1);
    }
  }

  private void addDeletePositionSlots(TableRef tblRef)
      throws AnalysisException {
    List<String> rawPath = new ArrayList<>();
    rawPath.add(tblRef.getUniqueAlias());
    // Add slot refs for position delete fields;
    String[] posFields = {IcebergPositionDeleteTable.FILE_PATH_COLUMN,
                          IcebergPositionDeleteTable.POS_COLUMN};
    for (String posField : posFields) {
      rawPath.add(posField);
      SingleNodePlanner.addSlotRefToDesc(analyzer_, rawPath);
      rawPath.remove(rawPath.size() - 1);
    }
  }

  private List<BinaryPredicate> createPositionJoinConjuncts(Analyzer analyzer,
      TupleDescriptor insertTupleDesc, TupleDescriptor deleteTupleDesc)
      throws AnalysisException {
    List<BinaryPredicate> ret = new ArrayList<>();
    for (SlotDescriptor deleteSlotDesc : deleteTupleDesc.getSlots()) {
      boolean foundMatch = false;
      Column col = deleteSlotDesc.getParent().getTable().getColumns().get(
          deleteSlotDesc.getMaterializedPath().get(0));
      Preconditions.checkState(col instanceof IcebergColumn);
      int fieldId = ((IcebergColumn)col).getFieldId();
      for (SlotDescriptor insertSlotDesc : insertTupleDesc.getSlots()) {
        TVirtualColumnType virtColType = insertSlotDesc.getVirtualColumnType();
        if (fieldId == IcebergTable.V2_FILE_PATH_FIELD_ID &&
            virtColType != TVirtualColumnType.INPUT_FILE_NAME) {
          continue;
        }
        if (fieldId == IcebergTable.V2_POS_FIELD_ID &&
            virtColType != TVirtualColumnType.FILE_POSITION) {
          continue;
        }
        foundMatch = true;
        BinaryPredicate pred = new BinaryPredicate(
            Operator.EQ, new SlotRef(insertSlotDesc), new SlotRef(deleteSlotDesc));
        pred.analyze(analyzer);
        ret.add(pred);
        break;
      }
      Preconditions.checkState(foundMatch);
    }
    return ret;
  }

  private void filterFileDescriptors() throws ImpalaException {
    TimeTravelSpec timeTravelSpec = tblRef_.getTimeTravelSpec();

    try (CloseableIterable<FileScanTask> fileScanTasks = IcebergUtil.planFiles(
        getIceTable(), icebergPredicates_, timeTravelSpec)) {
      long dataFilesCacheMisses = 0;
      for (FileScanTask fileScanTask : fileScanTasks) {
        Pair<FileDescriptor, Boolean> fileDesc = getFileDescriptor(fileScanTask.file());
        if (!fileDesc.second) ++dataFilesCacheMisses;
        if (fileScanTask.deletes().isEmpty()) {
          dataFilesWithoutDeletes_.add(fileDesc.first);
        } else {
          updateDataFilesWithDeletesStatistics(fileScanTask.file());
          dataFilesWithDeletes_.add(fileDesc.first);
          for (DeleteFile delFile : fileScanTask.deletes()) {
            // TODO(IMPALA-11388): Add support for equality deletes.
            if (delFile.content() == FileContent.EQUALITY_DELETES) {
              throw new ImpalaRuntimeException(String.format(
                  "Iceberg table %s has EQUALITY delete file which is currently " +
                  "not supported by Impala: %s", getIceTable().getFullName(),
                  delFile.path()));
            }
            Pair<FileDescriptor, Boolean> delFileDesc = getFileDescriptor(delFile);
            if (!delFileDesc.second) ++dataFilesCacheMisses;
            if (deleteFiles_.add(delFileDesc.first)) {
              // New delete file added to 'deleteFiles_'.
              updateDeleteFilesStatistics(delFile);
            }
          }
        }
      }

      if (dataFilesCacheMisses > 0) {
        Preconditions.checkState(timeTravelSpec != null);
        LOG.info("File descriptors had to be loaded on demand during time travel: " +
            String.valueOf(dataFilesCacheMisses));
      }
    } catch (IOException | TableLoadingException e) {
      throw new ImpalaRuntimeException(String.format(
          "Failed to load data files for Iceberg table: %s", getIceTable().getFullName()),
          e);
    }
  }

  private void updateDataFilesWithDeletesStatistics(ContentFile cf) {
    long pathSize = cf.path().toString().length();
    dataFilesWithDeletesSumPaths_ += pathSize;
    if (pathSize > dataFilesWithDeletesMaxPath_) {
      dataFilesWithDeletesMaxPath_ = pathSize;
    }
  }

  private void updateDeleteFilesStatistics(DeleteFile delFile) {
    deletesRecordCount_ += getRecordCount(delFile);
  }

  private long getRecordCount(DeleteFile delFile) {
    long recordCount = delFile.recordCount();
    // 'record_count' is a required field for Iceberg data files, but let's still
    // prepare for the case when a compute engine doesn't fill it.
    if (recordCount == -1) return 1000;
    return recordCount;
  }

  private FeIcebergTable getIceTable() { return (FeIcebergTable)tblRef_.getTable(); }

  private TColumnStats getFilePathStats() {
    TColumnStats colStats = new TColumnStats();
    colStats.avg_size = dataFilesWithDeletesSumPaths_ / dataFilesWithDeletes_.size();
    colStats.max_size = dataFilesWithDeletesMaxPath_;
    colStats.num_distinct_values = dataFilesWithDeletes_.size();
    colStats.num_nulls = 0;
    return colStats;
  }

  private Pair<FileDescriptor, Boolean> getFileDescriptor(ContentFile cf)
      throws ImpalaRuntimeException {
    boolean cachehit = true;
    String pathHash = IcebergUtil.getFilePathHash(cf);
    IcebergContentFileStore fileStore = getIceTable().getContentFileStore();
    FileDescriptor fileDesc = cf.content() == FileContent.DATA ?
        fileStore.getDataFileDescriptor(pathHash) :
        fileStore.getDeleteFileDescriptor(pathHash);

    if (fileDesc == null) {
      if (tblRef_.getTimeTravelSpec() == null) {
        // We should always find the data files in the cache when not doing time travel.
        throw new ImpalaRuntimeException("Cannot find file in cache: " + cf.path()
            + " with snapshot id: " + String.valueOf(getIceTable().snapshotId()));
      }
      // We can still find the file descriptor among the old file descriptors.
      fileDesc = fileStore.getOldFileDescriptor(pathHash);
      if (fileDesc != null) {
        return new Pair<>(fileDesc, true);
      }
      cachehit = false;
      try {
        fileDesc = FeIcebergTable.Utils.getFileDescriptor(
            new Path(cf.path().toString()),
            new Path(getIceTable().getIcebergTableLocation()),
            getIceTable());
      } catch (IOException ex) {
        throw new ImpalaRuntimeException(
            "Cannot load file descriptor for " + cf.path(), ex);
      }
      if (fileDesc == null) {
        throw new ImpalaRuntimeException(
            "Cannot load file descriptor for: " + cf.path());
      }
      // Add file descriptor to the cache.
      fileDesc = fileDesc.cloneWithFileMetadata(
          IcebergUtil.createIcebergMetadata(getIceTable(), cf));
      fileStore.addOldFileDescriptor(pathHash, fileDesc);
    }
    return new Pair<>(fileDesc, cachehit);
  }

  /**
   * Extracts predicates from conjuncts_ that can be pushed down to Iceberg.
   *
   * Since Iceberg will filter data files by metadata instead of scan data files,
   * if any of the predicates refer to an Iceberg partitioning column
   * we pushdown all predicates to Iceberg to get the minimum data files to scan. If none
   * of the predicates refer to a partition column then we don't pushdown predicates
   * since this way we avoid calling the expensive 'planFiles()' API call.
   * Here are three cases for predicate pushdown:
   * 1.The column is not part of any Iceberg partition expression
   * 2.The column is part of all partition keys without any transformation (i.e. IDENTITY)
   * 3.The column is part of all partition keys with transformation (i.e. MONTH/DAY/HOUR)
   * We can use case 1 and 3 to filter data files, but also need to evaluate it in the
   * scan, for case 2 we don't need to evaluate it in the scan. So we evaluate all
   * predicates in the scan to keep consistency. More details about Iceberg scanning,
   * please refer: https://iceberg.apache.org/spec/#scan-planning
   */
  private void extractIcebergConjuncts() throws ImpalaException {
    boolean isPartitionColumnIncluded = false;
    Map<SlotId, SlotDescriptor> idToSlotDesc = new HashMap<>();
    for (SlotDescriptor slotDesc : tblRef_.getDesc().getSlots()) {
      idToSlotDesc.put(slotDesc.getId(), slotDesc);
    }
    for (Expr expr : conjuncts_) {
      if (isPartitionColumnIncluded(expr, idToSlotDesc)) {
        isPartitionColumnIncluded = true;
        break;
      }
    }
    if (!isPartitionColumnIncluded) {
      return;
    }
    for (Expr expr : conjuncts_) {
      tryConvertIcebergPredicate(expr);
    }
  }

  private boolean isPartitionColumnIncluded(Expr expr,
      Map<SlotId, SlotDescriptor> idToSlotDesc) {
    List<TupleId> tupleIds = Lists.newArrayList();
    List<SlotId> slotIds = Lists.newArrayList();
    expr.getIds(tupleIds, slotIds);

    if (tupleIds.size() > 1) return false;
    if (!tupleIds.get(0).equals(tblRef_.getDesc().getId())) return false;

    for (SlotId sId : slotIds) {
      SlotDescriptor slotDesc = idToSlotDesc.get(sId);
      if (slotDesc == null) continue;
      Column col = slotDesc.getColumn();
      if (col == null) continue;
      Preconditions.checkState(col instanceof IcebergColumn);
      IcebergColumn iceCol = (IcebergColumn)col;
      if (IcebergUtil.isPartitionColumn(iceCol,
          getIceTable().getDefaultPartitionSpec())) {
        return true;
      }
    }

    return false;
  }

  /**
   * Returns Iceberg operator by BinaryPredicate operator, or null if the operation
   * is not supported by Iceberg.
   */
  private Operation getIcebergOperator(BinaryPredicate.Operator op) {
    switch (op) {
      case EQ: return Operation.EQ;
      case NE: return Operation.NOT_EQ;
      case LE: return Operation.LT_EQ;
      case GE: return Operation.GT_EQ;
      case LT: return Operation.LT;
      case GT: return Operation.GT;
      default: return null;
    }
  }

  /**
   * Returns Iceberg operator by CompoundPredicate operator, or null if the operation
   * is not supported by Iceberg.
   */
  private Operation getIcebergOperator(CompoundPredicate.Operator op) {
    switch (op) {
      case AND: return Operation.AND;
      case OR: return Operation.OR;
      case NOT: return Operation.NOT;
      default: return null;
    }
  }

  /**
   * Transform impala predicate to iceberg predicate
   */
  private void tryConvertIcebergPredicate(Expr expr)
      throws ImpalaException {
    Expression predicate = convertIcebergPredicate(expr);
    if (predicate != null) {
      icebergPredicates_.add(predicate);
      LOG.debug("Push down the predicate: " + predicate + " to iceberg");
    }
  }

  private Expression convertIcebergPredicate(Expr expr)
      throws ImpalaException {
    if (expr instanceof BinaryPredicate) {
      return convertIcebergPredicate((BinaryPredicate) expr);
    } else if (expr instanceof InPredicate) {
      return convertIcebergPredicate((InPredicate) expr);
    } else if (expr instanceof IsNullPredicate) {
      return convertIcebergPredicate((IsNullPredicate) expr);
    } else if (expr instanceof CompoundPredicate) {
      return convertIcebergPredicate((CompoundPredicate) expr);
    } else {
      return null;
    }
  }

  private UnboundPredicate<Object> convertIcebergPredicate(BinaryPredicate predicate)
      throws ImpalaException {
    Operation op = getIcebergOperator(predicate.getOp());
    if (op == null) {
      return null;
    }

    // Do not convert if there is an implicit cast
    if (!(predicate.getChild(0) instanceof SlotRef)) {
      return null;
    }
    SlotRef ref = (SlotRef) predicate.getChild(0);

    if (!(predicate.getChild(1) instanceof LiteralExpr)) {
      return null;
    }
    LiteralExpr literal = (LiteralExpr) predicate.getChild(1);

    // If predicate contains map/struct, this column would be null
    Column col = ref.getDesc().getColumn();
    if (col == null) {
      return null;
    }

    // Cannot push BinaryPredicate with null literal values
    if (Expr.IS_NULL_LITERAL.apply(literal)) {
      return null;
    }

    Object value = getIcebergValue(ref, literal);
    if (value == null) {
      return null;
    }

    return Expressions.predicate(op, col.getName(), value);
  }

  private UnboundPredicate<Object> convertIcebergPredicate(InPredicate predicate)
      throws ImpalaException {
    // Do not convert if there is an implicit cast
    if (!(predicate.getChild(0) instanceof SlotRef)) {
      return null;
    }
    SlotRef ref = (SlotRef) predicate.getChild(0);

    // If predicate contains map/struct, this column would be null
    Column col = ref.getDesc().getColumn();
    if (col == null) {
      return null;
    }

    // Expressions takes a list of values as Objects
    List<Object> values = new ArrayList<>();
    for (int i = 1; i < predicate.getChildren().size(); ++i) {
      if (!Expr.IS_LITERAL.apply(predicate.getChild(i))) {
        return null;
      }
      LiteralExpr literal = (LiteralExpr) predicate.getChild(i);

      // Cannot push IN or NOT_IN predicate with null literal values
      if (Expr.IS_NULL_LITERAL.apply(literal)) {
        return null;
      }

      Object value = getIcebergValue(ref, literal);
      if (value == null) {
        return null;
      }

      values.add(value);
    }

    // According to the method:
    // 'org.apache.iceberg.expressions.InclusiveMetricsEvaluator.MetricsEvalVisitor#notIn'
    // Expressions.notIn only works when the push-down column is the partition column
    if (predicate.isNotIn())
      return Expressions.notIn(col.getName(), values);
    else {
      return Expressions.in(col.getName(), values);
    }
  }

  private UnboundPredicate<Object> convertIcebergPredicate(IsNullPredicate predicate) {
    // Do not convert if there is an implicit cast
    if (!(predicate.getChild(0) instanceof SlotRef)) {
      return null;
    }
    SlotRef ref = (SlotRef) predicate.getChild(0);

    // If predicate contains map/struct, this column would be null
    Column col = ref.getDesc().getColumn();
    if (col == null) {
      return null;
    }

    if (predicate.isNotNull()) {
      return Expressions.notNull(col.getName());
    } else{
      return Expressions.isNull(col.getName());
    }
  }

  private Expression convertIcebergPredicate(CompoundPredicate predicate)
      throws ImpalaException {
    Operation op = getIcebergOperator(predicate.getOp());
    if (op == null) {
      return null;
    }

    Expression left = convertIcebergPredicate(predicate.getChild(0));
    if (left == null) {
      return null;
    }
    if (op.equals(Operation.NOT)) {
      return Expressions.not(left);
    }

    Expression right = convertIcebergPredicate(predicate.getChild(1));
    if (right == null) {
      return null;
    }
    return op.equals(Operation.AND) ? Expressions.and(left, right)
        : Expressions.or(left, right);
  }

  private Object getIcebergValue(SlotRef ref, LiteralExpr literal)
      throws ImpalaException {
    IcebergColumn iceCol = (IcebergColumn) ref.getDesc().getColumn();
    Schema iceSchema = getIceTable().getIcebergSchema();
    switch (literal.getType().getPrimitiveType()) {
      case BOOLEAN: return ((BoolLiteral) literal).getValue();
      case TINYINT:
      case SMALLINT:
      case INT: return ((NumericLiteral) literal).getIntValue();
      case BIGINT: return ((NumericLiteral) literal).getLongValue();
      case FLOAT: return (float) ((NumericLiteral) literal).getDoubleValue();
      case DOUBLE: return ((NumericLiteral) literal).getDoubleValue();
      case STRING:
      case DATETIME:
      case CHAR: return ((StringLiteral) literal).getUnescapedValue();
      case TIMESTAMP: return getIcebergTsValue(literal, iceCol, iceSchema);
      case DATE: return ((DateLiteral) literal).getValue();
      case DECIMAL: return getIcebergDecimalValue(ref, (NumericLiteral) literal);
      default: {
        Preconditions.checkState(false,
            "Unsupported iceberg type considered for predicate: %s",
            literal.getType().toSql());
      }
    }
    return null;
  }

  private BigDecimal getIcebergDecimalValue(SlotRef ref, NumericLiteral literal) {
    Type colType = ref.getDesc().getColumn().getType();
    int scale = colType.getDecimalDigits();
    BigDecimal literalValue = literal.getValue();

    if (literalValue.scale() > scale) return null;
    // Iceberg DecimalLiteral needs to have the exact same scale.
    if (literalValue.scale() < scale) return literalValue.setScale(scale);
    return literalValue;
  }

  private Object getIcebergTsValue(LiteralExpr literal,
      IcebergColumn iceCol, Schema iceSchema) throws AnalysisException {
    try {
      org.apache.iceberg.types.Type iceType = iceSchema.findType(iceCol.getFieldId());
      Preconditions.checkState(iceType instanceof Types.TimestampType);
      Types.TimestampType tsType = (Types.TimestampType) iceType;
      if (tsType.shouldAdjustToUTC()) {
        return ExprUtil.localTimestampToUnixTimeMicros(analyzer_, literal);
      } else {
        return ExprUtil.utcTimestampToUnixTimeMicros(analyzer_, literal);
      }
    } catch (InternalException ex) {
      // We cannot interpret the timestamp literal. Maybe the timestamp is invalid,
      // or the local timestamp ambigously converts to UTC due to daylight saving
      // time backward turn. E.g. '2021-10-31 02:15:00 Europe/Budapest' converts to
      // either '2021-10-31 00:15:00 UTC' or '2021-10-31 01:15:00 UTC'.
      LOG.warn("Exception occurred during timestamp conversion: " + ex.toString() +
          "\nThis means timestamp predicate is not pushed to Iceberg, let Impala " +
          "backend handle it.");
    }
    return null;
  }

}
