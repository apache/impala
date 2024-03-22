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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.True;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.IcebergExpressionCollector;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.analysis.MultiAggregateInfo;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TimeTravelSpec;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.IcebergContentFileStore;
import org.apache.impala.catalog.IcebergEqualityDeleteTable;
import org.apache.impala.catalog.IcebergPositionDeleteTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.VirtualColumn;
import org.apache.impala.catalog.iceberg.IcebergMetadataTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.IcebergPredicateConverter;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Pair;
import org.apache.impala.fb.FbIcebergMetadata;
import org.apache.impala.planner.JoinNode.DistributionMode;
import org.apache.impala.thrift.TColumnStats;
import org.apache.impala.thrift.TIcebergPartitionTransformType;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TVirtualColumnType;
import org.apache.impala.util.IcebergUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scanning Iceberg tables is not as simple as scanning legacy Hive tables. The
 * complexity comes from handling delete files and time travel. We also want to push
 * down Impala conjuncts to Iceberg to filter out partitions and data files. This
 * class deals with such complexities.
 */
public class IcebergScanPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergScanPlanner.class);

  private Analyzer analyzer_;
  private PlannerContext ctx_;
  private TableRef tblRef_;
  private List<Expr> conjuncts_;
  private MultiAggregateInfo aggInfo_;
  // Mapping for translated Impala expressions
  private final Map<Expression, Expr> impalaIcebergPredicateMapping_ =
      new LinkedHashMap<>();
  // Residual expressions after Iceberg planning
  private final Set<Expression> residualExpressions_ =
      new TreeSet<>(Comparator.comparing(ExpressionUtil::toSanitizedString));
  // Expressions filtered by Iceberg's planFiles, subset of
  // 'impalaIcebergPredicateMapping_''s values
  private final List<Expr> skippedExpressions_ = new ArrayList<>();
  // Impala expressions that can't be translated into Iceberg expressions
  private final List<Expr> untranslatedExpressions_ = new ArrayList<>();
  // Conjuncts on columns not involved in IDENTITY-partitioning.
  private List<Expr> nonIdentityConjuncts_ = new ArrayList<>();

  // Containers for different groupings of file descriptors.
  private List<FileDescriptor> dataFilesWithoutDeletes_ = new ArrayList<>();
  private List<FileDescriptor> dataFilesWithDeletes_ = new ArrayList<>();
  private Set<FileDescriptor> positionDeleteFiles_ = new HashSet<>();

  // Holds all the equalityFieldIds from the equality delete file descriptors involved in
  // this query.
  private Set<Integer> allEqualityFieldIds_ = new HashSet<>();
  private Map<List<Integer>, Set<FileDescriptor>> equalityIdsToDeleteFiles_ =
      new HashMap<>();


  // Statistics about the data and delete files. Useful for memory estimates of the
  // ANTI JOIN
  private long positionDeletesRecordCount_ = 0;
  private long dataFilesWithDeletesSumPaths_ = 0;
  private long dataFilesWithDeletesMaxPath_ = 0;
  // Stores how many delete records are involved broken down by equality field ID lists.
  private Map<List<Integer>, Long> equalityDeletesRecordCount_ = new HashMap<>();
  private Set<Long> equalityDeleteSequenceNumbers_ = new HashSet<>();

  private final long snapshotId_;

  public IcebergScanPlanner(Analyzer analyzer, PlannerContext ctx,
      TableRef iceTblRef, List<Expr> conjuncts, MultiAggregateInfo aggInfo)
      throws ImpalaException {
    Preconditions.checkState(iceTblRef.getTable() instanceof FeIcebergTable ||
        iceTblRef.getTable() instanceof IcebergMetadataTable);
    analyzer_ = analyzer;
    ctx_ = ctx;
    tblRef_ = iceTblRef;
    conjuncts_ = conjuncts;
    aggInfo_ = aggInfo;
    extractIcebergConjuncts();
    snapshotId_ = IcebergUtil.getSnapshotId(getIceTable(), tblRef_.getTimeTravelSpec());
  }

  public PlanNode createIcebergScanPlan() throws ImpalaException {
    if (!needIcebergForPlanning()) {
      analyzer_.materializeSlots(conjuncts_);
      setFileDescriptorsBasedOnFileStore();
      return createIcebergScanPlanImpl();
    }

    filterFileDescriptors();
    filterConjuncts();
    analyzer_.materializeSlots(conjuncts_);
    return createIcebergScanPlanImpl();
  }

  /**
   * We can avoid calling Iceberg's expensive planFiles() API when the followings are
   * true:
   *  - we don't push down predicates
   *  - no time travel
   */
  private boolean needIcebergForPlanning() {
    return !impalaIcebergPredicateMapping_.isEmpty()
        || tblRef_.getTimeTravelSpec() != null;
  }

  private void setFileDescriptorsBasedOnFileStore() throws ImpalaException {
    IcebergContentFileStore fileStore = getIceTable().getContentFileStore();
    dataFilesWithoutDeletes_ = fileStore.getDataFilesWithoutDeletes();
    dataFilesWithDeletes_ = fileStore.getDataFilesWithDeletes();
    positionDeleteFiles_ = new HashSet<>(fileStore.getPositionDeleteFiles());
    initEqualityIds(fileStore.getEqualityDeleteFiles());

    updateDeleteStatistics();
  }

  private boolean noDeleteFiles() {
    return positionDeleteFiles_.isEmpty() && equalityIdsToDeleteFiles_.isEmpty();
  }

  private PlanNode createIcebergScanPlanImpl() throws ImpalaException {
    if (noDeleteFiles()) {
      Preconditions.checkState(
          !ctx_.getQueryCtx().isOptimize_count_star_for_iceberg_v2());
      // If there are no delete files we can just create a single SCAN node.
      Preconditions.checkState(dataFilesWithDeletes_.isEmpty());
      PlanNode ret = new IcebergScanNode(ctx_.getNextNodeId(), tblRef_, conjuncts_,
          aggInfo_, dataFilesWithoutDeletes_, nonIdentityConjuncts_,
          getSkippedConjuncts(), snapshotId_);
      ret.init(analyzer_);
      return ret;
    }

    PlanNode joinNode = null;
    if (!positionDeleteFiles_.isEmpty()) joinNode = createPositionJoinNode();

    if (!equalityIdsToDeleteFiles_.isEmpty()) joinNode = createEqualityJoinNode(joinNode);
    Preconditions.checkNotNull(joinNode);

    // If the count star query can be optimized for Iceberg V2 table, the number of rows
    // of all DataFiles without corresponding DeleteFiles can be calculated by Iceberg
    // meta files, it's added using ArithmeticExpr.
    if (ctx_.getQueryCtx().isOptimize_count_star_for_iceberg_v2()) return joinNode;

    // All data files has corresponding delete files, so we just return an ANTI JOIN
    // between all data files and all delete files.
    if (dataFilesWithoutDeletes_.isEmpty()) return joinNode;

    // If there are data files without corresponding delete files to be applied, we
    // can just create a SCAN node for these and do a UNION ALL with the ANTI JOIN.
    IcebergScanNode dataScanNode = new IcebergScanNode(
        ctx_.getNextNodeId(), tblRef_, conjuncts_, aggInfo_, dataFilesWithoutDeletes_,
        nonIdentityConjuncts_, getSkippedConjuncts(), snapshotId_);
    dataScanNode.init(analyzer_);
    List<Expr> outputExprs = tblRef_.getDesc().getSlots().stream().map(
        SlotRef::new).collect(Collectors.toList());
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
    Preconditions.checkState(positionDeletesRecordCount_ != 0);
    Preconditions.checkState(dataFilesWithDeletesSumPaths_ != 0);
    Preconditions.checkState(dataFilesWithDeletesMaxPath_ != 0);
    // The followings just create separate scan nodes for data files and position delete
    // files, plus adds a LEFT ANTI HASH JOIN above them.
    PlanNodeId dataScanNodeId = ctx_.getNextNodeId();
    PlanNodeId deleteScanNodeId = ctx_.getNextNodeId();
    IcebergPositionDeleteTable deleteTable = new IcebergPositionDeleteTable(getIceTable(),
        getIceTable().getName() + "-POSITION-DELETE-" + deleteScanNodeId.toString(),
        positionDeleteFiles_, positionDeletesRecordCount_, getFilePathStats());
    analyzer_.addVirtualTable(deleteTable);
    TableRef deleteDeltaRef = TableRef.newTableRef(analyzer_,
        Arrays.asList(deleteTable.getDb().getName(), deleteTable.getName()),
        tblRef_.getUniqueAlias() + "-position-delete");
    addDataVirtualPositionSlots(tblRef_);
    if (!equalityIdsToDeleteFiles_.isEmpty()) addAllSlotsForEqualityDeletes(tblRef_);
    addDeletePositionSlots(deleteDeltaRef);
    IcebergScanNode dataScanNode = new IcebergScanNode(
        dataScanNodeId, tblRef_, conjuncts_, aggInfo_, dataFilesWithDeletes_,
        nonIdentityConjuncts_, getSkippedConjuncts(), deleteScanNodeId, snapshotId_);
    dataScanNode.init(analyzer_);
    IcebergScanNode deleteScanNode = new IcebergScanNode(
        deleteScanNodeId,
        deleteDeltaRef,
        Collections.emptyList(), /*conjuncts*/
        aggInfo_,
        Lists.newArrayList(positionDeleteFiles_),
        Collections.emptyList(), /*nonIdentityConjuncts*/
        Collections.emptyList(), /*skippedConjuncts*/
        snapshotId_);
    deleteScanNode.init(analyzer_);

    // Now let's create the JOIN node
    List<BinaryPredicate> positionJoinConjuncts = createPositionJoinConjuncts(
        analyzer_, tblRef_.getDesc(), deleteDeltaRef.getDesc());

    TQueryOptions queryOpts = analyzer_.getQueryCtx().client_request.query_options;
    JoinNode joinNode = null;
    if (queryOpts.disable_optimized_iceberg_v2_read) {
      joinNode = new HashJoinNode(dataScanNode, deleteScanNode,
          /*straight_join=*/true, DistributionMode.NONE, JoinOperator.LEFT_ANTI_JOIN,
          positionJoinConjuncts, /*otherJoinConjuncts=*/Collections.emptyList());
    } else {
      joinNode =
          new IcebergDeleteNode(dataScanNode, deleteScanNode, positionJoinConjuncts);
    }
    joinNode.setId(ctx_.getNextNodeId());
    joinNode.init(analyzer_);
    joinNode.setIsDeleteRowsJoin();
    return joinNode;
  }

  private void addDataVirtualPositionSlots(TableRef tblRef) throws AnalysisException {
    List<String> rawPath = Lists.newArrayList(
        tblRef.getUniqueAlias(), VirtualColumn.INPUT_FILE_NAME.getName());
    SlotDescriptor fileNameSlotDesc =
        SingleNodePlanner.addSlotRefToDesc(analyzer_, rawPath);
    fileNameSlotDesc.setStats(virtualInputFileNameStats());

    rawPath = Lists.newArrayList(
        tblRef.getUniqueAlias(), VirtualColumn.FILE_POSITION.getName());
    SlotDescriptor filePosSlotDesc =
        SingleNodePlanner.addSlotRefToDesc(analyzer_, rawPath);
    filePosSlotDesc.setStats(virtualFilePositionStats());
  }

  private void addAllSlotsForEqualityDeletes(TableRef tblRef) throws AnalysisException {
    addSlotsForEqualityDeletes(Lists.newArrayList(allEqualityFieldIds_), tblRef);
  }

  private void addSlotsForEqualityDeletes(List<Integer> equalityFieldIds, TableRef tblRef)
      throws AnalysisException {
    Preconditions.checkState(!equalityFieldIds.isEmpty());

    List<String> rawPath = Lists.newArrayList(
        tblRef.getUniqueAlias(), VirtualColumn.ICEBERG_DATA_SEQUENCE_NUMBER.getName());
    SlotDescriptor slotDesc = SingleNodePlanner.addSlotRefToDesc(analyzer_, rawPath);
    slotDesc.setStats(virtualDataSeqNumStats());

    for (Integer eqId : equalityFieldIds) {
      String eqColName = getIceTable().getIcebergSchema().findColumnName(eqId);
      Preconditions.checkNotNull(eqColName);
      rawPath = Lists.newArrayList(tblRef.getUniqueAlias(), eqColName);
      SingleNodePlanner.addSlotRefToDesc(analyzer_, rawPath);
    }
  }

  private void addDeletePositionSlots(TableRef tblRef)
      throws AnalysisException {
    SingleNodePlanner.addSlotRefToDesc(analyzer_,
        Lists.newArrayList(
            tblRef.getUniqueAlias(), IcebergPositionDeleteTable.FILE_PATH_COLUMN));
    SingleNodePlanner.addSlotRefToDesc(analyzer_,
        Lists.newArrayList(
            tblRef.getUniqueAlias(), IcebergPositionDeleteTable.POS_COLUMN));
  }

  private List<BinaryPredicate> createPositionJoinConjuncts(Analyzer analyzer,
      TupleDescriptor insertTupleDesc, TupleDescriptor deleteTupleDesc)
      throws AnalysisException {
    List<BinaryPredicate> ret = new ArrayList<>();
    BinaryPredicate filePathEq = null;
    BinaryPredicate posEq = null;
    for (SlotDescriptor deleteSlotDesc : deleteTupleDesc.getSlots()) {
      boolean foundMatch = false;
      Column col = deleteSlotDesc.getParent().getTable().getColumns().get(
          deleteSlotDesc.getMaterializedPath().get(0));
      Preconditions.checkState(col instanceof IcebergColumn);
      int fieldId = ((IcebergColumn)col).getFieldId();
      for (SlotDescriptor insertSlotDesc : insertTupleDesc.getSlots()) {
        TVirtualColumnType virtColType = insertSlotDesc.getVirtualColumnType();
        if (fieldId == IcebergTable.V2_FILE_PATH_FIELD_ID
            && virtColType == TVirtualColumnType.INPUT_FILE_NAME) {
          foundMatch = true;
          filePathEq = new BinaryPredicate(
              Operator.EQ, new SlotRef(insertSlotDesc), new SlotRef(deleteSlotDesc));
          filePathEq.analyze(analyzer);
          break;
        }
        if (fieldId == IcebergTable.V2_POS_FIELD_ID
            && virtColType == TVirtualColumnType.FILE_POSITION) {
          foundMatch = true;
          posEq = new BinaryPredicate(
              Operator.EQ, new SlotRef(insertSlotDesc), new SlotRef(deleteSlotDesc));
          posEq.analyze(analyzer);
          break;
        }
      }
      Preconditions.checkState(foundMatch);
    }
    Preconditions.checkState(filePathEq != null);
    Preconditions.checkState(posEq != null);
    ret.add(filePathEq);
    ret.add(posEq);
    return ret;
  }

  private Pair<List<BinaryPredicate>, List<Expr>> createEqualityJoinConjuncts(
      Analyzer analyzer, TupleDescriptor dataTupleDesc, TupleDescriptor deleteTupleDesc)
      throws AnalysisException, ImpalaRuntimeException {
    // Pre-process the slots for faster lookup by field ID.
    Map<Integer, SlotDescriptor> fieldIdToIcebergColumn = new HashMap<>();
    SlotDescriptor dataSeqNumSlot = null;
    for (SlotDescriptor dataSlotDesc : dataTupleDesc.getSlots()) {
      if (dataSlotDesc.getVirtualColumnType() ==
          TVirtualColumnType.ICEBERG_DATA_SEQUENCE_NUMBER) {
        dataSeqNumSlot = dataSlotDesc;
      } else if (dataSlotDesc.getColumn() instanceof IcebergColumn) {
        IcebergColumn icebergCol = (IcebergColumn)dataSlotDesc.getColumn();
        fieldIdToIcebergColumn.put(icebergCol.getFieldId(), dataSlotDesc);
      }
    }

    List<BinaryPredicate> eqPredicates = new ArrayList<>();
    List<Expr> seqNumPredicate = new ArrayList<>();
    for (SlotDescriptor deleteSlotDesc : deleteTupleDesc.getSlots()) {
      if (deleteSlotDesc.getVirtualColumnType() ==
          TVirtualColumnType.ICEBERG_DATA_SEQUENCE_NUMBER) {
        BinaryPredicate pred = new BinaryPredicate(Operator.LT,
            new SlotRef(dataSeqNumSlot), new SlotRef(deleteSlotDesc));
        pred.analyze(analyzer);
        seqNumPredicate.add(pred);
      } else {
        Preconditions.checkState(deleteSlotDesc.getColumn() instanceof IcebergColumn);
        int fieldId = ((IcebergColumn)deleteSlotDesc.getColumn()).getFieldId();
        if (!fieldIdToIcebergColumn.containsKey(fieldId)) {
          throw new ImpalaRuntimeException("Field ID not found in table: " + fieldId);
        }
        SlotRef dataSlotRef = new SlotRef(fieldIdToIcebergColumn.get(fieldId));
        SlotRef deleteSlotRef = new SlotRef(deleteSlotDesc);
        BinaryPredicate eqColPred = new BinaryPredicate(
            Operator.NOT_DISTINCT, dataSlotRef, deleteSlotRef);
        eqColPred.analyze(analyzer);
        eqPredicates.add(eqColPred);
      }
    }
    return new Pair<>(eqPredicates, seqNumPredicate);
  }

  private ColumnStats virtualInputFileNameStats() {
    ColumnStats ret = new ColumnStats(Type.STRING);
    ret.setNumDistinctValues(dataFilesWithDeletes_.size());
    return ret;
  }

  private ColumnStats virtualFilePositionStats() {
    ColumnStats ret = new ColumnStats(Type.BIGINT);
    ret.setNumDistinctValues(positionDeletesRecordCount_ / dataFilesWithDeletes_.size());
    return ret;
  }

  private ColumnStats virtualDataSeqNumStats() {
    ColumnStats ret = new ColumnStats(Type.BIGINT);
    ret.setNumDistinctValues(equalityDeleteSequenceNumbers_.size());
    return ret;
  }

  private PlanNode createEqualityJoinNode(PlanNode positionJoinNode)
      throws ImpalaException {
    Preconditions.checkState(!equalityIdsToDeleteFiles_.isEmpty());

    if (getIceTable().getPartitionSpecs().size() > 1) {
      throw new ImpalaRuntimeException("Equality delete files are not supported for " +
          "tables with partition evolution");
    }

    PlanNode leftSideOfJoin = null;
    if (positionJoinNode != null) {
      leftSideOfJoin = positionJoinNode;
    } else {
      PlanNodeId dataScanNodeId = ctx_.getNextNodeId();
      IcebergScanNode dataScanNode = new IcebergScanNode(
          dataScanNodeId, tblRef_, conjuncts_, aggInfo_, dataFilesWithDeletes_,
          nonIdentityConjuncts_, getSkippedConjuncts(), snapshotId_);
      addAllSlotsForEqualityDeletes(tblRef_);
      dataScanNode.init(analyzer_);

      leftSideOfJoin = dataScanNode;
    }

    List<List<Integer>> orderedEqualityFieldIds =
        getOrderedEqualityFieldIds(equalityDeletesRecordCount_);
    JoinNode joinNode = null;
    for (List<Integer> equalityIds : orderedEqualityFieldIds) {
      Set<FileDescriptor> equalityDeleteFiles =
          equalityIdsToDeleteFiles_.get(equalityIds);
      Preconditions.checkState(equalityDeleteFiles != null &&
          !equalityDeleteFiles.isEmpty());
      Long numRecordsInDeletes = equalityDeletesRecordCount_.get(equalityIds);
      Preconditions.checkState(numRecordsInDeletes != null && numRecordsInDeletes > 0);

      PlanNodeId deleteScanNodeId = ctx_.getNextNodeId();
      IcebergEqualityDeleteTable deleteTable =
          new IcebergEqualityDeleteTable(getIceTable(),
              getIceTable().getName() + "-EQUALITY-DELETE-" + deleteScanNodeId.toString(),
              equalityDeleteFiles, equalityIds, numRecordsInDeletes);
      analyzer_.addVirtualTable(deleteTable);

      TableRef deleteTblRef = TableRef.newTableRef(analyzer_,
          Arrays.asList(deleteTable.getDb().getName(), deleteTable.getName()),
          tblRef_.getUniqueAlias() + "-equality-delete-" + deleteScanNodeId.toString());
      addSlotsForEqualityDeletes(equalityIds, deleteTblRef);

      // TODO IMPALA-12608: As an optimization we can populate the conjuncts below that
      // are relevant for the delete scan node.
      IcebergScanNode deleteScanNode = new IcebergScanNode(
          deleteScanNodeId,
          deleteTblRef,
          Collections.emptyList(), /*conjuncts*/
          aggInfo_,
          Lists.newArrayList(equalityDeleteFiles),
          Collections.emptyList(), /*nonIdentityConjuncts*/
          Collections.emptyList(),
          snapshotId_); /*skippedConjuncts*/
      deleteScanNode.init(analyzer_);

      Pair<List<BinaryPredicate>, List<Expr>> equalityJoinConjuncts =
          createEqualityJoinConjuncts(
              analyzer_, tblRef_.getDesc(), deleteTblRef.getDesc());

      joinNode = new HashJoinNode(leftSideOfJoin, deleteScanNode,
          /*straight_join=*/true, DistributionMode.NONE, JoinOperator.LEFT_ANTI_JOIN,
          equalityJoinConjuncts.first, equalityJoinConjuncts.second);
      joinNode.setId(ctx_.getNextNodeId());
      joinNode.init(analyzer_);
      leftSideOfJoin = joinNode;
    }
    return joinNode;
  }

  /**
   * Based on the equality delete fields ID lists and the number of delete rows associated
   * to them this function gives back the equality field ID lists in an order we'd like to
   * use them for join node creation.
   * The more delete rows are involved the earlier the equality field ID list is placed in
   * the ordering. If some equality field ID lists have the same amount of delete rows
   * then the order between them is decided by preferring the longest lists first and in
   * case of equal length the numerical order of the field IDs in the lists is used.
   * E.g. If they have the same amount of delete rows then [1,2] comes before [1] because
   * of the length and comes before [2,3] because of the numerical order.
   */
  static List<List<Integer>> getOrderedEqualityFieldIds(
      Map<List<Integer>, Long> equalityDeletesRecordCount) {
    Preconditions.checkState(!equalityDeletesRecordCount.isEmpty());

    return equalityDeletesRecordCount.entrySet().stream()
        .sorted(Map.Entry.<List<Integer>, Long>comparingByValue().reversed()
            .thenComparing((e1, e2) -> {
                List<Integer> list1 = e1.getKey();
                List<Integer> list2 = e2.getKey();
                // Order the longest equality field ID lists first.
                if (list1.size() < list2.size()) return 1;
                if (list2.size() < list1.size()) return -1;

                for (int i = 0; i < list1.size(); ++i) {
                  if (list1.get(i) < list2.get(i)) return -1;
                  if (list2.get(i) < list1.get(i)) return 1;
                }
                return 0;
            }))
        .map(e -> e.getKey())
        .collect(Collectors.toList());
  }

  private void filterFileDescriptors() throws ImpalaException {
    Preconditions.checkState(allEqualityFieldIds_.isEmpty());
    Preconditions.checkState(equalityIdsToDeleteFiles_.isEmpty());

    TimeTravelSpec timeTravelSpec = tblRef_.getTimeTravelSpec();

    try (CloseableIterable<FileScanTask> fileScanTasks =
        IcebergUtil.planFiles(getIceTable(),
            new ArrayList<>(impalaIcebergPredicateMapping_.keySet()), timeTravelSpec)) {
      long dataFilesCacheMisses = 0;
      for (FileScanTask fileScanTask : fileScanTasks) {
        Expression residualExpr = fileScanTask.residual();
        if (residualExpr != null && !(residualExpr instanceof True)) {
          residualExpressions_.add(residualExpr);
        }
        Pair<FileDescriptor, Boolean> fileDesc = getFileDescriptor(fileScanTask.file());
        if (!fileDesc.second) ++dataFilesCacheMisses;
        if (fileScanTask.deletes().isEmpty()) {
          dataFilesWithoutDeletes_.add(fileDesc.first);
        } else {
          dataFilesWithDeletes_.add(fileDesc.first);
          for (DeleteFile delFile : fileScanTask.deletes()) {
            Pair<FileDescriptor, Boolean> delFileDesc = getFileDescriptor(delFile);
            if (!delFileDesc.second) ++dataFilesCacheMisses;
            if (delFile.content() == FileContent.EQUALITY_DELETES) {
              addEqualityDeletesAndIds(delFileDesc.first);
            } else {
              Preconditions.checkState(delFile.content() == FileContent.POSITION_DELETES);
              positionDeleteFiles_.add(delFileDesc.first);
            }
          }
        }
      }
      if (dataFilesCacheMisses > 0) {
        Preconditions.checkState(timeTravelSpec != null);
        LOG.info("File descriptors had to be loaded on demand during time travel: {}",
            dataFilesCacheMisses);
      }
    } catch (IOException | TableLoadingException e) {
      throw new ImpalaRuntimeException(String.format(
          "Failed to load data files for Iceberg table: %s", getIceTable().getFullName()),
          e);
    }
    updateDeleteStatistics();
  }

  private void addEqualityDeletesAndIds(FileDescriptor fd) {
    FbIcebergMetadata fileMetadata = fd.getFbFileMetadata().icebergMetadata();
    List<Integer> eqFieldIdList = new ArrayList<>();
    for (int i = 0; i < fileMetadata.equalityFieldIdsLength(); ++i) {
      eqFieldIdList.add(fileMetadata.equalityFieldIds(i));
      allEqualityFieldIds_.add(fileMetadata.equalityFieldIds(i));
    }
    if (!equalityIdsToDeleteFiles_.containsKey(eqFieldIdList)) {
      equalityIdsToDeleteFiles_.put(eqFieldIdList, new HashSet<>());
    }
    equalityIdsToDeleteFiles_.get(eqFieldIdList).add(fd);
  }

  private void initEqualityIds(List<FileDescriptor> equalityDeleteFiles) {
    Preconditions.checkState(allEqualityFieldIds_.isEmpty());
    Preconditions.checkState(equalityIdsToDeleteFiles_.isEmpty());
    for (FileDescriptor fd : equalityDeleteFiles) addEqualityDeletesAndIds(fd);
  }

  private void filterConjuncts() {
    if (residualExpressions_.isEmpty()) {
      conjuncts_.removeAll(impalaIcebergPredicateMapping_.values());
      return;
    }
    if (!analyzer_.getQueryOptions().iceberg_predicate_pushdown_subsetting) return;
    trySubsettingPredicatesBeingPushedDown();
  }

  private boolean trySubsettingPredicatesBeingPushedDown() {
    long startTime = System.currentTimeMillis();
    List<Expr> expressionsToRetain = new ArrayList<>(untranslatedExpressions_);
    for (Expression expression : residualExpressions_) {
      List<Expression> locatedExpressions = ExpressionVisitors.visit(expression,
          new IcebergExpressionCollector());
      for (Expression located : locatedExpressions) {
        Expr expr = impalaIcebergPredicateMapping_.get(located);
        // If we fail to locate any of the Iceberg residual expressions then we skip
        // filtering the predicates to be pushed down to Impala scanner.
        if (expr == null) return false;
        expressionsToRetain.add(expr);
      }
    }
    skippedExpressions_.addAll(
        conjuncts_.stream().filter(expr -> !expressionsToRetain.contains(expr)).collect(
            Collectors.toList()));
    conjuncts_ = expressionsToRetain;
    LOG.debug("Iceberg predicate pushdown subsetting took {} ms",
        (System.currentTimeMillis() - startTime));
    return true;
  }

  private List<Expr> getSkippedConjuncts() {
    if (!residualExpressions_.isEmpty()) return skippedExpressions_;
    return new ArrayList<>(impalaIcebergPredicateMapping_.values());
  }

  private void updateDeleteStatistics() {
    for (FileDescriptor fd : dataFilesWithDeletes_) {
      updateDataFilesWithDeletesStatistics(fd);
    }
    for (FileDescriptor fd : positionDeleteFiles_) {
      updatePositionDeleteFilesStatistics(fd);
    }
    for (Map.Entry<List<Integer>, Set<FileDescriptor>> entry :
        equalityIdsToDeleteFiles_.entrySet()) {
      updateEqualityDeleteFilesStatistics(entry.getKey(), entry.getValue());
    }
  }

  private void updateDataFilesWithDeletesStatistics(FileDescriptor fd) {
    String path = fd.getAbsolutePath(getIceTable().getLocation());
    long pathSize = path.length();
    dataFilesWithDeletesSumPaths_ += pathSize;
    if (pathSize > dataFilesWithDeletesMaxPath_) {
      dataFilesWithDeletesMaxPath_ = pathSize;
    }
  }

  private void updatePositionDeleteFilesStatistics(FileDescriptor fd) {
    positionDeletesRecordCount_ += getRecordCount(fd);
  }

  private void updateEqualityDeleteFilesStatistics(List<Integer> equalityIds,
      Set<FileDescriptor> fileDescriptors) {
    long numRecords = 0;
    for (FileDescriptor fd : fileDescriptors) {
      numRecords += getRecordCount(fd);
      equalityDeleteSequenceNumbers_.add(
          fd.getFbFileMetadata().icebergMetadata().dataSequenceNumber());
    }
    equalityDeletesRecordCount_.put(equalityIds, numRecords);
  }

  private long getRecordCount(FileDescriptor fd) {
    long recordCount = fd.getFbFileMetadata().icebergMetadata().recordCount();
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
            + " with snapshot id: " + getIceTable().snapshotId());
      }
      // We can still find the file descriptor among the old file descriptors.
      fileDesc = fileStore.getOldFileDescriptor(pathHash);
      if (fileDesc != null) {
        return new Pair<>(fileDesc, true);
      }
      cachehit = false;
      try {
        fileDesc = FeIcebergTable.Utils.getFileDescriptor(cf, getIceTable());
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
    // Track identity conjuncts by their index in conjuncts_.
    // The array values are initialized to false.
    boolean[] identityConjunctIndex = new boolean[conjuncts_.size()];
    for (SlotDescriptor slotDesc : tblRef_.getDesc().getSlots()) {
      idToSlotDesc.put(slotDesc.getId(), slotDesc);
    }
    for (int i = 0; i < conjuncts_.size(); i++) {
      Expr expr = conjuncts_.get(i);
      if (isPartitionColumnIncluded(expr, idToSlotDesc)) {
        isPartitionColumnIncluded = true;
        if (isIdentityPartitionIncluded(expr, idToSlotDesc)) {
          identityConjunctIndex[i] = true;
        }
      }
    }
    if (!isPartitionColumnIncluded) {
      // No partition conjuncts, i.e. every conjunct is non-identity conjunct.
      nonIdentityConjuncts_ = conjuncts_;
      return;
    }
    for (int i = 0; i < conjuncts_.size(); i++) {
      Expr expr = conjuncts_.get(i);
      if (tryConvertIcebergPredicate(expr)) {
        if (!identityConjunctIndex[i]) {
          nonIdentityConjuncts_.add(expr);
        }
      } else {
        nonIdentityConjuncts_.add(expr);
      }
    }
  }

  private boolean isPartitionColumnIncluded(Expr expr,
      Map<SlotId, SlotDescriptor> idToSlotDesc) {
    return hasPartitionTransformType(expr, idToSlotDesc,
        transformType -> transformType != TIcebergPartitionTransformType.VOID);
  }

  private boolean isIdentityPartitionIncluded(Expr expr,
      Map<SlotId, SlotDescriptor> idToSlotDesc) {
    return hasPartitionTransformType(expr, idToSlotDesc,
        transformType -> transformType == TIcebergPartitionTransformType.IDENTITY);
  }

  private boolean hasPartitionTransformType(Expr expr,
      Map<SlotId, SlotDescriptor> idToSlotDesc,
      Predicate<TIcebergPartitionTransformType> pred) {
    List<TupleId> tupleIds = Lists.newArrayList();
    List<SlotId> slotIds = Lists.newArrayList();
    expr.getIds(tupleIds, slotIds);

    if (tupleIds.size() != 1) return false;
    if (!tupleIds.get(0).equals(tblRef_.getDesc().getId())) return false;

    for (SlotId sId : slotIds) {
      SlotDescriptor slotDesc = idToSlotDesc.get(sId);
      if (slotDesc == null) continue;
      Column col = slotDesc.getColumn();
      if (col == null) continue;
      Preconditions.checkState(col instanceof IcebergColumn);
      IcebergColumn iceCol = (IcebergColumn)col;
      TIcebergPartitionTransformType transformType =
          IcebergUtil.getPartitionTransformType(
              iceCol,
              getIceTable().getDefaultPartitionSpec());
      if (pred.test(transformType)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Transform Impala predicate to Iceberg predicate
   */
  private boolean tryConvertIcebergPredicate(Expr expr) {
    IcebergPredicateConverter converter =
        new IcebergPredicateConverter(getIceTable().getIcebergSchema(), analyzer_);
    try {
      Expression predicate = converter.convert(expr);
      impalaIcebergPredicateMapping_.put(predicate, expr);
      LOG.debug("Push down the predicate: {} to iceberg", predicate);
      return true;
    }
    catch (ImpalaException e) {
      untranslatedExpressions_.add(expr);
      return false;
    }
  }

}
