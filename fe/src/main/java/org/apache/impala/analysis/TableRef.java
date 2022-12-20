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

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.planner.JoinNode.DistributionMode;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.TReplicaPreference;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Superclass of all table references, including references to views, base tables
 * (Hdfs, HBase or DataSource tables), and nested collections. Contains the join
 * specification. An instance of a TableRef (and not a subclass thereof) represents
 * an unresolved table reference that must be resolved during analysis. All resolved
 * table references are subclasses of TableRef.
 *
 * The analysis of table refs follows a two-step process:
 *
 * 1. Resolution: A table ref's path is resolved and then the generic TableRef is
 * replaced by a concrete table ref (a BaseTableRef, CollectionTableRef or ViewRef)
 * in the originating stmt and that is given the resolved path. This step is driven by
 * Analyzer.resolveTableRef().
 *
 * 2. Analysis/registration: After resolution, the concrete table ref is analyzed
 * to register a tuple descriptor for its resolved path and register other table-ref
 * specific state with the analyzer (e.g., whether it is outer/semi joined, etc.).
 *
 * Therefore, subclasses of TableRef should never call the analyze() of its superclass.
 *
 * TODO for 2.3: The current TableRef class hierarchy and the related two-phase analysis
 * feels convoluted and is hard to follow. We should reorganize the TableRef class
 * structure for clarity of analysis and avoid a table ref 'switching genders' in between
 * resolution and registration.
 *
 * TODO for 2.3: Rename this class to CollectionRef and re-consider the naming and
 * structure of all subclasses.
 */
public class TableRef extends StmtNode {
  // Path to a collection type. Not set for inline views.
  protected List<String> rawPath_;

  // Legal aliases of this table ref. Contains the explicit alias as its sole element if
  // there is one. Otherwise, contains the two implicit aliases. Implicit aliases are set
  // in the c'tor of the corresponding resolved table ref (subclasses of TableRef) during
  // analysis. By convention, for table refs with multiple implicit aliases, aliases_[0]
  // contains the fully-qualified implicit alias to ensure that aliases_[0] always
  // uniquely identifies this table ref regardless of whether it has an explicit alias.
  protected String[] aliases_;

  // Indicates whether this table ref is given an explicit alias,
  protected boolean hasExplicitAlias_;

  // Analysis registers privilege and/or audit requests based on this privilege.
  protected final Privilege priv_;
  protected final boolean requireGrantOption_;

  // Optional TABLESAMPLE clause. Null if not specified.
  protected TableSampleClause sampleParams_;

  protected JoinOperator joinOp_;
  protected List<PlanHint> joinHints_ = new ArrayList<>();
  protected List<String> usingColNames_;
  // Whether we should allow an empty ON clause in cases where it would usually be
  // rejected in analysis. Used for TableRefs generated during query rewriting.
  protected boolean allowEmptyOn_ = false;

  protected List<PlanHint> tableHints_ = new ArrayList<>();
  protected TReplicaPreference replicaPreference_;
  protected boolean randomReplica_;
  // A sampling percent if convert_limit_to_sample hint is supplied. Valid range
  // is 1-100. -1 indicates no hint.
  protected int convertLimitToSampleHintPercent_;

  // Hinted distribution mode for this table ref; set after analyzeJoinHints()
  // TODO: Move join-specific members out of TableRef.
  private DistributionMode distrMode_ = DistributionMode.NONE;

  public enum ZippingUnnestType {
    NONE,
    FROM_CLAUSE_ZIPPING_UNNEST,
    SELECT_LIST_ZIPPING_UNNEST
  }

  // Indicates if this TableRef is for the purpose of zipping unnest for arrays.
  protected ZippingUnnestType zippingUnnestType_ = ZippingUnnestType.NONE;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // Resolution of rawPath_ if applicable. Result of analysis.
  protected Path resolvedPath_;

  protected Expr onClause_;

  // the ref to the left of us, if we're part of a JOIN clause
  protected TableRef leftTblRef_;

  // true if this TableRef has been analyzed; implementing subclass should set it to true
  // at the end of analyze() call.
  protected boolean isAnalyzed_;

  // Lists of table ref ids and materialized tuple ids of the full sequence of table
  // refs up to and including this one. These ids are cached during analysis because
  // we may alter the chain of table refs during plan generation, but we still rely
  // on the original list of ids for correct predicate assignment.
  // Populated in analyzeJoin().
  protected List<TupleId> allTableRefIds_ = new ArrayList<>();
  protected List<TupleId> allMaterializedTupleIds_ = new ArrayList<>();

  // All physical tuple ids that this table ref is correlated with:
  // Tuple ids of root descriptors from outer query blocks that this table ref
  // (if a CollectionTableRef) or contained CollectionTableRefs (if an InlineViewRef)
  // are rooted at. Populated during analysis.
  protected List<TupleId> correlatedTupleIds_ = new ArrayList<>();

  // analysis output
  protected TupleDescriptor desc_;

  // true if this table is masked by a table masking view and need to expose its nested
  // columns via the view.
  protected boolean exposeNestedColumnsByTableMaskView_ = false;

  // Columns referenced in the query. Used in resolving column mask.
  protected Map<String, Column> columns_ = new HashMap<>();

  // Time travel spec of this table ref. It contains information specified in the
  // FOR SYSTEM_TIME AS OF <timestamp> or FOR SYSTEM_TIME AS OF <version> clause.
  protected TimeTravelSpec timeTravelSpec_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  // true if this table ref is hidden, because e.g. it was generated during statement
  // rewrite.
  private boolean isHidden_ = false;

  // Value of query hint 'TABLE_NUM_ROWS' on this table. Used in constructing ScanNode if
  // the table does not have stats, or has corrupt stats. -1 indicates no hint. Currently,
  // this hint is valid for hdfs and kudu table.
  private long tableNumRowsHint_ = -1;
  // Table row hint name used in sql.
  private static final String TABLE_ROW_HINT = "TABLE_NUM_ROWS";

  /**
   * Returns a new, resolved, and analyzed table ref.
   */
  public static TableRef newTableRef(Analyzer analyzer, List<String> rawPath,
      String alias) throws AnalysisException {
    TableRef ret = new TableRef(rawPath, alias);
    ret = analyzer.resolveTableRef(ret);
    ret.analyze(analyzer);
    return ret;
  }

  public TableRef(List<String> path, String alias) {
    this(path, alias, Privilege.SELECT);
  }

  public TableRef(List<String> path, String alias, TableSampleClause tableSample) {
    this(path, alias, tableSample, null);
  }

  public TableRef(List<String> path, String alias, TableSampleClause tableSample,
      TimeTravelSpec timeTravel) {
    this(path, alias, tableSample, timeTravel, Privilege.SELECT, false);
  }

  public TableRef(List<String> path, String alias, Privilege priv) {
    this(path, alias, null, null, priv, false);
  }

  public TableRef(List<String> path, String alias, Privilege priv,
      boolean requireGrantOption) {
    this(path, alias, null, null, priv, requireGrantOption);
  }

  public TableRef(List<String> path, String alias, TableSampleClause sampleParams,
      TimeTravelSpec timeTravel, Privilege priv, boolean requireGrantOption) {
    rawPath_ = path;
    if (alias != null) {
      aliases_ = new String[] { alias.toLowerCase() };
      hasExplicitAlias_ = true;
    } else {
      hasExplicitAlias_ = false;
    }
    sampleParams_ = sampleParams;
    priv_ = priv;
    requireGrantOption_ = requireGrantOption;
    isAnalyzed_ = false;
    replicaPreference_ = null;
    randomReplica_ = false;
    convertLimitToSampleHintPercent_ = -1;
    timeTravelSpec_ = timeTravel;
  }

  /**
   * C'tor for cloning.
   */
  protected TableRef(TableRef other) {
    rawPath_ = other.rawPath_;
    resolvedPath_ = other.resolvedPath_;
    aliases_ = other.aliases_;
    hasExplicitAlias_ = other.hasExplicitAlias_;
    sampleParams_ = other.sampleParams_;
    timeTravelSpec_ = other.timeTravelSpec_ != null ?
                      other.timeTravelSpec_.clone() : null;
    priv_ = other.priv_;
    requireGrantOption_ = other.requireGrantOption_;
    joinOp_ = other.joinOp_;
    joinHints_ = Lists.newArrayList(other.joinHints_);
    onClause_ = (other.onClause_ != null) ? other.onClause_.clone() : null;
    usingColNames_ =
        (other.usingColNames_ != null) ? Lists.newArrayList(other.usingColNames_) : null;
    allowEmptyOn_ = other.allowEmptyOn_;
    tableHints_ = Lists.newArrayList(other.tableHints_);
    replicaPreference_ = other.replicaPreference_;
    convertLimitToSampleHintPercent_ = other.convertLimitToSampleHintPercent_;
    randomReplica_ = other.randomReplica_;
    distrMode_ = other.distrMode_;
    // The table ref links are created at the statement level, so cloning a set of linked
    // table refs is the responsibility of the statement.
    leftTblRef_ = null;
    isAnalyzed_ = other.isAnalyzed_;
    allTableRefIds_ = Lists.newArrayList(other.allTableRefIds_);
    allMaterializedTupleIds_ = Lists.newArrayList(other.allMaterializedTupleIds_);
    correlatedTupleIds_ = Lists.newArrayList(other.correlatedTupleIds_);
    desc_ = other.desc_;
    exposeNestedColumnsByTableMaskView_ = other.exposeNestedColumnsByTableMaskView_;
    columns_ = new LinkedHashMap<>(other.columns_);
    isHidden_ = other.isHidden_;
    zippingUnnestType_ = other.zippingUnnestType_;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    throw new IllegalStateException(
        "Should not call analyze() on an unresolved TableRef.");
  }

  /**
   * Creates and returns an empty TupleDescriptor registered with the analyzer
   * based on the resolvedPath_.
   * This method is called from the analyzer when registering this table reference.
   */
  public TupleDescriptor createTupleDescriptor(Analyzer analyzer)
      throws AnalysisException {
    TupleDescriptor result = analyzer.getDescTbl().createTupleDescriptor(
        getClass().getSimpleName() + " " + getUniqueAlias());
    result.setPath(resolvedPath_);
    return result;
  }

  /**
   * Set this table's context-dependent join attributes from the given table.
   * Does not clone the attributes.
   */
  protected void setJoinAttrs(TableRef other) {
    this.joinOp_ = other.joinOp_;
    this.joinHints_ = other.joinHints_;
    this.tableHints_ = other.tableHints_;
    this.onClause_ = other.onClause_;
    this.usingColNames_ = other.usingColNames_;
    this.allowEmptyOn_ = other.allowEmptyOn_;
  }

  public JoinOperator getJoinOp() {
    // if it's not explicitly set, we're doing an inner join
    return (joinOp_ == null ? JoinOperator.INNER_JOIN : joinOp_);
  }

  public boolean allowEmptyOn() { return allowEmptyOn_ ; }
  public void setAllowEmptyOn(boolean v) { allowEmptyOn_ = v; }

  public TReplicaPreference getReplicaPreference() { return replicaPreference_; }
  public boolean getRandomReplica() { return randomReplica_; }
  public boolean hasConvertLimitToSampleHint() {
    return convertLimitToSampleHintPercent_ != -1;
  }
  public int getConvertLimitToSampleHintPercent() {
    return convertLimitToSampleHintPercent_;
  }

  public long getTableNumRowsHint() {
    return tableNumRowsHint_;
  }

  /**
   * Returns true if this table ref has a resolved path that is rooted at a registered
   * tuple descriptor, false otherwise.
   */
  public boolean isRelative() { return false; }

  public boolean isCollectionInSelectList() { return false; }

  /**
   * Indicates if this TableRef directly or indirectly references another TableRef from
   * an outer query block.
   */
  public boolean isCorrelated() { return !correlatedTupleIds_.isEmpty(); }

  public List<String> getPath() { return rawPath_; }
  public Path getResolvedPath() { return resolvedPath_; }

  /**
   * Returns all legal aliases of this table ref.
   */
  public String[] getAliases() { return aliases_; }

  /**
   * Returns the explicit alias or the fully-qualified implicit alias. The returned alias
   * is guaranteed to be unique (i.e., column/field references against the alias cannot
   * be ambiguous).
   */
  public String getUniqueAlias() { return aliases_[0]; }

  /**
   * Returns true if this table ref has an explicit alias.
   * Note that getAliases().length() == 1 does not imply an explicit alias because
   * nested collection refs have only a single implicit alias.
   */
  public boolean hasExplicitAlias() { return hasExplicitAlias_; }

  /**
   * Returns the explicit alias if this table ref has one, null otherwise.
   */
  public String getExplicitAlias() {
    if (hasExplicitAlias()) return getUniqueAlias();
    return null;
  }

  public FeTable getTable() {
    Preconditions.checkNotNull(resolvedPath_);
    return resolvedPath_.getRootTable();
  }
  public TableSampleClause getSampleParams() { return sampleParams_; }
  public TimeTravelSpec getTimeTravelSpec() { return timeTravelSpec_; }
  public Privilege getPrivilege() { return priv_; }
  public boolean requireGrantOption() { return requireGrantOption_; }
  public List<PlanHint> getJoinHints() { return joinHints_; }
  public List<PlanHint> getTableHints() { return tableHints_; }
  public Expr getOnClause() { return onClause_; }
  public void setJoinOp(JoinOperator op) { this.joinOp_ = op; }
  public void setOnClause(Expr e) { this.onClause_ = e; }
  public void setUsingClause(List<String> colNames) { this.usingColNames_ = colNames; }
  public TableRef getLeftTblRef() { return leftTblRef_; }
  public void setLeftTblRef(TableRef leftTblRef) { this.leftTblRef_ = leftTblRef; }
  public void setExposeNestedColumnsByTableMaskView() {
    exposeNestedColumnsByTableMaskView_ = true;
  }
  public boolean exposeNestedColumnsByTableMaskView() {
    return exposeNestedColumnsByTableMaskView_;
  }

  public void setHidden(boolean isHidden) { isHidden_ = isHidden; }
  public boolean isHidden() { return isHidden_; }

  public void setJoinHints(List<PlanHint> hints) {
    Preconditions.checkNotNull(hints);
    joinHints_ = hints;
  }

  public void setTableHints(List<PlanHint> hints) {
    Preconditions.checkNotNull(hints);
    tableHints_ = hints;
  }

  public void setTableSampleClause(TableSampleClause sampleParams) {
    sampleParams_ = sampleParams;
  }

  public boolean isBroadcastJoin() { return distrMode_ == DistributionMode.BROADCAST; }

  public boolean isPartitionedJoin() {
    return distrMode_ == DistributionMode.PARTITIONED;
  }

  public DistributionMode getDistributionMode() { return distrMode_; }
  public List<TupleId> getCorrelatedTupleIds() { return correlatedTupleIds_; }
  public boolean isAnalyzed() { return isAnalyzed_; }
  public boolean isResolved() { return !getClass().equals(TableRef.class); }

  public boolean isFromClauseZippingUnnest() {
    return zippingUnnestType_ == ZippingUnnestType.FROM_CLAUSE_ZIPPING_UNNEST;
  }
  public boolean isZippingUnnest() {
    return zippingUnnestType_ != ZippingUnnestType.NONE;
  }
  public ZippingUnnestType getZippingUnnestType() { return zippingUnnestType_; }
  public void setZippingUnnestType(ZippingUnnestType t) { zippingUnnestType_ = t; }


  /**
   * This method should only be called after the TableRef has been analyzed.
   */
  public TupleDescriptor getDesc() {
    Preconditions.checkState(isAnalyzed_);
    // after analyze(), desc should be set.
    Preconditions.checkState(desc_ != null);
    return desc_;
  }

  /**
   * This method should only be called after the TableRef has been analyzed.
   */
  public TupleId getId() {
    Preconditions.checkState(isAnalyzed_);
    // after analyze(), desc should be set.
    Preconditions.checkNotNull(desc_);
    return desc_.getId();
  }

  public List<TupleId> getMaterializedTupleIds() {
    // This function should only be called after analyze().
    Preconditions.checkState(isAnalyzed_);
    Preconditions.checkNotNull(desc_);
    return desc_.getId().asList();
  }

  /**
   * Returns the list of tuple ids materialized by the full sequence of
   * table refs up to and including this one.
   */
  public List<TupleId> getAllMaterializedTupleIds() {
    Preconditions.checkState(isAnalyzed_);
    return allMaterializedTupleIds_;
  }

  /**
   * Return the list of table ref ids of the full sequence of table refs up to
   * and including this one.
   */
  public List<TupleId> getAllTableRefIds() {
    Preconditions.checkState(isAnalyzed_);
    return allTableRefIds_;
  }

  protected void analyzeTableSample(Analyzer analyzer) throws AnalysisException {
    if (sampleParams_ == null) return;
    sampleParams_.analyze(analyzer);
    if (!(this instanceof BaseTableRef)
        || !(resolvedPath_.destTable() instanceof FeFsTable)) {
      throw new AnalysisException(
          "TABLESAMPLE is only supported on HDFS tables: " + getUniqueAlias());
    }
  }

  protected void analyzeTimeTravel(Analyzer analyzer) throws AnalysisException {
    // We are analyzing the time travel spec before we know the table type, so we
    // cannot check if the table supports time travel.
    if (timeTravelSpec_ != null) {
      timeTravelSpec_.analyze(analyzer);
    }
  }

  protected void analyzeHints(Analyzer analyzer) throws AnalysisException {
    // We prefer adding warnings over throwing exceptions here to maintain view
    // compatibility with Hive.
    Preconditions.checkState(isResolved());
    analyzeTableHints(analyzer);
    analyzeJoinHints(analyzer);
  }

  private void analyzeTableHints(Analyzer analyzer) {
    if (tableHints_.isEmpty()) return;
    if (!(this instanceof BaseTableRef)) {
      analyzer.addWarning("Table hints not supported for inline view and collections");
      return;
    }
    // BaseTableRef will always have their path resolved at this point.
    Preconditions.checkState(getResolvedPath() != null);
    boolean isTableHintSupported = true;
    if (getResolvedPath().destTable() != null &&
        !supportTableHint(getResolvedPath().destTable(), analyzer)) {
      isTableHintSupported = false;
    }
    for (PlanHint hint: tableHints_) {
      if (hint.is("SCHEDULE_CACHE_LOCAL")) {
        analyzer.setHasPlanHints();
        replicaPreference_ = TReplicaPreference.CACHE_LOCAL;
      } else if (hint.is("SCHEDULE_DISK_LOCAL")) {
        analyzer.setHasPlanHints();
        replicaPreference_ = TReplicaPreference.DISK_LOCAL;
      } else if (hint.is("SCHEDULE_REMOTE")) {
        analyzer.setHasPlanHints();
        replicaPreference_ = TReplicaPreference.REMOTE;
      } else if (hint.is("SCHEDULE_RANDOM_REPLICA")) {
        analyzer.setHasPlanHints();
        randomReplica_ = true;
      } else if (hint.is("CONVERT_LIMIT_TO_SAMPLE")) {
        List<String> args = hint.getArgs();
        if (args == null || args.size() != 1) {
          addHintWarning(hint, analyzer);
          return;
        } else {
          try {
            int samplePercent = Integer.parseInt(args.get(0));
            if (samplePercent < 1 || samplePercent > 100) {
              addHintWarning(hint, analyzer);
              return;
            }
            analyzer.setHasPlanHints();
            convertLimitToSampleHintPercent_ = samplePercent;
          } catch (NumberFormatException e) {
            addHintWarning(hint, analyzer);
          }
        }
      } else if (hint.is(TABLE_ROW_HINT)) {
        List<String> args = hint.getArgs();
        if (args == null || args.size() != 1 || !isTableHintSupported) {
          addHintWarning(hint, analyzer);
          return;
        }
        analyzer.setHasPlanHints();
        tableNumRowsHint_ = Long.parseLong(args.get(0));
      } else {
        addHintWarning(hint, analyzer);
      }
    }
  }

  /**
   * Returns whether the table supports hint. Currently, hdfs table support table hint
   * usage, kudu table only support {@link this#TABLE_ROW_HINT} hint.
   *
   * TODO: Add other table hints for kudu table.
   */
  private boolean supportTableHint(FeTable table, Analyzer analyzer) {
    if (table instanceof FeKuduTable) {
      if (!(tableHints_.size() == 1 && tableHints_.get(0).is(TABLE_ROW_HINT))) {
        analyzer.addWarning(String.format("Kudu table only support '%s' hint.",
            TABLE_ROW_HINT));
        return false;
      }
    } else if (!(table instanceof FeFsTable)) {
      analyzer.addWarning("Table hints only supported for Hdfs/Kudu tables.");
      return false;
    }
    return true;
  }

  private void addHintWarning(PlanHint hint, Analyzer analyzer) {
    Preconditions.checkState(getAliases() != null && getAliases().length > 0);
    analyzer.addWarning("Table hint not recognized for table " + getUniqueAlias() +
        ": " + hint);
  }

  private void analyzeJoinHints(Analyzer analyzer) throws AnalysisException {
    if (joinHints_.isEmpty()) return;
    for (PlanHint hint: joinHints_) {
      if (hint.is("BROADCAST")) {
        if (joinOp_ == JoinOperator.RIGHT_OUTER_JOIN
            || joinOp_ == JoinOperator.FULL_OUTER_JOIN
            || joinOp_ == JoinOperator.RIGHT_SEMI_JOIN
            || joinOp_ == JoinOperator.RIGHT_ANTI_JOIN) {
          throw new AnalysisException(
              joinOp_.toString() + " does not support BROADCAST.");
        }
        if (isPartitionedJoin()) {
          throw new AnalysisException("Conflicting JOIN hint: " + hint);
        }
        distrMode_ = DistributionMode.BROADCAST;
        analyzer.setHasPlanHints();
      } else if (hint.is("SHUFFLE")) {
        if (joinOp_ == JoinOperator.CROSS_JOIN) {
          throw new AnalysisException("CROSS JOIN does not support SHUFFLE.");
        }
        if (isBroadcastJoin()) {
          throw new AnalysisException("Conflicting JOIN hint: " + hint);
        }
        distrMode_ = DistributionMode.PARTITIONED;
        analyzer.setHasPlanHints();
      } else {
        analyzer.addWarning("JOIN hint not recognized: " + hint);
      }
    }
  }

  /**
   * Analyzes the join clause. Populates allTableRefIds_ and allMaterializedTupleIds_.
   * The join clause can only be analyzed after the left table has been analyzed
   * and the TupleDescriptor (desc) of this table has been created.
   */
  public void analyzeJoin(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkState(leftTblRef_ == null || leftTblRef_.isAnalyzed_);
    Preconditions.checkState(desc_ != null);

    // Populate the lists of all table ref and materialized tuple ids.
    allTableRefIds_.clear();
    allMaterializedTupleIds_.clear();
    if (leftTblRef_ != null) {
      allTableRefIds_.addAll(leftTblRef_.getAllTableRefIds());
      allMaterializedTupleIds_.addAll(leftTblRef_.getAllMaterializedTupleIds());
    }
    allTableRefIds_.add(getId());
    allMaterializedTupleIds_.addAll(getMaterializedTupleIds());

    if (joinOp_ == JoinOperator.CROSS_JOIN) {
      // A CROSS JOIN is always a broadcast join, regardless of the join hints
      distrMode_ = DistributionMode.BROADCAST;
    }

    if (usingColNames_ != null) {
      Preconditions.checkState(joinOp_ != JoinOperator.CROSS_JOIN);
      // Turn USING clause into equivalent ON clause.
      onClause_ = null;
      for (String colName: usingColNames_) {
        // check whether colName exists both for our table and the one
        // to the left of us
        Path leftColPath = new Path(leftTblRef_.getDesc(),
            Lists.newArrayList(colName.toLowerCase()));
        if (!leftColPath.resolve()) {
          throw new AnalysisException(
              "unknown column " + colName + " for alias "
              + leftTblRef_.getUniqueAlias() + " (in \"" + this.toSql() + "\")");
        }
        Path rightColPath = new Path(desc_,
            Lists.newArrayList(colName.toLowerCase()));
        if (!rightColPath.resolve()) {
          throw new AnalysisException(
              "unknown column " + colName + " for alias "
              + getUniqueAlias() + " (in \"" + this.toSql() + "\")");
        }

        // create predicate "<left>.colName = <right>.colName"
        BinaryPredicate eqPred =
            new BinaryPredicate(BinaryPredicate.Operator.EQ,
              new SlotRef(Path.createRawPath(leftTblRef_.getUniqueAlias(), colName)),
              new SlotRef(Path.createRawPath(getUniqueAlias(), colName)));
        onClause_ = CompoundPredicate.createConjunction(eqPred, onClause_);
      }
    }

    // at this point, both 'this' and leftTblRef have been analyzed and registered;
    // register the tuple ids of the TableRefs on the nullable side of an outer join
    if (joinOp_ == JoinOperator.LEFT_OUTER_JOIN
        || joinOp_ == JoinOperator.FULL_OUTER_JOIN) {
      analyzer.registerOuterJoinedTids(getId().asList(), this);
    }
    if (joinOp_ == JoinOperator.RIGHT_OUTER_JOIN
        || joinOp_ == JoinOperator.FULL_OUTER_JOIN) {
      analyzer.registerOuterJoinedTids(leftTblRef_.getAllTableRefIds(), this);
    }
    // register the tuple ids of a full outer join
    if (joinOp_ == JoinOperator.FULL_OUTER_JOIN) {
      analyzer.registerFullOuterJoinedTids(leftTblRef_.getAllTableRefIds(), this);
      analyzer.registerFullOuterJoinedTids(getId().asList(), this);
    }
    // register the tuple id of the rhs of a left semi join
    TupleId semiJoinedTupleId = null;
    if (joinOp_ == JoinOperator.LEFT_SEMI_JOIN
        || joinOp_ == JoinOperator.LEFT_ANTI_JOIN
        || joinOp_ == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) {
      analyzer.registerSemiJoinedTid(getId(), this);
      semiJoinedTupleId = getId();
    }
    // register the tuple id of the lhs of a right semi join
    if (joinOp_ == JoinOperator.RIGHT_SEMI_JOIN
        || joinOp_ == JoinOperator.RIGHT_ANTI_JOIN) {
      analyzer.registerSemiJoinedTid(leftTblRef_.getId(), this);
      semiJoinedTupleId = leftTblRef_.getId();
    }

    if (onClause_ != null) {
      Preconditions.checkState(joinOp_ != JoinOperator.CROSS_JOIN);
      analyzer.setVisibleSemiJoinedTuple(semiJoinedTupleId);
      onClause_.analyze(analyzer);
      analyzer.setVisibleSemiJoinedTuple(null);
      onClause_.checkReturnsBool("ON clause", true);
        if (onClause_.contains(Expr.IS_AGGREGATE)) {
          throw new AnalysisException(
              "aggregate function not allowed in ON clause: " + toSql());
      }
      if (onClause_.contains(AnalyticExpr.class)) {
        throw new AnalysisException(
            "analytic expression not allowed in ON clause: " + toSql());
      }
      if (onClause_.contains(Subquery.class)) {
        throw new AnalysisException(
            "Subquery is not allowed in ON clause: " + toSql());
      }
      Set<TupleId> onClauseTupleIds = new HashSet<>();
      List<Expr> conjuncts = onClause_.getConjuncts();
      // Outer join clause conjuncts are registered for this particular table ref
      // (ie, can only be evaluated by the plan node that implements this join).
      // The exception are conjuncts that only pertain to the nullable side
      // of the outer join; those can be evaluated directly when materializing tuples
      // without violating outer join semantics.
      analyzer.registerOnClauseConjuncts(conjuncts, this);
      for (Expr e: conjuncts) {
        List<TupleId> tupleIds = new ArrayList<>();
        e.getIds(tupleIds, null);
        onClauseTupleIds.addAll(tupleIds);
      }
    } else if (!allowEmptyOn() && !isRelative() && !isCorrelated()
        && (getJoinOp().isOuterJoin() || getJoinOp().isSemiJoin())) {
      throw new AnalysisException(
          joinOp_.toString() + " requires an ON or USING clause.");
    } else {
      // Indicate that this table ref has an empty ON-clause.
      analyzer.registerOnClauseConjuncts(Collections.<Expr>emptyList(), this);
    }
  }

  public void rewriteExprs(ExprRewriter rewriter, Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkState(isAnalyzed_);
    if (onClause_ != null) onClause_ = rewriter.rewrite(onClause_, analyzer);
  }

  public String debugString() { return tableRefToSql(); }

  protected String tableRefToSql() { return tableRefToSql(DEFAULT); }

  protected String tableRefToSql(ToSqlOptions options) {
    String aliasSql = null;
    String alias = getExplicitAlias();
    if (alias != null) aliasSql = ToSqlUtils.getIdentSql(alias);
    List<String> path = rawPath_;
    if (resolvedPath_ != null) path = resolvedPath_.getFullyQualifiedRawPath();
    return ToSqlUtils.getPathSql(path) + ((aliasSql != null) ? " " + aliasSql : "");
  }

  @Override
  public final String toSql() {
    return toSql(DEFAULT);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    if (isZippingUnnest()) return "";
    if (joinOp_ == null) {
      // prepend "," if we're part of a sequence of table refs w/o an
      // explicit JOIN clause
      return (leftTblRef_ != null ? ", " : "") + tableRefToSql(options);
    }

    StringBuilder output = new StringBuilder(" " + joinOp_.toString() + " ");
    if (!joinHints_.isEmpty())
      output.append(ToSqlUtils.getPlanHintsSql(options, joinHints_)).append(" ");
    output.append(tableRefToSql(options));
    if (usingColNames_ != null) {
      output.append(" USING (").append(Joiner.on(", ").join(usingColNames_)).append(")");
    } else if (onClause_ != null) {
      output.append(" ON ").append(onClause_.toSql(options));
    }
    return output.toString();
  }

  /**
   * Returns a deep clone of this table ref without also cloning the chain of table refs.
   * Sets leftTblRef_ in the returned clone to null.
   */
  @Override
  protected TableRef clone() { return new TableRef(this); }

  public void reset() {
    isAnalyzed_ = false;
    resolvedPath_ = null;
    if (usingColNames_ != null) {
      // The using col names are converted into an on-clause predicate during analysis,
      // so unset the on-clause here.
      onClause_ = null;
    } else if (onClause_ != null) {
      onClause_.reset();
    }
    leftTblRef_ = null;
    allTableRefIds_.clear();
    allMaterializedTupleIds_.clear();
    correlatedTupleIds_.clear();
    desc_ = null;
    if (timeTravelSpec_ != null) timeTravelSpec_.reset();
  }

  public boolean isTableMaskingView() { return false; }

  public void registerColumn(Column column) {
    columns_.put(column.getName(), column);
  }

  /**
   * @return an unmodifiable list of all columns, but with partition columns at the end of
   * the list rather than the beginning. This is equivalent to the order in which Hive
   * enumerates columns.
   */
  public List<Column> getColumnsInHiveOrder() {
    return getTable().getColumnsInHiveOrder();
  }

  public List<Column> getSelectedColumnsInHiveOrder() {
    // Map from column name to the Column object (null if not selected).
    // Use LinkedHashMap to preserve the order.
    Map<String, Column> colSelection = new LinkedHashMap<>();
    for (Column c : getColumnsInHiveOrder()) {
      colSelection.put(c.getName(), null);
    }
    // Update 'colSelection' with selected columns. Virtual columns will also be added.
    for (String colName : columns_.keySet()) {
      colSelection.put(colName, columns_.get(colName));
    }
    List<Column> res = new ArrayList<>();
    for (Column c : colSelection.values()) {
      if (c != null) res.add(c);
    }
    // Make sure not missing any columns
    Preconditions.checkState(res.size() == columns_.size(),
        "missing columns: " + res.size() + " != " + columns_.size());
    return res;
  }

  void migratePropertiesTo(TableRef other) {
    other.aliases_ = aliases_;
    other.onClause_ = onClause_;
    other.usingColNames_ = usingColNames_;
    other.joinOp_ = joinOp_;
    other.joinHints_ = joinHints_;
    other.tableHints_ = tableHints_;
    other.timeTravelSpec_ = timeTravelSpec_;
    // Clear properties. Don't clear aliases_ since it's still used in resolving slots
    // in the query block of 'other'.
    // Don't clear timeTravelSpec_ as it is still relevant.
    onClause_ = null;
    usingColNames_ = null;
    joinOp_ = null;
    joinHints_ = new ArrayList<>();
    tableHints_ = new ArrayList<>();
  }
}