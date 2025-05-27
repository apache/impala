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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationContext;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.PrivilegeRequestBuilder;
import org.apache.impala.authorization.TableMask;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDataSourceTable;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeIncompleteTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeSystemTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.IcebergTimeTravelTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.MaterializedViewHdfsTable;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.catalog.VirtualColumn;
import org.apache.impala.catalog.VirtualTable;
import org.apache.impala.catalog.iceberg.IcebergMetadataTable;
import org.apache.impala.catalog.local.LocalKuduTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.IdGenerator;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.planner.JoinNode;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.rewrite.BetweenToCompoundRule;
import org.apache.impala.rewrite.CountStarToConstRule;
import org.apache.impala.rewrite.SimplifyCastExprRule;
import org.apache.impala.rewrite.ConvertToCNFRule;
import org.apache.impala.rewrite.EqualityDisjunctsToInRule;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.rewrite.ExtractCommonConjunctRule;
import org.apache.impala.rewrite.ExtractCompoundVerticalBarExprRule;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.apache.impala.rewrite.NormalizeBinaryPredicatesRule;
import org.apache.impala.rewrite.NormalizeCountStarRule;
import org.apache.impala.rewrite.NormalizeExprsRule;
import org.apache.impala.rewrite.SimplifyCastStringToTimestamp;
import org.apache.impala.rewrite.SimplifyConditionalsRule;
import org.apache.impala.rewrite.SimplifyDistinctFromRule;
import org.apache.impala.rewrite.CountDistinctToNdvRule;
import org.apache.impala.rewrite.DefaultNdvScaleRule;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.QueryConstants;
import org.apache.impala.thrift.TAccessEvent;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TImpalaQueryOptions;
import org.apache.impala.thrift.TLineageGraph;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.DisjointSet;
import org.apache.impala.util.ExecutorMembershipSnapshot;
import org.apache.impala.util.Graph.RandomAccessibleGraph;
import org.apache.impala.util.Graph.SccCondensedGraph;
import org.apache.impala.util.Graph.WritableGraph;
import org.apache.impala.util.IntIterator;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.impala.util.TSessionStateUtil;
import org.apache.kudu.client.KuduClient;
import org.github.jamm.CannotAccessFieldException;
import org.github.jamm.MemoryMeter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Repository of analysis state for single select block.
 *
 * Conjuncts:
 * Conjuncts are registered during analysis (registerConjuncts()) and assigned during the
 * planning process (getUnassigned[Oj]Conjuncts()/isConjunctAssigned()/
 * markConjunctsAssigned()).
 * All conjuncts are assigned a unique id when initially registered, and all registered
 * conjuncts are referenced by their id (ie, there are no containers other than the one
 * holding the referenced conjuncts), to make substitute() simple.
 *
 * Slot value transfers:
 * Slot A has a value transfer to slot B if a predicate on A can be applied to B at some
 * point in the plan. Determining the lowest correct placement of that predicate is
 * subject to the conventional assignment rules.
 * Each slot is contained in exactly one equivalence class. A slot equivalence class is a
 * set of slots where each pair of slots has a mutual value transfer. Equivalence classes
 * are assigned an arbitrary id to distinguish them from another.
 *
 * Implied mutual value transfers are registered with createAuxEqPredicate(); they are
 * never assigned during plan generation.
 * Also tracks each catalog object access, so authorization checks can be performed once
 * analysis is complete.
 * TODO: We often use the terms stmt/block/analyzer interchangeably, although they may
 * have slightly different meanings (sometimes depending on the context). Use the terms
 * more accurately and consistently here and elsewhere.
 */
public class Analyzer {
  public static final byte ACCESSTYPE_READ = (byte)2;
  public static final byte ACCESSTYPE_WRITE = (byte)4;
  public static final byte ACCESSTYPE_READWRITE = (byte)8;

  // Common analysis error messages
  public final static String DB_DOES_NOT_EXIST_ERROR_MSG = "Database does not exist: ";
  public final static String DB_ALREADY_EXISTS_ERROR_MSG = "Database already exists: ";
  public final static String TBL_DOES_NOT_EXIST_ERROR_MSG = "Table does not exist: ";
  public final static String TBL_ALREADY_EXISTS_ERROR_MSG = "Table already exists: ";
  public final static String VIEW_ALREADY_EXISTS_ERROR_MSG = "View already exists: ";
  public final static String FN_DOES_NOT_EXIST_ERROR_MSG = "Function does not exist: ";
  public final static String FN_ALREADY_EXISTS_ERROR_MSG = "Function already exists: ";
  public final static String DATA_SRC_DOES_NOT_EXIST_ERROR_MSG =
      "Data source does not exist: ";
  public final static String DATA_SRC_ALREADY_EXISTS_ERROR_MSG =
      "Data source already exists: ";
  private static final String TRANSACTIONAL_TABLE_NOT_SUPPORTED =
      "%s not supported on transactional (ACID) table: %s";
  private static final String FULL_TRANSACTIONAL_TABLE_NOT_SUPPORTED =
      "%s not supported on full transactional (ACID) table: %s";
  private static final String BUCKETED_TABLE_NOT_SUPPORTED =
      "%s is a bucketed table. Only read operations are supported on such tables.";
  private static final String TABLE_NOT_SUPPORTED =
      "%s not supported. Table %s  access type is: %s";

  private final static Logger LOG = LoggerFactory.getLogger(Analyzer.class);

  private final User user_;

  // Indicates whether this query block contains a straight join hint.
  private boolean isStraightJoin_ = false;

  // Whether to use Hive's auto-generated column labels.
  private boolean useHiveColLabels_ = false;

  // True if the corresponding select block has a limit and/or offset clause.
  private boolean hasLimitOffsetClause_ = false;

  // Current depth of nested analyze() calls. Used for enforcing a
  // maximum expr-tree depth. Needs to be manually maintained by the user
  // of this Analyzer with incrementCallDepth() and decrementCallDepth().
  private int callDepth_ = 0;

  // Flag indicating if this analyzer instance belongs to a subquery.
  private boolean isSubquery_ = false;

  // Tracks the simple LIMIT status of this query block. First item of the
  // pair indicates whether a simple limit exists or not, second item is
  // the actual limit value if it does exist. We use a pair instead of just
  // a single nullable field because a query block may not have a LIMIT but
  // if it is an inline view it may be eligible for limit pushdown from an
  // outer query, so for such case the simpleLimitStatus_ will be non-null
  // but the pair will be <false, null>.
  private Pair<Boolean, Long>  simpleLimitStatus_ = null;

  // Flag indicating whether this analyzer belongs to a WITH clause view.
  private boolean hasWithClause_ = false;

  // If set, when masked privilege requests are registered they will use this error
  // error message.
  private String authErrorMsg_;

  // If true privilege requests are added in maskedPrivileReqs_. Otherwise, privilege
  // requests are added to privilegeReqs_.
  private boolean maskPrivChecks_ = false;

  // If false, privilege requests are not registered.
  private boolean enablePrivChecks_ = true;

  // By default, all registered semi-joined tuples are invisible, i.e., their slots
  // cannot be referenced. If set, this semi-joined tuple is made visible. Such a tuple
  // should only be made visible for analyzing the On-clause of its semi-join.
  // In particular, if there are multiple semi-joins in the same query block, then the
  // On-clause of any such semi-join is not allowed to reference other semi-joined tuples
  // except its own. Therefore, only a single semi-joined tuple can be visible at a time.
  private TupleId visibleSemiJoinedTupleId_ = null;

  // During analysis if a materialized view table ref is encountered, we check
  // authorization on its source tables and set the field below to non-null if
  // if an exception was encountered.
  private String mvAuthExceptionMsg_ = null;

  // Total records num V1 is calculated by all DataFiles of the Iceberg V1 table.
  private long totalRecordsNumV1_;

  // Total records num V2 is calculated by all DataFiles without corresponding DeleteFiles
  // to be applied of the Iceberg V2 table.
  private long totalRecordsNumV2_;

  // The method 'registerSlotRef()' is called recursively for complex types (structs and
  // collections). When creating new 'SlotDescriptor's it is important that they are
  // inserted into the correct 'TupleDescriptor'. This stack is used to keep track of the
  // current (most nested) 'TupleDescriptor' at each step.
  //
  // When a new top-level path is registered, the root descriptor of the path is pushed on
  // the stack. When registering a complex type, we push its item tuple desc on the stack
  // before analysing the children, and pop it afterwards. New 'SlotDescriptor's are
  // always added to the 'TupleDescriptor' at the top of the stack.
  //
  // To ensure that every push operation has a corresponding pop operation, these should
  // not be called manually. Instead, use 'TupleStackGuard' objects in try-with-resources
  // blocks; see more in its documentation.
  private Deque<TupleDescriptor> tupleStack_ = new ArrayDeque<>();

  // When created, pushes the provided TupleDescriptor to the enclosing Analyzer object's
  // 'tupleStack_' and pops it in the close() method. Implements the AutoCloseable
  // interface so it can be used in try-with-resources blocks.
  private class TupleStackGuard implements AutoCloseable {
    private final TupleDescriptor tupleDesc_;
    private boolean isClosed_ = false;
    private final int stackLen_;

    public TupleStackGuard(TupleDescriptor tupleDesc) {
      Preconditions.checkNotNull(tupleStack_);
      Preconditions.checkNotNull(tupleDesc);
      tupleDesc_ = tupleDesc;
      stackLen_ = tupleStack_.size();
      tupleStack_.push(tupleDesc_);
    }

    @Override
    public void close() {
      if (!isClosed_) {
        Preconditions.checkState(tupleStack_.peek() == tupleDesc_);
        tupleStack_.pop();
        Preconditions.checkState(tupleStack_.size() == stackLen_);
        isClosed_ = true;
      }
    }
  }

  // Required Operation type: Read, write, any(read or write).
  public enum OperationType {
    READ,
    WRITE,
    ANY
  };

  public void setIsSubquery() {
    isSubquery_ = true;
    globalState_.containsSubquery = true;
  }

  public void setSimpleLimitStatus(Pair<Boolean, Long> simpleLimitStatus) {
    simpleLimitStatus_ = simpleLimitStatus;
  }

  public Pair<Boolean, Long> getSimpleLimitStatus() {
    return simpleLimitStatus_;
  }

  public void setHasTopLevelAcidCollectionTableRef() {
    globalState_.hasTopLevelAcidCollectionTableRef = true;
  }

  public boolean hasTopLevelAcidCollectionTableRef() {
    return globalState_.hasTopLevelAcidCollectionTableRef;
  }

  public boolean setHasPlanHints() { return globalState_.hasPlanHints = true; }
  public boolean hasPlanHints() { return globalState_.hasPlanHints; }
  public void setHasWithClause() { hasWithClause_ = true; }
  public boolean hasWithClause() { return hasWithClause_; }
  public void setSetOpNeedsRewrite() { globalState_.setOperationNeedsRewrite = true; }

  /**
   * @param table Table whose properties need to be checked.
   * @param operationStr The unsupported operation.
   * @throws AnalysisException If table is full acid table.
   */
  public static void ensureTableNotFullAcid(FeTable table, String operationStr)
      throws AnalysisException {
    if (AcidUtils.isFullAcidTable(table.getMetaStoreTable().getParameters())) {
      throw new AnalysisException(String.format(FULL_TRANSACTIONAL_TABLE_NOT_SUPPORTED,
          operationStr, table.getFullName()));
    }
  }

  public static void ensureTableNotTransactional(FeTable table, String operationStr)
      throws AnalysisException {
    if (AcidUtils.isTransactionalTable(table.getMetaStoreTable().getParameters())) {
      throw new AnalysisException(String.format(TRANSACTIONAL_TABLE_NOT_SUPPORTED,
          operationStr, table.getFullName()));
    }
  }

  /**
   * @param table Table need to be checked
   * @throws AnalysisException If table is a bucketed table.
   */
  public static void ensureTableNotBucketed(FeTable table)
      throws AnalysisException {
    if (MetaStoreUtil.isBucketedTable(table.getMetaStoreTable())) {
      throw new AnalysisException(String.format(BUCKETED_TABLE_NOT_SUPPORTED,
              table.getFullName()));
    }
  }

  /**
   * Check if the table supports the operation
   * @param table Table need to check
   * @param type The type of operation
   * @throws AnalysisException If the table does not support the operation
   */
  public static void checkTableCapability(FeTable table, OperationType type)
      throws AnalysisException {
    switch(type) {
      case WRITE:
        ensureTableWriteSupported(table);
        break;
      case READ:
      case ANY:
      default:
        ensureTableSupported(table);
        break;
    }
  }

  /**
   * Check if the table supports write operations
   * @param table Table need to check
   * @throws AnalysisException If the table does not support write.
   */
  private static void ensureTableWriteSupported(FeTable table)
      throws AnalysisException {
    ensureTableNotBucketed(table);
    if (MetastoreShim.getMajorVersion() > 2) {
      byte writeRequires = ACCESSTYPE_WRITE | ACCESSTYPE_READWRITE;
      // Kudu tables do not put new table properties to HMS and HMS need
      // OBJCAPABILIES to grant managed unacid table write permission
      // TODO: remove following kudu check when these issues are fixed
      if (KuduTable.isKuduTable(table.getMetaStoreTable())) return;
      if (!MetastoreShim.hasTableCapability(table.getMetaStoreTable(), writeRequires)) {
        // Error messages with explanations.
        throw new AnalysisException(String.format(TABLE_NOT_SUPPORTED, "Write",
            table.getFullName(),
            MetastoreShim.getTableAccessType(table.getMetaStoreTable())));
      }
    } else {
      ensureTableNotTransactional(table, "Write");
    }
  }

  /**
   * Check if the table type is supported
   * @param table Table need to check capabilities
   * @throws AnalysisException if the table type is not supported.
   */
  private static void ensureTableSupported(FeTable table)
      throws AnalysisException {
    if (MetastoreShim.getMajorVersion() > 2) {
      byte capabilities = ACCESSTYPE_READ | ACCESSTYPE_WRITE | ACCESSTYPE_READWRITE;
      if (!MetastoreShim.hasTableCapability(table.getMetaStoreTable(), capabilities)) {
        // Return error messages by table type checking
        // TODO: After Hive provides API calls to send back hints on why
        // the operations are not supported, we will generate error messages
        // accordingly.
        throw new AnalysisException(String.format(TABLE_NOT_SUPPORTED, "Operations",
            table.getFullName(),
            MetastoreShim.getTableAccessType(table.getMetaStoreTable())));
      }
    } else {
      ensureTableNotTransactional(table, "Operation");
    }
  }

  // State shared between all objects of an Analyzer tree. We use LinkedHashMap and
  // LinkedHashSet where applicable to preserve the iteration order and make the class
  // behave identical across different implementations of the JVM.
  // TODO: Many maps here contain properties about tuples, e.g., whether
  // a tuple is outer/semi joined, etc. Remove the maps in favor of making
  // them properties of the tuple descriptor itself.
  private static class GlobalState {
    public final TQueryCtx queryCtx;
    public final AuthorizationFactory authzFactory;
    public final AuthorizationContext authzCtx;
    public final DescriptorTable descTbl = new DescriptorTable();
    public final IdGenerator<ExprId> conjunctIdGenerator = ExprId.createGenerator();
    public final ColumnLineageGraph lineageGraph;

    // True if we are analyzing an explain request. Should be set before starting
    // analysis.
    public boolean isExplain;

    // Indicates whether the query has plan hints.
    public boolean hasPlanHints = false;

    // True if at least one of the analyzers belongs to a subquery.
    public boolean containsSubquery = false;

    // True if one of the analyzers belongs to a set operand of type EXCEPT or INTERSECT
    // which needs to be rewritten using joins.
    public boolean setOperationNeedsRewrite = false;

    // True when the query directly scans collection items, e.g.:
    // select item from complextypestbl.int_array;
    public boolean hasTopLevelAcidCollectionTableRef = false;

    // True when a SystemTableScanNode is present.
    public boolean includeAllCoordinatorsInScheduling = false;

    // all registered conjuncts (map from expr id to conjunct). We use a LinkedHashMap to
    // preserve the order in which conjuncts are added.
    public final Map<ExprId, Expr> conjuncts = new LinkedHashMap<>();

    // all registered conjuncts without auxiliary predicate. This additional map is used
    // for performance reasons of getUnassignedConjuncts(List<TupleId> tupleIds,
    // boolean inclOjConjuncts).
    public final Map<ExprId, Expr> conjunctsFromQuery = new LinkedHashMap<>();

    // all registered inferred conjuncts (map from tuple id to conjuncts). This map is
    // used to make sure that slot equivalences are not enforced multiple times (e.g.
    // duplicated to previously inferred conjuncts).
    public final Map<TupleId, List<BinaryPredicate>> assignedConjunctsByTupleId =
        new HashMap<>();

    // all registered conjuncts bound by a single tuple id; used in getBoundPredicates()
    public final List<ExprId> singleTidConjuncts = new ArrayList<>();

    // eqJoinConjuncts[tid] contains all conjuncts of the form
    // "<lhs> = <rhs>" in which either lhs or rhs is fully bound by tid
    // and the other side is not bound by tid (ie, predicates that express equi-join
    // conditions between two tablerefs).
    // A predicate such as "t1.a = t2.b" has two entries, one for 't1' and
    // another one for 't2'.
    public final Map<TupleId, List<ExprId>> eqJoinConjuncts = new HashMap<>();

    // set of conjuncts that have been assigned to some PlanNode
    public Set<ExprId> assignedConjuncts =
        Collections.newSetFromMap(new IdentityHashMap<ExprId, Boolean>());

    // map from outer-joined tuple id, i.e., one that is nullable,
    // to the last Join clause (represented by its rhs table ref) that outer-joined it
    public final Map<TupleId, TableRef> outerJoinedTupleIds = new HashMap<>();

    // Map of registered conjunct to the last full outer join (represented by its
    // rhs table ref) that outer joined it.
    public final Map<ExprId, TableRef> fullOuterJoinedConjuncts = new HashMap<>();

    // Map of full-outer-joined tuple id to the last full outer join that outer-joined it
    public final Map<TupleId, TableRef> fullOuterJoinedTupleIds = new HashMap<>();

    // Map from semi-joined tuple id, i.e., one that is invisible outside the join's
    // On-clause, to its Join clause (represented by its rhs table ref). An anti-join is
    // a kind of semi-join, so anti-joined tuples are also registered here.
    public final Map<TupleId, TableRef> semiJoinedTupleIds = new HashMap<>();

    // Map from right-hand side table-ref id of an outer join to the list of
    // conjuncts in its On clause. There is always an entry for an outer join, but the
    // corresponding value could be an empty list. There is no entry for non-outer joins.
    public final Map<TupleId, List<ExprId>> conjunctsByOjClause = new HashMap<>();

    // map from registered conjunct to its containing outer join On clause (represented
    // by its right-hand side table ref); this is limited to conjuncts that can only be
    // correctly evaluated by the originating outer join, including constant conjuncts
    public final Map<ExprId, TableRef> ojClauseByConjunct = new HashMap<>();

    // map from registered conjunct to its containing semi join On clause (represented
    // by its right-hand side table ref)
    public final Map<ExprId, TableRef> sjClauseByConjunct = new HashMap<>();

    // map from registered conjunct to its containing inner join On clause (represented
    // by its right-hand side table ref)
    public final Map<ExprId, TableRef> ijClauseByConjunct = new HashMap<>();

    // map from slot id to the analyzer/block in which it was registered
    public final Map<SlotId, Analyzer> blockBySlot = new HashMap<>();

    // Tracks all privilege requests on catalog objects.
    private final Set<PrivilegeRequest> privilegeReqs = new LinkedHashSet<>();

    // List of PrivilegeRequest to custom authorization failure error message.
    // Tracks all privilege requests on catalog objects that need a custom
    // error message returned to avoid exposing existence of catalog objects.
    private final List<Pair<PrivilegeRequest, String>> maskedPrivilegeReqs =
        new ArrayList<>();

    // accesses to catalog objects
    // TODO: This can be inferred from privilegeReqs. They should be coalesced.
    public Set<TAccessEvent> accessEvents = new HashSet<>();

    // Tracks all warnings (e.g. non-fatal errors) that were generated during analysis.
    // These are passed to the backend and eventually propagated to the shell. Maps from
    // warning message to the number of times that warning was logged (in order to avoid
    // duplicating the same warning over and over).
    public final Map<String, Integer> warnings = new LinkedHashMap<>();

    // Tracks whether the warnings have been retrieved from this analyzer. If set to true,
    // adding new warnings will result in an error. This helps to make sure that no
    // warnings are added which will not be displayed.
    public boolean warningsRetrieved = false;

    // The SCC-condensed graph representation of all slot value transfers.
    private SccCondensedGraph valueTransferGraph;

    private final List<Pair<SlotId, SlotId>> registeredValueTransfers =
        new ArrayList<>();

    // Bidirectional map between Integer index and TNetworkAddress.
    // Decreases the size of the scan range locations.
    private final ListMap<TNetworkAddress> hostIndex = new ListMap<>();

    // Cache of statement-relevant table metadata populated before analysis.
    private final StmtTableCache stmtTableCache;

    // Expr rewriter for folding constants.
    private final ExprRewriter constantFolder_ =
        new ExprRewriter(FoldConstantsRule.INSTANCE);

    // Expr rewriter for normalizing and rewriting expressions.
    private final ExprRewriter exprRewriter_;

    // Null slots cache - for expressions with slots replaced by nulls - to reduce
    // backend expression evaluation when query contains many similar expressions.
    private final Cache<Expr, Boolean> nullSlotsCache;

    // Total number of expressions across the statement (including all subqueries). This
    // is used to enforce a limit on the total number of expressions. Incremented by
    // incrementNumStmtExprs(). Note that this does not include expressions that do not
    // require analysis (e.g. some literal expressions).
    private int numStmtExprs_ = 0;

    // When set to a value > -1, it represents the number of executors for current
    // iteration of compilation. Used by auto scaling to pass the info to the planner in
    // place of ExecutorMembershipSnapshot.numExecutors(). ExecutorMembershipSnapshot is a
    // singleton.
    private int numExecutorsForPlanning_ = -1;

    // Number of available cores per executor node.
    // Set by Frontend.java.
    private int availableCoresPerNode_ = -1;

    // Cache of KuduTables opened for this query. (map from table name to kudu table)
    // This cache prevent multiple openTable calls for a given table in the same query.
    public final Map<String, org.apache.kudu.client.KuduTable> kuduTables =
        new HashMap<>();

    // This holds the nullable side slot ids from the outer join's equi-join conjuncts
    // e.g. t1 left join t2 on t1.id = t2.id, the slot id of t2.id will be added to
    // this set.
    public Set<SlotId> ojNullableSlotsInEquiPreds = new HashSet<>();

    // This holds the tuple id's of the arrays that are given as a zipping unnest table
    // ref. If the table ref is originated from a view then also add the tuple IDs for the
    // respective table refs from the view.
    public Set<TupleId> zippingUnnestTupleIds = new HashSet<>();

    // Shows how many zipping unnests were in the query;
    public int numZippingUnnests = 0;

    // Ids of (collection-typed) SlotDescriptors that were registered with
    // 'duplicateIfCollections=true' in 'Analyzer.registerSlotRef()'.
    public Set<SlotId> duplicateCollectionSlots = new HashSet<>();

    public GlobalState(StmtTableCache stmtTableCache, TQueryCtx queryCtx,
        AuthorizationFactory authzFactory, AuthorizationContext authzCtx) {
      this.stmtTableCache = stmtTableCache;
      this.queryCtx = queryCtx;
      this.authzCtx = authzCtx;
      this.authzFactory = authzFactory;
      this.lineageGraph = new ColumnLineageGraph();
      List<ExprRewriteRule> rules = new ArrayList<>();
      // BetweenPredicates must be rewritten to be executable. Other non-essential
      // expr rewrites can be disabled via a query option. When rewrites are enabled
      // BetweenPredicates should be rewritten first to help trigger other rules.
      rules.add(BetweenToCompoundRule.INSTANCE);
      // Binary predicates must be rewritten to a canonical form for both Kudu predicate
      // pushdown and Parquet row group pruning based on min/max statistics.
      rules.add(NormalizeBinaryPredicatesRule.INSTANCE);
      rules.add(ExtractCompoundVerticalBarExprRule.INSTANCE);
      if (queryCtx.getClient_request().getQuery_options().enable_expr_rewrites) {
        rules.add(FoldConstantsRule.INSTANCE);
        rules.add(NormalizeExprsRule.INSTANCE);
        rules.add(ExtractCommonConjunctRule.INSTANCE);
        if (queryCtx.getClient_request().getQuery_options().isEnable_cnf_rewrites()) {
          rules.add(new ConvertToCNFRule(queryCtx.getClient_request().getQuery_options()
              .getMax_cnf_exprs(),true));
        }
        // Relies on FoldConstantsRule and NormalizeExprsRule.
        rules.add(SimplifyConditionalsRule.INSTANCE);
        rules.add(EqualityDisjunctsToInRule.INSTANCE);
        rules.add(NormalizeCountStarRule.INSTANCE);
        rules.add(SimplifyDistinctFromRule.INSTANCE);
        rules.add(SimplifyCastStringToTimestamp.INSTANCE);
        rules.add(CountDistinctToNdvRule.INSTANCE);
        rules.add(DefaultNdvScaleRule.INSTANCE);
        rules.add(SimplifyCastExprRule.INSTANCE);
      }
      rules.add(CountStarToConstRule.INSTANCE);
      exprRewriter_ = new ExprRewriter(rules);
      nullSlotsCache =
          queryCtx.getClient_request().getQuery_options().use_null_slots_cache ?
          CacheBuilder.newBuilder().concurrencyLevel(1).recordStats().build() : null;
    }
  };

  private final GlobalState globalState_;

  public boolean containsSubquery() { return globalState_.containsSubquery; }
  public boolean containsSetOperation() { return globalState_.setOperationNeedsRewrite; }

  public int numExecutorsForPlanning() {
    return (globalState_.numExecutorsForPlanning_ >= 0) ?
        globalState_.numExecutorsForPlanning_ :
        ExecutorMembershipSnapshot.getCluster().numExecutors();
  }
  public void setNumExecutorsForPlanning(int x) {
    globalState_.numExecutorsForPlanning_ = x;
  }

  public int getAvailableCoresPerNode() {
    Preconditions.checkState(globalState_.availableCoresPerNode_ > 0);
    return globalState_.availableCoresPerNode_;
  }

  public void setAvailableCoresPerNode(int x) {
    Preconditions.checkArgument(x > 0);
    globalState_.availableCoresPerNode_ =
        Math.min(QueryConstants.MAX_FRAGMENT_INSTANCES_PER_NODE, x);
  }

  public int getMinParallelismPerNode() {
    if (getQueryOptions().isCompute_processing_cost()) {
      return getQueryOptions().getProcessing_cost_min_threads();
    } else {
      return 1;
    }
  }

  public int getMaxParallelismPerNode() {
    if (getQueryOptions().isCompute_processing_cost()) {
      return Math.max(getMinParallelismPerNode(),
          Math.min(getQueryOptions().getMax_fragment_instances_per_node(),
              getAvailableCoresPerNode()));
    } else if (getQueryOptions().getMt_dop() > 0) {
      return getQueryOptions().getMt_dop();
    } else {
      return 1;
    }
  }

  public boolean includeAllCoordinatorsInScheduling() {
    return globalState_.includeAllCoordinatorsInScheduling;
  }

  public void setIncludeAllCoordinatorsInScheduling(boolean flag) {
    globalState_.includeAllCoordinatorsInScheduling = flag;
  }

  // An analyzer stores analysis state for a single select block. A select block can be
  // a top level select statement, or an inline view select block.
  // ancestors contains the Analyzers of the enclosing select blocks of 'this'
  // (ancestors[0] contains the immediate parent, etc.).
  private final List<Analyzer> ancestors_;

  // map from lowercase table alias to a view definition in this analyzer's scope
  private final Map<String, FeView> localViews_ = new HashMap<>();

  // Map from lowercase table alias to descriptor. Tables without an explicit alias
  // are assigned two implicit aliases: the unqualified and fully-qualified table name.
  // Such tables have two entries pointing to the same descriptor. If an alias is
  // ambiguous, then this map retains the first entry with that alias to simplify error
  // checking (duplicate vs. ambiguous alias).
  private final Map<String, TupleDescriptor> aliasMap_ = new HashMap<>();

  // Map from tuple id to its corresponding table ref.
  private final Map<TupleId, TableRef> tableRefMap_ = new HashMap<>();

  // This is populated by UnnestExpr during analysis. Each UnnestExpr creates a
  // CollectionTableRef and adds it to this set. Later on SelectStmt will add these table
  // refs to the table refs in the FROM clause.
  private final Set<CollectionTableRef> tableRefsFromUnnestExpr_ = new HashSet<>();

  // Set of lowercase ambiguous implicit table aliases.
  private final Set<String> ambiguousAliases_ = new HashSet<>();

  // Map from lowercase fully-qualified path to its slot descriptor. Only contains paths
  // that have a scalar or struct type as destination (see registerSlotRef()).
  private final Map<List<String>, SlotDescriptor> slotPathMap_ = new HashMap<>();

  // Indicates whether this analyzer/block is guaranteed to have an empty result set
  // due to a limit 0 or constant conjunct evaluating to false.
  private boolean hasEmptyResultSet_ = false;

  // Indicates whether the select-project-join (spj) portion of this query block
  // is guaranteed to return an empty result set. Set due to a constant non-Having
  // conjunct evaluating to false.
  private boolean hasEmptySpjResultSet_ = false;

  public Analyzer(StmtTableCache stmtTableCache, TQueryCtx queryCtx,
      AuthorizationFactory authzFactory, AuthorizationContext authzCtx) {
    ancestors_ = new ArrayList<>();
    globalState_ = new GlobalState(stmtTableCache, queryCtx, authzFactory, authzCtx);
    user_ = new User(TSessionStateUtil.getEffectiveUser(queryCtx.session));
  }

  /**
   * Analyzer constructor for nested select block. GlobalState is inherited from the
   * parentAnalyzer.
   */
  public Analyzer(Analyzer parentAnalyzer) {
    this(parentAnalyzer, parentAnalyzer.globalState_);
  }

  /**
   * Analyzer constructor for nested select block with the specified global state.
   */
  private Analyzer(Analyzer parentAnalyzer, GlobalState globalState) {
    ancestors_ = Lists.newArrayList(parentAnalyzer);
    ancestors_.addAll(parentAnalyzer.ancestors_);
    globalState_ = globalState;
    user_ = parentAnalyzer.getUser();
    useHiveColLabels_ = parentAnalyzer.useHiveColLabels_;
    authErrorMsg_ = parentAnalyzer.authErrorMsg_;
    maskPrivChecks_ = parentAnalyzer.maskPrivChecks_;
    enablePrivChecks_ = parentAnalyzer.enablePrivChecks_;
    hasWithClause_ = parentAnalyzer.hasWithClause_;
  }

  /**
   * Returns a new analyzer with the specified parent analyzer but with a new
   * global state.
   */
  public static Analyzer createWithNewGlobalState(Analyzer parentAnalyzer) {
    GlobalState globalState = new GlobalState(parentAnalyzer.globalState_.stmtTableCache,
        parentAnalyzer.getQueryCtx(), parentAnalyzer.getAuthzFactory(),
        parentAnalyzer.getAuthzCtx());
    return new Analyzer(parentAnalyzer, globalState);
  }

  /**
   * Makes the given semi-joined tuple visible such that its slots can be referenced.
   * If tid is null, makes the currently visible semi-joined tuple invisible again.
   */
  public void setVisibleSemiJoinedTuple(TupleId tid) {
    Preconditions.checkState(tid == null
        || globalState_.semiJoinedTupleIds.containsKey(tid));
    Preconditions.checkState(tid == null || visibleSemiJoinedTupleId_ == null);
    visibleSemiJoinedTupleId_ = tid;
  }

  public boolean hasAncestors() { return !ancestors_.isEmpty(); }
  public Analyzer getParentAnalyzer() {
    return hasAncestors() ? ancestors_.get(0) : null;
  }

  /**
   * Returns the analyzer that has an entry for the given tuple descriptor in its
   * tableRefMap, or null if no such analyzer could be found. Searches the hierarchy
   * of analyzers bottom-up.
   */
  public Analyzer findAnalyzer(TupleId tid) {
    if (tableRefMap_.containsKey(tid)) return this;
    if (hasAncestors()) return getParentAnalyzer().findAnalyzer(tid);
    return null;
  }

  /**
   * Returns a list of each warning logged, indicating if it was logged more than once.
   * After this function has been called, no warning may be added to the Analyzer anymore.
   */
  public List<String> getWarnings() {
    globalState_.warningsRetrieved = true;
    List<String> result = new ArrayList<>();
    for (Map.Entry<String, Integer> e : globalState_.warnings.entrySet()) {
      String error = e.getKey();
      int count = e.getValue();
      Preconditions.checkState(count > 0);
      if (count == 1) {
        result.add(error);
      } else {
        result.add(error + " (" + count + " warnings like this)");
      }
    }
    return result;
  }

  /**
   * Registers a local view definition with this analyzer. Throws an exception if a view
   * definition with the same alias has already been registered or if the number of
   * explicit column labels is greater than the number of columns in the view statement.
   */
  public void registerLocalView(FeView view) throws AnalysisException {
    Preconditions.checkState(view.isLocalView());
    if (view.getColLabels() != null) {
      List<String> viewLabels = view.getColLabels();
      List<String> queryStmtLabels = view.getQueryStmt().getColLabels();
      if (viewLabels.size() > queryStmtLabels.size()) {
        throw new AnalysisException("WITH-clause view '" + view.getName() +
            "' returns " + queryStmtLabels.size() + " columns, but " +
            viewLabels.size() + " labels were specified. The number of column " +
            "labels must be smaller or equal to the number of returned columns.");
      }
    }
    if (localViews_.put(view.getName().toLowerCase(), view) != null) {
      throw new AnalysisException(
          String.format("Duplicate table alias: '%s'", view.getName()));
    }
  }

  /**
   * Checks if a table ref has already been registered in this analyzer and returns it.
   * Uses the unique alias from the table ref for the check. Returns null if the table
   * ref has not been registered.
   */
  public TableRef getRegisteredTableRef(String uniqueAlias) {
    if (uniqueAlias == null) return null;
    TupleDescriptor tupleDesc = aliasMap_.get(uniqueAlias);
    if (tupleDesc == null) return null;
    return tableRefMap_.get(tupleDesc.getId());
  }

  /**
   * Creates an returns an empty TupleDescriptor for the given table ref and registers
   * it against all its legal aliases. For tables refs with an explicit alias, only the
   * explicit alias is legal. For tables refs with no explicit alias, the fully-qualified
   * and unqualified table names are legal aliases. Column references against unqualified
   * implicit aliases can be ambiguous, therefore, we register such ambiguous aliases
   * here. Requires that all views have been substituted.
   * Throws if an existing explicit alias or implicit fully-qualified alias
   * has already been registered for another table ref.
   */
  public TupleDescriptor registerTableRef(TableRef ref) throws AnalysisException {
    String uniqueAlias = ref.getUniqueAlias();
    if (aliasMap_.containsKey(uniqueAlias)) {
      throw new AnalysisException("Duplicate table alias: '" + uniqueAlias + "'");
    }

    // If ref has no explicit alias, then the unqualified and the fully-qualified table
    // names are legal implicit aliases. Column references against unqualified implicit
    // aliases can be ambiguous, therefore, we register such ambiguous aliases here.
    String unqualifiedAlias = null;
    String[] aliases = ref.getAliases();
    if (aliases.length > 1) {
      unqualifiedAlias = aliases[1];
      TupleDescriptor tupleDesc = aliasMap_.get(unqualifiedAlias);
      if (tupleDesc != null) {
        if (tupleDesc.hasExplicitAlias()) {
          throw new AnalysisException(
              "Duplicate table alias: '" + unqualifiedAlias + "'");
        } else {
          ambiguousAliases_.add(unqualifiedAlias);
        }
      }
    }

    // Delegate creation of the tuple descriptor to the concrete table ref.
    TupleDescriptor result = ref.createTupleDescriptor(this);
    result.setAliases(aliases, ref.hasExplicitAlias());
    // Register all legal aliases.
    for (String alias: aliases) {
      aliasMap_.put(alias, result);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Register aliases {} to tuple {}", aliases, result.debugString());
    }
    tableRefMap_.put(result.getId(), ref);
    return result;
  }

  // Registers a CollectionTableRef with given alias and tuple descriptor.
  // Used to register collections that come from a view and are used in the from clause.
  public void addCollectionTableRef(
      String alias, CollectionTableRef ref, TupleDescriptor desc) {
    aliasMap_.put(alias, desc);
    tableRefMap_.put(desc.getId(), ref);
  }

  public void addAlias(String alias, TupleDescriptor desc) {
    aliasMap_.put(alias, desc);
  }

  /**
   * Resolves the given TableRef into a concrete BaseTableRef, ViewRef or
   * CollectionTableRef. Returns the new resolved table ref or the given table
   * ref if it is already resolved.
   * Registers privilege requests and throws an AnalysisException if the tableRef's
   * path could not be resolved. The privilege requests are added to ensure that
   * an AuthorizationException is preferred over an AnalysisException so as not to
   * accidentally reveal the non-existence of tables/databases.
   */
  public TableRef resolveTableRef(TableRef tableRef)
      throws AnalysisException {
    // Return the table if it is already resolved. This also avoids the table being
    // masked again.
    if (tableRef.isResolved()) return tableRef;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Resolving TableRef {}", ToSqlUtils.getPathSql(tableRef.getPath()));
    }
    // Try to find a matching local view.
    if (tableRef.getPath().size() == 1) {
      // Searches the hierarchy of analyzers bottom-up for a registered local view with
      // a matching alias.
      String viewAlias = tableRef.getPath().get(0).toLowerCase();
      Analyzer analyzer = this;
      do {
        FeView localView = analyzer.localViews_.get(viewAlias);
        if (localView != null) return new InlineViewRef(localView, tableRef);
        analyzer = (analyzer.ancestors_.isEmpty() ? null : analyzer.ancestors_.get(0));
      } while (analyzer != null);
    }

    // Resolve the table ref's path and determine what resolved table ref
    // to replace it with.
    List<String> rawPath = tableRef.getPath();
    Path resolvedPath = null;
    try {
      resolvedPath =
          resolvePathWithMasking(rawPath, PathType.TABLE_REF, tableRef.timeTravelSpec_);
    } catch (AnalysisException e) {
      // Register privilege requests to prefer reporting an authorization error over
      // an analysis error. We should not accidentally reveal the non-existence of a
      // table/database if the user is not authorized.
      registerPrivReqOnRawPath(tableRef, rawPath);
      throw e;
    } catch (TableLoadingException e) {
      throw new AnalysisException(String.format(
          "Failed to load metadata for table: '%s'", Joiner.on(".").join(rawPath)), e);
    }

    Preconditions.checkNotNull(resolvedPath);
    if (resolvedPath.destTable() != null) {
      FeTable table = resolvedPath.destTable();
      if (table instanceof FeView) return new InlineViewRef((FeView) table, tableRef);
      if (table instanceof IcebergMetadataTable) {
        return new IcebergMetadataTableRef(tableRef, resolvedPath);
      }
      if (table instanceof FeSystemTable) {
        return new SystemTableRef(tableRef, resolvedPath);
      }
      // The table must be a base table.
      Preconditions.checkState(table instanceof FeFsTable ||
          table instanceof FeKuduTable ||
          table instanceof FeHBaseTable ||
          table instanceof FeDataSourceTable);
      return new BaseTableRef(tableRef, resolvedPath);
    } else {
      return new CollectionTableRef(tableRef, resolvedPath, false);
    }
  }

  /**
   * Register privilege requests based on the 'tableRawPath'. Only used when we fail to
   * resolve the TableRef. With these requests we can prefer reporting an authorization
   * error over an analysis error. See more in resolveTableRef().
   */
  private void registerPrivReqOnRawPath(TableRef tableRef, List<String> tableRawPath) {
    if (tableRawPath.size() > 1) {
      registerPrivReq(builder -> {
        builder.onTableUnknownOwner(
            tableRawPath.get(0), tableRawPath.get(1)).allOf(tableRef.getPrivilege());
        if (tableRef.requireGrantOption()) {
          builder.grantOption();
        }
        return builder.build();
      });
    } else {
      registerPrivReq(builder -> {
        builder.onTableUnknownOwner(
            getDefaultDb(), tableRawPath.get(0)).allOf(tableRef.getPrivilege());
        if (tableRef.requireGrantOption()) {
          builder.grantOption();
        }
        return builder.build();
      });
    }
  }

  /**
   * Resolves column-masking/row-filtering policies on the given table. Returns a table
   * masking view if any of these policies exist. The TableRef should be resolved first
   * so we know the target table/view/collection.
   *
   * @param resolvedTableRef A resolved TableRef for table masking
   */
  public TableRef resolveTableMask(TableRef resolvedTableRef) throws AnalysisException {
    Preconditions.checkState(resolvedTableRef.isResolved(), "Table should be resolved");
    // Only do table masking when authorization is enabled and the authorization
    // factory supports column-masking/row-filtering. If both of these are false,
    // return the unmasked table ref.
    if (!getAuthzFactory().getAuthorizationConfig().isEnabled()
        || !getAuthzFactory().supportsTableMasking()) {
      return resolvedTableRef;
    }
    // Performing table masking.
    AuthorizationChecker authChecker = getAuthzFactory().newAuthorizationChecker(
        getCatalog().getAuthPolicy());
    String dbName;
    String tblName;
    if (resolvedTableRef instanceof InlineViewRef) {
      FeView view = ((InlineViewRef) resolvedTableRef).getView();
      dbName = view.getDb().getName();
      tblName = view.getName();
    } else if (resolvedTableRef instanceof CollectionTableRef
        && resolvedTableRef.isRelative()) {
       // Relative table refs don't need masking. Its base table will be masked.
       return resolvedTableRef;
    } else {
      dbName = resolvedTableRef.getTable().getDb().getName();
      tblName = resolvedTableRef.getTable().getName();
    }
    // The selected columns should be in the same relative order as they are in the
    // corresponding Hive table so that the order of the SelectListItem's in the
    // table mask view (if needs masking or filtering) would be correct.
    List<Column> columns = resolvedTableRef.getSelectedColumnsInHiveOrder();
    TableMask tableMask = new TableMask(authChecker, dbName, tblName, columns, user_);
    try {
      if (resolvedTableRef instanceof CollectionTableRef) {
        if (tableMask.needsRowFiltering()) {
          // The table ref is a non-relative CollectionTableRef, e.g. the table ref in
          // "select item from functional_parquet.complextypestbl.int_array". We can't
          // replace "complextypestbl" with a table masking view here.
          // TODO: Support this in IMPALA-10484 by rewriting it to relative ref, e.g.
          //  select a.item from functional_parquet.complextypestbl t, t.int_array a;
          throw new AnalysisException(String.format("Using non-relative collection " +
              "column %s of table %s.%s is not supported since there are row-filtering " +
              "policies on this table (IMPALA-10484). Rewrite query to use relative " +
              "reference.",
              String.join(".", resolvedTableRef.getResolvedPath().getRawPath()),
              dbName, tblName));
        }
      } else if (!(resolvedTableRef instanceof InlineViewRef) &&
          resolvedTableRef.getTable() instanceof MaterializedViewHdfsTable &&
          ((MaterializedViewHdfsTable) resolvedTableRef.getTable())
            .isReferencesMaskedTables(authChecker, getCatalog(), getUser())) {
        // If a materialized view definition references tables that have table masking
        // policies defined, we set a flag indicating that this is an authorization
        // exception instead of throwing an AnalysisException here. Later, during the
        // AnalysisContext.analyzeAndAuthorize() we throw the AuthorizationException.
        mvAuthExceptionMsg_ = String.format("Materialized view %s.%s " +
            "references tables with column masking or " +
            "row filtering policies.", dbName, tblName);
      } else if (tableMask.needsMaskingOrFiltering()) {
        return InlineViewRef.createTableMaskView(resolvedTableRef, tableMask,
            getAuthzCtx());
      }
      return resolvedTableRef;
    } catch (InternalException e) {
      String msg = "Error resolving table mask on " + dbName + "." + tblName;
      LOG.error(msg, e);
      throw new AnalysisException(msg, e);
    }
  }

  public boolean encounteredMVAuthException() {
    return mvAuthExceptionMsg_ != null;
  }

  public String getMVAuthExceptionMsg() {
    return mvAuthExceptionMsg_;
  }

  public void setTotalRecordsNumV1(long totalRecordsNumV1) {
    totalRecordsNumV1_ = totalRecordsNumV1;
  }

  public long getTotalRecordsNumV1() { return totalRecordsNumV1_; }

  public void setTotalRecordsNumV2(long totalRecordsNumV2) {
    totalRecordsNumV2_ = totalRecordsNumV2;
  }

  public long getTotalRecordsNumV2() {
    return totalRecordsNumV2_;
  }

  /**
   * Check if 'count(*)' FunctionCallExpr can be rewritten as LiteralExpr. When
   * totalRecordsNum_ is 0, no optimization 'count(*)' is still very fast, so return true
   * only if totalRecordsNum_ is greater than 0.
   */
  public boolean canRewriteCountStarForV1() { return totalRecordsNumV1_ > 0; }

  public boolean canRewriteCountStartForV2() { return totalRecordsNumV2_ > 0; }

  /**
   * Register conjuncts that are outer joined by a full outer join. For a given
   * predicate, we record the last full outer join that outer-joined any of its
   * tuple ids. We need this additional information because full-outer joins obey
   * different rules with respect to predicate pushdown compared to left and right
   * outer joins.
   */
  public void registerFullOuterJoinedConjunct(Expr e) {
    Preconditions.checkState(
        !globalState_.fullOuterJoinedConjuncts.containsKey(e.getId()));
    List<TupleId> tids = new ArrayList<>();
    e.getIds(tids, null);
    for (TupleId tid: tids) {
      if (!globalState_.fullOuterJoinedTupleIds.containsKey(tid)) continue;
      TableRef currentOuterJoin = globalState_.fullOuterJoinedTupleIds.get(tid);
      globalState_.fullOuterJoinedConjuncts.put(e.getId(), currentOuterJoin);
      break;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("registerFullOuterJoinedConjunct: " +
          globalState_.fullOuterJoinedConjuncts.toString());
    }
  }

  /**
   * Register tids as being outer-joined by a full outer join clause represented by
   * rhsRef.
   */
  public void registerFullOuterJoinedTids(List<TupleId> tids, TableRef rhsRef) {
    for (TupleId tid: tids) {
      globalState_.fullOuterJoinedTupleIds.put(tid, rhsRef);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("registerFullOuterJoinedTids: " +
          globalState_.fullOuterJoinedTupleIds.toString());
    }
  }

  /**
   * Register tids as being outer-joined by Join clause represented by rhsRef.
   */
  public void registerOuterJoinedTids(List<TupleId> tids, TableRef rhsRef) {
    for (TupleId tid: tids) {
      globalState_.outerJoinedTupleIds.put(tid, rhsRef);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("registerOuterJoinedTids: " +
          globalState_.outerJoinedTupleIds.toString());
    }
  }

  /**
   * Register the given tuple id as being the invisible side of a semi-join.
   */
  public void registerSemiJoinedTid(TupleId tid, TableRef rhsRef) {
    globalState_.semiJoinedTupleIds.put(tid, rhsRef);
  }

  public void addZippingUnnestTupleId(CollectionTableRef tblRef) {
    Expr collExpr = tblRef.getCollectionExpr();
    addZippingUnnestTupleId(collExpr);
  }

  public void addZippingUnnestTupleId(Expr collExpr) {
    if (collExpr == null) return;
    if (!(collExpr instanceof SlotRef)) return;
    SlotRef slotCollExpr = (SlotRef)collExpr;
    SlotDescriptor collSlotDesc = slotCollExpr.getDesc();
    Preconditions.checkNotNull(collSlotDesc);
    TupleDescriptor collTupleDesc = collSlotDesc.getItemTupleDesc();
    Preconditions.checkNotNull(collTupleDesc);
    globalState_.zippingUnnestTupleIds.add(collTupleDesc.getId());
  }

  public void addZippingUnnestTupleId(TupleId tid) {
    globalState_.zippingUnnestTupleIds.add(tid);
  }

  public Set<TupleId> getZippingUnnestTupleIds() {
    return globalState_.zippingUnnestTupleIds;
  }

  public void increaseZippingUnnestCount() {
    ++globalState_.numZippingUnnests;
  }

  public int getNumZippingUnnests() {
    return globalState_.numZippingUnnests;
  }

  /**
   * Returns the descriptor of the given explicit or implicit table alias or null if no
   * such alias has been registered.
   * Throws an AnalysisException if the given table alias is ambiguous.
   */
  public TupleDescriptor getDescriptor(String tableAlias) throws AnalysisException {
    String lookupAlias = tableAlias.toLowerCase();
    if (ambiguousAliases_.contains(lookupAlias)) {
      throw new AnalysisException(String.format(
          "Unqualified table alias is ambiguous: '%s'", tableAlias));
    }
    return aliasMap_.get(lookupAlias);
  }

  public TupleDescriptor getTupleDesc(TupleId id) {
    return globalState_.descTbl.getTupleDesc(id);
  }

  public SlotDescriptor getSlotDesc(SlotId id) {
    return globalState_.descTbl.getSlotDesc(id);
  }

  /**
   * Helper to get all slot descriptors in list.
   */
  public List<SlotDescriptor> getSlotDescs(List<SlotId> ids) {
    List<SlotDescriptor> result = new ArrayList<>(ids.size());
    for (SlotId id : ids) {
      result.add(getSlotDesc(id));
    }
    return result;
  }

  public int getNumTableRefs() { return tableRefMap_.size(); }
  public TableRef getTableRef(TupleId tid) { return tableRefMap_.get(tid); }
  public Map<TupleId, TableRef> getTableRefs() { return tableRefMap_; }
  public ExprRewriter getConstantFolder() { return globalState_.constantFolder_; }
  public ExprRewriter getExprRewriter() { return globalState_.exprRewriter_; }

  public Set<CollectionTableRef> getTableRefsFromUnnestExpr() {
    return tableRefsFromUnnestExpr_;
  }
  public void addTableRefFromUnnestExpr(CollectionTableRef ref) {
    tableRefsFromUnnestExpr_.add(ref);
  }

  /**
   * Given a list of {"table alias", "column alias"}, return the SlotDescriptor.
   */
  public SlotDescriptor getSlotDescriptor(List<String> qualifiedColumnName) {
    return slotPathMap_.get(qualifiedColumnName);
  }

  /**
   * Return true if this analyzer has no ancestors. (i.e. false for the analyzer created
   * for inline views/ set operands (except/intersect/union), etc.)
   */
  public boolean isRootAnalyzer() { return ancestors_.isEmpty(); }

  /**
   * Returns true if the query block corresponding to this analyzer is guaranteed
   * to return an empty result set, e.g., due to a limit 0 or a constant predicate
   * that evaluates to false.
   */
  public boolean hasEmptyResultSet() { return hasEmptyResultSet_; }
  public void setHasEmptyResultSet() { hasEmptyResultSet_ = true; }

  /**
   * Returns true if the select-project-join portion of this query block returns
   * an empty result set.
   */
  public boolean hasEmptySpjResultSet() { return hasEmptySpjResultSet_; }

  /**
   * Resolves 'rawPath' according to the given path type. Deal with paths that got
   * resolved to nested columns of table masking views.
   */
  public Path resolvePathWithMasking(List<String> rawPath, PathType pathType)
      throws AnalysisException, TableLoadingException {
    return this.resolvePathWithMasking(rawPath, pathType, null);
  }

  public Path resolvePathWithMasking(List<String> rawPath, PathType pathType,
      TimeTravelSpec timeTravelSpec) throws AnalysisException, TableLoadingException {
    Path resolvedPath = resolvePath(rawPath, pathType, timeTravelSpec);
    // Skip normal resolution cases that don't relate to nested types.
    if (pathType == PathType.TABLE_REF) {
      if (resolvedPath.destTable() != null || !resolvedPath.isRootedAtTuple()) {
        return resolvedPath;
      }
    } else if (pathType == PathType.SLOT_REF) {
      if (!resolvedPath.getMatchedTypes().get(0).isStructType()) {
        return resolvedPath;
      }
    } else if (pathType == PathType.STAR) {
      // For "v.*", return directly if "v" is a table/view.
      // If it's a struct column, we need to further resolve it if it's now resolved on a
      // table masking view.
      if (resolvedPath.getMatchedTypes().isEmpty()) return resolvedPath;
      Preconditions.checkState(resolvedPath.destType().isStructType());
    }
    // In this case, resolvedPath is resolved on a nested column. Check if it's resolved
    // on a table masking view. The root TableRef(table/view) could be at a parent query
    // block (correlated case, e.g. "t.int_array" in query
    // "SELECT ... FROM tbl t, (SELECT * FROM t.int_array) a" roots at "tbl t" which is
    // in the parent block), so we should find the parent block first then we can find
    // the root TableRef.
    TupleId rootTupleId = resolvedPath.getRootDesc().getId();
    Analyzer parentAnalyzer = findAnalyzer(rootTupleId);
    TableRef rootTblRef = parentAnalyzer.getTableRef(rootTupleId);
    Preconditions.checkNotNull(rootTblRef);
    if (!rootTblRef.isTableMaskingView()) return resolvedPath;
    // resolvedPath is resolved on a nested column of a table masking view. The view
    // won't produce results of nested columns. It just exposes the nested columns of the
    // underlying BaseTableRef in the fields of its output type. (See more in
    // InlineViewRef#createTupleDescriptor()). We need to resolve 'rawPath' inside the
    // view as if the underlying table is not masked. So the resolved path can point to
    // the real table and be used to create materialized slot in the TupleDescriptor of
    // the real table.
    InlineViewRef tableMaskingView = (InlineViewRef) rootTblRef;
    Preconditions.checkState(
        tableMaskingView.getUnMaskedTableRef() instanceof BaseTableRef);
    // Resolve rawPath inside the table masking view to point to the real table.
    Path maskedPath = tableMaskingView.inlineViewAnalyzer_.resolvePath(
        rawPath, pathType, timeTravelSpec);
    maskedPath.setPathBeforeMasking(resolvedPath);
    return maskedPath;
  }

  /**
   * Resolves the given raw path according to the given path type, as follows:
   * SLOT_REF and STAR: Resolves the path in the context of all registered tuple
   * descriptors, considering qualified as well as unqualified matches.
   * TABLE_REF: Resolves the path in the context of all registered tuple descriptors
   * only considering qualified matches, as well as catalog tables/views.
   *
   * Path resolution:
   * Regardless of the path type, a raw path can have multiple successful resolutions.
   * A resolution is said to be 'successful' if all raw path elements can be mapped
   * to a corresponding alias/table/column/field.
   *
   * Path legality:
   * A successful resolution may be illegal with respect to the path type, e.g.,
   * a SlotRef cannot reference intermediate collection types, etc.
   *
   * Path ambiguity:
   * A raw path is ambiguous if it has multiple legal resolutions. Otherwise,
   * the ambiguity is resolved in favor of the legal resolution.
   *
   * Returns the single legal path resolution if it exists.
   * Throws if there was no legal resolution or if the path is ambiguous.
   */
  public Path resolvePath(List<String> rawPath, PathType pathType)
      throws AnalysisException, TableLoadingException {
    return this.resolvePath(rawPath, pathType, null);
  }

  public Path resolvePath(List<String> rawPath, PathType pathType,
      TimeTravelSpec timeTravelSpec) throws AnalysisException, TableLoadingException {
    // We only allow correlated references in predicates of a subquery.
    boolean resolveInAncestors = false;
    if (pathType == PathType.TABLE_REF || pathType == PathType.ANY) {
      resolveInAncestors = true;
    } else if (pathType == PathType.SLOT_REF) {
      resolveInAncestors = isSubquery_;
    }
    // Convert all path elements to lower case.
    List<String> lcRawPath = Lists.newArrayListWithCapacity(rawPath.size());
    for (String s: rawPath) lcRawPath.add(s.toLowerCase());
    return resolvePath(lcRawPath, pathType, resolveInAncestors, timeTravelSpec);
  }

  private Path resolvePath(List<String> rawPath, PathType pathType,
      boolean resolveInAncestors, TimeTravelSpec timeTravelSpec)
      throws AnalysisException, TableLoadingException {
    // List of all candidate paths with different roots. Paths in this list are initially
    // unresolved and may be illegal with respect to the pathType.
    List<Path> candidates = getTupleDescPaths(rawPath);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Candidates for {} {}: {}", pathType, ToSqlUtils.getPathSql(rawPath),
          candidates);
    }
    LinkedList<String> errors = Lists.newLinkedList();
    if (pathType == PathType.SLOT_REF || pathType == PathType.STAR) {
      // Paths rooted at all of the unique registered tuple descriptors.
      for (TableRef tblRef: tableRefMap_.values()) {
        if (tblRef.isHidden()) continue;
        candidates.add(new Path(tblRef.getDesc(), rawPath));
      }
    } else {
      // Always prefer table ref paths rooted at a registered tuples descriptor.
      Preconditions.checkState(pathType == PathType.TABLE_REF ||
          pathType == PathType.ANY);
      Path result = resolvePaths(rawPath, candidates, pathType, errors);
      if (result != null) return result;
      candidates.clear();

      // Add paths rooted at a table with an unqualified and fully-qualified table name.
      List<TableName> candidateTbls = Path.getCandidateTables(rawPath, getDefaultDb());
      for (int tblNameIdx = 0; tblNameIdx < candidateTbls.size(); ++tblNameIdx) {
        TableName tblName = candidateTbls.get(tblNameIdx);
        FeTable tbl = null;
        try {
          tbl = getTable(tblName, /* must_exist */ false);
        } catch (AnalysisException e) {
          // Ignore to allow path resolution to continue.
        }
        if (tbl != null) {
          if (timeTravelSpec != null) {
            if (!(tbl instanceof FeIcebergTable)) {
              throw new AnalysisException(String.format(
                  "FOR %s AS OF clause is only supported for Iceberg tables. "
                      + "%s is not an Iceberg table.",
                  timeTravelSpec.getKind() == TimeTravelSpec.Kind.TIME_AS_OF ?
                      "SYSTEM_TIME" :
                      "SYSTEM_VERSION",
                  tbl.getFullName()));
            }
            timeTravelSpec.analyze(this);

            FeIcebergTable rootTable = (FeIcebergTable) tbl;
            tbl = new IcebergTimeTravelTable(rootTable, timeTravelSpec);
          }
          int offset = tblNameIdx + (tbl instanceof IcebergMetadataTable ? 2 : 1);
          candidates.add(new Path(tbl, rawPath.subList(offset, rawPath.size())));
        }
      }
      LOG.trace("Replace candidates with {}", candidates);
    }

    Path result = resolvePaths(rawPath, candidates, pathType, errors);
    if (result == null && resolveInAncestors && hasAncestors()) {
      LOG.trace("Resolve in ancestors");
      result = getParentAnalyzer().resolvePath(rawPath, pathType, true, timeTravelSpec);
    }
    if (result == null) {
      Preconditions.checkState(!errors.isEmpty());
      throw new AnalysisException(errors.getFirst());
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Resolved {} {} to {}", pathType, ToSqlUtils.getPathSql(rawPath),
          result.debugString());
    }
    return result;
  }

  /**
   * Returns a list of unresolved Paths that are rooted at a registered tuple
   * descriptor matching a prefix of the given raw path.
   */
  public List<Path> getTupleDescPaths(List<String> rawPath)
      throws AnalysisException {
    List<Path> result = new ArrayList<>();

    // Path rooted at a tuple desc with an explicit or implicit unqualified alias.
    TupleDescriptor rootDesc = getDescriptor(rawPath.get(0));
    if (rootDesc != null && !rootDesc.isHidden()) {
      result.add(new Path(rootDesc, rawPath.subList(1, rawPath.size())));
    }

    // Path rooted at a tuple desc with an implicit qualified alias.
    if (rawPath.size() > 1) {
      rootDesc = getDescriptor(rawPath.get(0) + "." + rawPath.get(1));
      if (rootDesc != null && !rootDesc.isHidden()) {
        result.add(new Path(rootDesc, rawPath.subList(2, rawPath.size())));
      }
    }
    return result;
  }

  /**
   * Resolves the given paths and checks them for legality and ambiguity. Returns the
   * single legal path resolution if it exists, null otherwise.
   * Populates 'errors' with a prioritized list of error messages starting with the
   * most relevant one. The list contains at least one error message if null is returned.
   */
  private Path resolvePaths(List<String> rawPath, List<Path> paths, PathType pathType,
      LinkedList<String> errors) {
    // For generating error messages.
    String pathTypeStr = null;
    String pathStr = Joiner.on(".").join(rawPath);
    if (pathType == PathType.SLOT_REF) {
      pathTypeStr = "Column/field reference";
    } else if (pathType == PathType.TABLE_REF) {
      pathTypeStr = "Table reference";
    } else if (pathType == PathType.ANY) {
      pathTypeStr = "Path";
    } else {
      Preconditions.checkState(pathType == PathType.STAR);
      pathTypeStr = "Star expression";
      pathStr += ".*";
    }

    List<Path> legalPaths = new ArrayList<>();
    for (Path p: paths) {
      if (!p.resolve()) {
        LOG.trace("Can't resolve path {}", p);
        continue;
      }

      // Check legality of the resolved path.
      if (p.isRootedAtTuple() && !isVisible(p.getRootDesc().getId())) {
        errors.addLast(String.format(
            "Illegal %s '%s' of semi-/anti-joined table '%s'",
            pathTypeStr.toLowerCase(), pathStr, p.getRootDesc().getAlias()));
        continue;
      }
      switch (pathType) {
        // Illegal cases:
        // 1. Destination type is not a collection.
        case TABLE_REF: {
          if (!p.destType().isCollectionType()) {
            errors.addFirst(String.format(
                "Illegal table reference to non-collection type: '%s'\n" +
                    "Path resolved to type: %s", pathStr, p.destType().toSql()));
            continue;
          }
          break;
        }
        case SLOT_REF: {
          // Illegal cases:
          // 1. Path contains an intermediate collection reference.
          // 2. Destination of the path is a catalog table or a registered alias.
          if (p.hasNonDestCollection()) {
            errors.addFirst(String.format(
                "Illegal column/field reference '%s' with intermediate " +
                "collection '%s' of type '%s'",
                pathStr, p.getFirstCollectionName(),
                p.getFirstCollectionType().toSql()));
            continue;
          }
          // Error should be "Could not resolve...". No need to add it here explicitly.
          if (p.getMatchedTypes().isEmpty()) continue;
          break;
        }
        // Illegal cases:
        // 1. Path contains an intermediate collection reference.
        // 2. Destination type of the path is not a struct.
        case STAR: {
          if (p.hasNonDestCollection()) {
            errors.addFirst(String.format(
                "Illegal star expression '%s' with intermediate " +
                "collection '%s' of type '%s'",
                pathStr, p.getFirstCollectionName(),
                p.getFirstCollectionType().toSql()));
            continue;
          }
          if (!p.destType().isStructType()) {
            errors.addFirst(String.format(
                "Cannot expand star in '%s' because path '%s' resolved to type '%s'." +
                "\nStar expansion is only valid for paths to a struct type.",
                pathStr, Joiner.on(".").join(rawPath), p.destType().toSql()));
            continue;
          }
          break;
        }
        case ANY: {
          // Any path is valid.
          break;
        }
      }
      legalPaths.add(p);
    }

    if (legalPaths.size() > 1) {
      errors.addFirst(String.format("%s is ambiguous: '%s'",
          pathTypeStr, pathStr));
      return null;
    }
    if (legalPaths.isEmpty()) {
      if (errors.isEmpty()) {
        errors.addFirst(String.format("Could not resolve %s: '%s'",
            pathTypeStr.toLowerCase(), pathStr));
      }
      return null;
    }
    if (LOG.isTraceEnabled()) LOG.trace("Legal candidate: {}", legalPaths.get(0));
    return legalPaths.get(0);
  }

  public SlotDescriptor registerSlotRef(Path slotPath) throws AnalysisException {
    return registerSlotRef(slotPath, true);
  }

  /**
   * Returns an existing or new SlotDescriptor for the given path.
   * If 'duplicateIfCollections' is true, then always returns a new empty SlotDescriptor
   * for paths with a collection-typed destination.
   */
  public SlotDescriptor registerSlotRef(Path slotPath,
      boolean duplicateIfCollections) throws AnalysisException {
    Preconditions.checkState(slotPath.isRootedAtTuple());
    // If 'tupleStack_' is empty then this is a top level call to this function (not a
    // recursive call) and we push the root TupleDescriptor to 'tupleStack_'.
    try (TupleStackGuard guard = tupleStack_.isEmpty()
        ? new TupleStackGuard(slotPath.getRootDesc()) : null) {
      if (slotPath.destType().isCollectionType() && duplicateIfCollections) {
        // Register a new slot descriptor for collection types. The BE currently
        // relies on this behavior for setting unnested collection slots to NULL.
        SlotDescriptor res = createAndRegisterRawSlotDesc(slotPath, false);
        globalState_.duplicateCollectionSlots.add(res.getId());
        return res;
      }
      // SlotRefs with scalar or struct types are registered against the slot's
      // fully-qualified lowercase path.
      List<String> key = slotPath.getFullyQualifiedRawPath();
      Preconditions.checkState(key.stream().allMatch(s -> s.equals(s.toLowerCase())),
          "Slot paths should be lower case: " + key);
      SlotDescriptor existingSlotDesc = slotPathMap_.get(key);
      if (existingSlotDesc != null) return existingSlotDesc;

      SlotDescriptor existingInTuple = findPathInCurrentTuple(slotPath);
      if (existingInTuple != null) return existingInTuple;

      return createAndRegisterSlotDesc(slotPath);
    }
  }

  // It is possible that another Analyzer, for example a child Analyzer in an inline view,
  // has already inserted a SlotDescriptor with the current path in the current tuple. But
  // because it was a different Analyzer, the path is not present in this Analyzer's
  // 'slotPathMap_'. We should reuse the existing SlotDescriptor unless it was explicitly
  // added with 'duplicateIfCollections=true' in 'registerSlotRef()'.
  //
  // Returns the existing SlotDescriptor with the same path in the current tuple if it
  // exists or 'null' if it doesn't.
  private SlotDescriptor findPathInCurrentTuple(Path slotPath) {
    Preconditions.checkNotNull(slotPath);

    TupleDescriptor currentTupleDesc = tupleStack_.peek();
    Preconditions.checkNotNull(currentTupleDesc);

    final List<String> slotPathFQR = slotPath.getFullyQualifiedRawPath();
    Preconditions.checkNotNull(slotPathFQR);

    for (SlotDescriptor slotDesc : currentTupleDesc.getSlots()) {
      final List<String> tupleSlotFQR = slotDesc.getPath().getFullyQualifiedRawPath();
      if (slotPathFQR.equals(tupleSlotFQR) &&
            !globalState_.duplicateCollectionSlots.contains(slotDesc.getId())) {
        return slotDesc;
      }
    }
    return null;
  }

  private SlotDescriptor createAndRegisterSlotDesc(Path slotPath)
      throws AnalysisException {
    if (slotPath.destType().isCollectionType()) {
      SlotDescriptor result = registerCollectionSlotRef(slotPath);
      registerSlotDesc(slotPath, result, true);
      return result;
    }

    SlotDescriptor result = createAndRegisterRawSlotDesc(slotPath, true);
    if (slotPath.destType().isStructType()) {
      createStructTuplesAndSlotDescs(result);
    }
    return result;
  }

  /**
   * Creates a new SlotDescriptor with path 'slotPath' in the tuple at the top of
   * 'tupleStack_'. Does not create SlotDescriptors for children of complex types. If
   * 'insertIntoSlotPath' is true, inserts the new SlotDescriptor into 'slotPathMap_'.
   * Also registers a column-level privilege request for the new SlotDescriptor.
   */
  private SlotDescriptor createAndRegisterRawSlotDesc(Path slotPath,
      boolean insertIntoSlotPathMap) {
    final SlotDescriptor result = addSlotDescriptorAtCurrentLevel();
    registerSlotDesc(slotPath, result, insertIntoSlotPathMap);
    return result;
  }

  /**
   *  Sets 'slotPath' as the path of 'desc' and registers a column-level privilege request
   *  for it. If 'insertIntoSlotPath' is true, inserts the new SlotDescriptor into
   *  'slotPathMap_'.
   */
  private void registerSlotDesc(Path slotPath, SlotDescriptor desc,
      boolean insertIntoSlotPathMap) {
    Preconditions.checkState(slotPath.isRootedAtTuple());
    desc.setPath(slotPath);
    if (insertIntoSlotPathMap) {
      slotPathMap_.put(slotPath.getFullyQualifiedRawPath(), desc);
    }
    registerColumnPrivReq(desc);
  }

  public void createStructTuplesAndSlotDescs(SlotDescriptor desc)
      throws AnalysisException {
    Preconditions.checkState(desc.getType().isStructType());
    TupleDescriptor structTuple = getDescTbl().createTupleDescriptor("struct_tuple");
    Path slotPath = desc.getPath();
    if (slotPath != null) structTuple.setPath(slotPath);
    final StructType type = (StructType) desc.getType();
    structTuple.setType(type);
    structTuple.setParentSlotDesc(desc);
    desc.setItemTupleDesc(structTuple);

    try (TupleStackGuard guard = new TupleStackGuard(structTuple)) {
      for (StructField structField : type.getFields()) {
        // 'slotPath' could be null e.g. when the query has an order by clause and
        // this is the sorting tuple.
        // TODO: IMPALA-12160: When we enable structs containing collections in sorting
        // tuples we need to revisit this. Currently collection SlotDescriptors cannot be
        // created without a path. Maybe descriptors should have a path even in the
        // sorting tuple.
        if (slotPath == null) {
          createStructTuplesAndSlotDescsWithoutPath(slotPath, structField);
        } else {
          Path childRelPath = Path.createRelPath(slotPath, structField.getName());
          childRelPath.resolve();
          SlotDescriptor childDesc = createAndRegisterSlotDesc(childRelPath);
        }
      }
    }
  }

  private void createStructTuplesAndSlotDescsWithoutPath(Path slotPath,
      StructField structField) throws AnalysisException {
    Preconditions.checkState(slotPath == null);
    Preconditions.checkState(!structField.getType().isCollectionType());

    SlotDescriptor childDesc = addSlotDescriptorAtCurrentLevel();
    childDesc.setType(structField.getType());

    if (childDesc.getType().isStructType()) {
      createStructTuplesAndSlotDescs(childDesc);
    }
  }

  /**
   * Registers a collection and its descendants.
   * Creates a CollectionTableRef for all collections on the path.
   */
  private SlotDescriptor registerCollectionSlotRef(Path slotPath)
      throws AnalysisException {
    Preconditions.checkState(slotPath.isResolved());
    Preconditions.checkState(slotPath.destType().isCollectionType());
    List<String> rawPath = slotPath.getRawPath();
    List<String> collectionTableRawPath = new ArrayList<>();
    TupleDescriptor rootDesc = slotPath.getRootDesc();
    Preconditions.checkNotNull(rootDesc);
    if (rootDesc.hasExplicitAlias()) {
      collectionTableRawPath.add(rootDesc.getAlias());
    } else {
      collectionTableRawPath.addAll(Arrays.asList(rootDesc.getAlias().split("\\.")));
    }
    collectionTableRawPath.addAll(rawPath);

    TableRef tblRef = new TableRef(collectionTableRawPath, null);
    CollectionTableRef collTblRef = new CollectionTableRef(tblRef, slotPath, true);
    collTblRef.analyze(this);

    Preconditions.checkState(collTblRef.getCollectionExpr() instanceof SlotRef);
    SlotDescriptor desc = ((SlotRef) collTblRef.getCollectionExpr()).getDesc();
    desc.setShouldMaterializeRecursively(true);

    try (TupleStackGuard guard = new TupleStackGuard(desc.getItemTupleDesc())) {
      if (slotPath.destType().isArrayType()) {
        // Resolve path
        List<String> rawPathToItem = Arrays.asList(Path.ARRAY_ITEM_FIELD_NAME);
        resolveAndRegisterDescendantPath(collTblRef, rawPathToItem);
      } else {
        Preconditions.checkState(slotPath.destType().isMapType());

        List<String> rawPathToKey = Arrays.asList(Path.MAP_KEY_FIELD_NAME);
        resolveAndRegisterDescendantPath(collTblRef, rawPathToKey);

        List<String> rawPathToValue = Arrays.asList(Path.MAP_VALUE_FIELD_NAME);
        resolveAndRegisterDescendantPath(collTblRef, rawPathToValue);
      }
    }

    return desc;
  }

  private void resolveAndRegisterDescendantPath(CollectionTableRef collTblRef,
      List<String> rawPath) throws AnalysisException {
    Path resolvedPath = new Path(collTblRef.getDesc(), rawPath);
    boolean isResolved = resolvedPath.resolve();
    Preconditions.checkState(isResolved);

    registerSlotRef(resolvedPath, false);
  }

  /**
   * Registers a column-level privilege request if 'slotDesc' directly or indirectly
   * refers to a table column. It handles both scalar and complex-typed columns.
   */
  private void registerColumnPrivReq(SlotDescriptor slotDesc) {
    Preconditions.checkNotNull(slotDesc.getPath());
    TupleDescriptor tupleDesc = slotDesc.getParent();
    if (tupleDesc.isMaterialized() && tupleDesc.getTable() != null) {
      Column column = tupleDesc.getTable().getColumn(
          slotDesc.getPath().getRawPath().get(0));
      if (column != null) {
        registerPrivReq(builder -> builder
            .allOf(Privilege.SELECT)
            .onColumn(tupleDesc.getTableName().getDb(), tupleDesc.getTableName().getTbl(),
                column.getName(), tupleDesc.getTable().getOwnerUser()).build());
      }
    }
  }

  /**
   * Register columns for resolving column mask. The order in which columns are registered
   * is not necessarily the same as the relative order of those columns in the
   * corresponding Hive table.
   */
  public void registerColumnForMasking(SlotDescriptor slotDesc) {
    Preconditions.checkNotNull(slotDesc.getPath());
    TupleDescriptor tupleDesc = slotDesc.getParent();
    // Pass the full path for nested types even though these are ignore currently.
    // TODO: RANGER-3525: Clarify handling of column masks on nested types
    Column column;
    if (slotDesc.isVirtualColumn()) {
      column = VirtualColumn.getVirtualColumn(slotDesc.getVirtualColumnType());
    }
    else {
      column = new Column(
          String.join(".", slotDesc.getPath().getRawPath()), slotDesc.getType(),
            /*position*/-1);
    }
    Analyzer analyzer = this;
    TableRef tblRef;
    do {
      tblRef = analyzer.tableRefMap_.get(tupleDesc.getId());
      // Search in parent query block for correlative reference.
      analyzer = analyzer.getParentAnalyzer();
    } while (tblRef == null && analyzer != null);
    if (tblRef == null) return;
    Preconditions.checkNotNull(tblRef,
        "Failed to find TableRef of tuple {}", tupleDesc);
    tblRef.registerColumn(column);
  }

  /**
   * Creates a new slot descriptor and related state in globalState.
   */
  public SlotDescriptor addSlotDescriptor(TupleDescriptor tupleDesc) {
    SlotDescriptor result = globalState_.descTbl.addSlotDescriptor(tupleDesc);
    globalState_.blockBySlot.put(result.getId(), this);
    return result;
  }

  /**
   * Creates a new slot descriptor and related state in globalState. The new slot
   * descriptor will be created in the tuple at the top of 'tupleStack_'.
   */
  private SlotDescriptor addSlotDescriptorAtCurrentLevel() {
    TupleDescriptor tupleDesc = tupleStack_.peek();
    Preconditions.checkNotNull(tupleDesc);
    return addSlotDescriptor(tupleDesc);
  }

  /**
   * Adds a new slot descriptor in tupleDesc that is identical to srcSlotDesc
   * except for the path and slot id.
   */
  public SlotDescriptor copySlotDescriptor(SlotDescriptor srcSlotDesc,
      TupleDescriptor tupleDesc) {
    SlotDescriptor result = globalState_.descTbl.addSlotDescriptor(tupleDesc);
    globalState_.blockBySlot.put(result.getId(), this);
    result.setSourceExprs(srcSlotDesc.getSourceExprs());
    result.setLabel(srcSlotDesc.getLabel());
    result.setStats(srcSlotDesc.getStats());
    result.setType(srcSlotDesc.getType());
    result.setItemTupleDesc(srcSlotDesc.getItemTupleDesc());
    return result;
  }

  /**
   * Register all conjuncts in a list of predicates as Having-clause conjuncts.
   */
  public void registerConjuncts(List<Expr> l) throws AnalysisException {
    for (Expr e: l) {
      registerConjuncts(e, true);
    }
  }

  /**
   * Register all conjuncts in 'conjuncts' that make up the On-clause of the given
   * right-hand side of a join. Assigns each conjunct a unique id. If rhsRef is
   * the right-hand side of an outer join, then the conjuncts conjuncts are
   * registered such that they can only be evaluated by the node implementing that
   * join.
   */
  public void registerOnClauseConjuncts(List<Expr> conjuncts, TableRef rhsRef)
      throws AnalysisException {
    Preconditions.checkNotNull(rhsRef);
    Preconditions.checkNotNull(conjuncts);
    List<ExprId> ojConjuncts = null;
    if (rhsRef.getJoinOp().isOuterJoin()) {
      ojConjuncts = globalState_.conjunctsByOjClause.get(rhsRef.getId());
      if (ojConjuncts == null) {
        ojConjuncts = new ArrayList<>();
        globalState_.conjunctsByOjClause.put(rhsRef.getId(), ojConjuncts);
      }
    }
    boolean foundConstantFalse = false;
    for (Expr conjunct: conjuncts) {
      conjunct.setIsOnClauseConjunct(true);
      registerConjunct(conjunct);
      if (rhsRef.getJoinOp().isOuterJoin()) {
        globalState_.ojClauseByConjunct.put(conjunct.getId(), rhsRef);
        ojConjuncts.add(conjunct.getId());
      }
      if (rhsRef.getJoinOp().isSemiJoin()) {
        globalState_.sjClauseByConjunct.put(conjunct.getId(), rhsRef);
      }
      if (rhsRef.getJoinOp().isInnerJoin()) {
        globalState_.ijClauseByConjunct.put(conjunct.getId(), rhsRef);
      }
      if (markConstantConjunct(conjunct, false)) {
        foundConstantFalse = true;
      }
    }
    // If a constant FALSE conjunct is found, we don't need to evaluate the other
    // conjuncts in the same list. By marking the conjunct as assigned, the
    // getUnassignedConjuncts() method will not return it, which means we won't
    // consider it anymore.
    if (foundConstantFalse) {
      markConjunctsAssigned(conjuncts);
    }
  }

  /**
   * Register all conjuncts that make up 'e'. If fromHavingClause is false, this conjunct
   * is assumed to originate from a WHERE or ON clause.
   */
  public void registerConjuncts(Expr e, boolean fromHavingClause)
      throws AnalysisException {
    boolean foundConstantFalse = false;
    for (Expr conjunct : e.getConjuncts()) {
      registerConjunct(conjunct);
      foundConstantFalse = markConstantConjunct(conjunct, fromHavingClause);
      if (foundConstantFalse) break;
    }
    // If a constant FALSE conjunct is found, we don't need to evaluate the other
    // conjuncts in the same list. By marking the conjunct as assigned, the
    // getUnassignedConjuncts() method will not return it, which means we won't
    // consider it anymore.
    if (foundConstantFalse) {
      markConjunctsAssigned(e.getConjuncts());
    }
  }

  /**
   * If the given conjunct is a constant non-oj conjunct, marks it as assigned, and
   * evaluates the conjunct. If the conjunct evaluates to false, marks this query
   * block as having an empty result set or as having an empty select-project-join
   * portion, if fromHavingClause is true or false, respectively.
   * No-op if the conjunct is not constant or is outer joined.
   * Return true, if conjunct is constant FALSE.
   * Throws an AnalysisException if there is an error evaluating `conjunct`
   */
  private boolean markConstantConjunct(Expr conjunct, boolean fromHavingClause)
      throws AnalysisException {
    if (!conjunct.isConstant() || isOjConjunct(conjunct)) return false;
    markConjunctAssigned(conjunct);
    if ((!fromHavingClause && !hasEmptySpjResultSet_)
        || (fromHavingClause && !hasEmptyResultSet_)) {
      try {
        if (conjunct instanceof BetweenPredicate) {
          // Rewrite the BetweenPredicate into a CompoundPredicate so we can evaluate it
          // below (BetweenPredicates are not executable). We might be in the first
          // analysis pass, so the conjunct may not have been rewritten yet.
          ExprRewriter rewriter = new ExprRewriter(BetweenToCompoundRule.INSTANCE);
          conjunct = rewriter.rewrite(conjunct, this);
          // analyze this conjunct here: we know it can't contain references to select list
          // aliases and having it analyzed is needed for the following EvalPredicate() call
          conjunct.analyze(this);
        }
        if (!FeSupport.EvalPredicate(conjunct, globalState_.queryCtx)) {
          if (fromHavingClause) {
            hasEmptyResultSet_ = true;
          } else {
            hasEmptySpjResultSet_ = true;
          }
          return true;
        }
      } catch (InternalException ex) {
        throw new AnalysisException("Error evaluating \"" + conjunct.toSql() + "\"", ex);
      }
    }
    return false;
  }

  /**
   * Assigns a new id to the given conjunct and registers it with all tuple and slot ids
   * it references and with the global conjunct list.
   */
  private void registerConjunct(Expr e) {
    // always generate a new expr id; this might be a cloned conjunct that already
    // has the id of its origin set
    e.setId(globalState_.conjunctIdGenerator.getNextId());
    globalState_.conjuncts.put(e.getId(), e);

    if (!e.isAuxExpr()) {
      globalState_.conjunctsFromQuery.put(e.getId(), e);
    }

    List<TupleId> tupleIds = new ArrayList<>();
    List<SlotId> slotIds = new ArrayList<>();
    e.getIds(tupleIds, slotIds);
    registerFullOuterJoinedConjunct(e);

    // register single tid conjuncts
    if (tupleIds.size() == 1 && !e.isAuxExpr()) {
      globalState_.singleTidConjuncts.add(e.getId());
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("register tuple/slotConjunct: " + Integer.toString(e.getId().asInt())
          + " " + e.toSql() + " " + e.debugString());
    }

    if (!(e instanceof BinaryPredicate)) return;
    BinaryPredicate binaryPred = (BinaryPredicate) e;

    // check whether this is an equi-join predicate, ie, something of the
    // form <expr1> = <expr2> where at least one of the exprs is bound by
    // exactly one tuple id
    if (binaryPred.getOp() != BinaryPredicate.Operator.EQ &&
       binaryPred.getOp() != BinaryPredicate.Operator.NULL_MATCHING_EQ &&
       binaryPred.getOp() != BinaryPredicate.Operator.NOT_DISTINCT) {
      return;
    }
    // the binary predicate must refer to at least two tuples to be an eqJoinConjunct
    if (tupleIds.size() < 2) return;

    // examine children and update eqJoinConjuncts
    for (int i = 0; i < 2; ++i) {
      tupleIds = new ArrayList<>();
      binaryPred.getChild(i).getIds(tupleIds, null);
      if (tupleIds.size() == 1) {
        if (!globalState_.eqJoinConjuncts.containsKey(tupleIds.get(0))) {
          List<ExprId> conjunctIds = new ArrayList<>();
          conjunctIds.add(e.getId());
          globalState_.eqJoinConjuncts.put(tupleIds.get(0), conjunctIds);
        } else {
          globalState_.eqJoinConjuncts.get(tupleIds.get(0)).add(e.getId());
        }
        binaryPred.setIsEqJoinConjunct(true);
        LOG.trace("register eqJoinConjunct: " + Integer.toString(e.getId().asInt()));
      }
    }
  }

  /**
   * Create and register an auxiliary predicate to express a mutual value transfer
   * between two exprs (BinaryPredicate with EQ); this predicate does not need to be
   * assigned, but it's used for value transfer computation.
   * Does nothing if the lhs or rhs expr are NULL. Registering with NULL would be
   * incorrect, because <expr> = NULL is false (even NULL = NULL).
   */
  public void createAuxEqPredicate(Expr lhs, Expr rhs) {
    // Check the expr type as well as the class because  NullLiteral could have been
    // implicitly cast to a type different than NULL.
    if (Expr.IS_NULL_LITERAL.apply(lhs) || Expr.IS_NULL_LITERAL.apply(rhs) ||
        lhs.getType().isNull() || rhs.getType().isNull()) {
      return;
    }
    // create an eq predicate between lhs and rhs
    BinaryPredicate p = new BinaryPredicate(BinaryPredicate.Operator.EQ, lhs, rhs);
    p.setIsAuxExpr();
    if (LOG.isTraceEnabled()) {
      LOG.trace("register auxiliary eq predicate: " + p.toSql() + " " + p.debugString());
    }
    registerConjunct(p);
  }

  /**
   * Creates an inferred equality predicate between the given slots.
   */
  public BinaryPredicate createInferredEqPred(SlotId lhsSlotId, SlotId rhsSlotId) {
    BinaryPredicate pred = new BinaryPredicate(BinaryPredicate.Operator.EQ,
        new SlotRef(globalState_.descTbl.getSlotDesc(lhsSlotId)),
        new SlotRef(globalState_.descTbl.getSlotDesc(rhsSlotId)));
    pred.setIsInferred();
    // create casts if needed
    pred.analyzeNoThrow(this);
    return pred;
  }

  /**
   * Return all unassigned non-constant registered conjuncts that are fully bound by
   * given list of tuple ids. If 'inclOjConjuncts' is false, conjuncts tied to an
   * Outer Join clause are excluded.
   */
  public List<Expr> getUnassignedConjuncts(
      List<TupleId> tupleIds, boolean inclOjConjuncts) {
    List<Expr> result = new ArrayList<>();
    for (Expr e: globalState_.conjunctsFromQuery.values()) {
      if (e.isBoundByTupleIds(tupleIds)
          && !globalState_.assignedConjuncts.contains(e.getId())
          && ((inclOjConjuncts && !e.isConstant())
              || !globalState_.ojClauseByConjunct.containsKey(e.getId()))) {
        result.add(e);
      }
    }
    return result;
  }

  public TableRef getOjRef(Expr e) {
    return globalState_.ojClauseByConjunct.get(e.getId());
  }

  public boolean isOjConjunct(Expr e) {
    return globalState_.ojClauseByConjunct.containsKey(e.getId());
  }

  public boolean isIjConjunct(Expr e) {
    return globalState_.ijClauseByConjunct.containsKey(e.getId());
  }

  public boolean isSjConjunct(Expr e) {
    return globalState_.sjClauseByConjunct.containsKey(e.getId());
  }

  public TableRef getFullOuterJoinRef(Expr e) {
    return globalState_.fullOuterJoinedConjuncts.get(e.getId());
  }

  public boolean isFullOuterJoined(Expr e) {
    return globalState_.fullOuterJoinedConjuncts.containsKey(e.getId());
  }

  /**
   * Return all unassigned registered conjuncts for node's table ref ids.
   * Wrapper around getUnassignedConjuncts(List<TupleId> tupleIds).
   */
  public final List<Expr> getUnassignedConjuncts(PlanNode node) {
    List<TupleId> tupleIds = Lists.newCopyOnWriteArrayList(node.getTblRefIds());
    if (node instanceof JoinNode) {
      for (TupleId tid : node.getTblRefIds()) {
        // Pick up TupleId of the masked table since the table masking view and the masked
        // table are the same in the original query. SlotRefs of table's nested columns
        // are resolved into table's tuple, but not the table masking view's tuple. As a
        // result, predicates referencing such nested columns also reference the masked
        // table's tuple id.
        TupleDescriptor tuple = getTupleDesc(tid);
        Preconditions.checkNotNull(tuple);
        BaseTableRef maskedTable = tuple.getMaskedTable();
        if (maskedTable != null && maskedTable.exposeNestedColumnsByTableMaskView()) {
          tupleIds.add(maskedTable.getId());
        }
      }
    }
    return getUnassignedConjuncts(tupleIds);
  }

  /**
   * Return all unassigned registered conjuncts that are fully bound by the given
   * (logical) tuple ids, can be evaluated by 'tupleIds' and are not tied to an
   * Outer Join clause.
   */
  public List<Expr> getUnassignedConjuncts(List<TupleId> tupleIds) {
    List<Expr> result = new ArrayList<>();
    for (Expr e: getUnassignedConjuncts(tupleIds, true)) {
      if (canEvalPredicate(tupleIds, e)) result.add(e);
    }
    return result;
  }

  public String conjunctAssignmentsDebugString() {
    StringBuilder res = new StringBuilder();
    for (Expr _e : globalState_.conjuncts.values()) {
      String state = globalState_.assignedConjuncts.contains(_e.getId()) ? "assigned"
              : "unassigned";
      res.append("\n\t" + state + " " + _e.debugString());
    }
    return res.toString();
  }

  /**
   * Returns true if 'e' must be evaluated after or by a join node. Note that it may
   * still be safe to evaluate 'e' elsewhere as well, but in any case 'e' must be
   * evaluated again by or after a join.
   */
  public boolean evalAfterJoin(Expr e) {
    List<TupleId> tids = new ArrayList<>();
    e.getIds(tids, null);
    if (tids.isEmpty()) return false;
    if (tids.size() > 1 || isOjConjunct(e) || isFullOuterJoined(e)
        || (isOuterJoined(tids.get(0))
            && (!e.isOnClauseConjunct() || isIjConjunct(e)))
        || (isAntiJoinedConjunct(e) && !isSemiJoined(tids.get(0)))) {
      return true;
    }
    return false;
  }

  /**
   * Return all unassigned conjuncts of the outer join referenced by right-hand side
   * table ref.
   */
  public List<Expr> getUnassignedOjConjuncts(TableRef ref) {
    Preconditions.checkState(ref.getJoinOp().isOuterJoin());
    List<Expr> result = new ArrayList<>();
    List<ExprId> candidates = globalState_.conjunctsByOjClause.get(ref.getId());
    if (candidates == null) return result;
    for (ExprId conjunctId: candidates) {
      if (!globalState_.assignedConjuncts.contains(conjunctId)) {
        Expr e = globalState_.conjuncts.get(conjunctId);
        Preconditions.checkNotNull(e);
        result.add(e);
      }
    }
    return result;
  }

  /**
   * Return rhs ref of last Join clause that outer-joined id.
   */
  public TableRef getLastOjClause(TupleId id) {
    return globalState_.outerJoinedTupleIds.get(id);
  }

  /**
   * Return slot descriptor corresponding to column referenced in the context of
   * tupleDesc, or null if no such reference exists.
   */
  public SlotDescriptor getColumnSlot(TupleDescriptor tupleDesc, Column col) {
    for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
      if (slotDesc.getColumn() == col) return slotDesc;
    }
    return null;
  }

  public DescriptorTable getDescTbl() { return globalState_.descTbl; }
  public FeCatalog getCatalog() { return globalState_.stmtTableCache.catalog; }
  public StmtTableCache getStmtTableCache() { return globalState_.stmtTableCache; }
  public Set<String> getAliases() { return aliasMap_.keySet(); }
  public void removeAlias(String alias) { aliasMap_.remove(alias); }

  /**
   * Returns list of candidate equi-join conjuncts to be evaluated by the join node
   * that is specified by the table ref ids of its left and right children.
   * If the join to be performed is an outer join, then only equi-join conjuncts
   * from its On-clause are returned. If an equi-join conjunct is full outer joined,
   * then it is only added to the result if this join is the one to full-outer join it.
   */
  public List<Expr> getEqJoinConjuncts(List<TupleId> lhsTblRefIds,
      List<TupleId> rhsTblRefIds) {
    // Contains all equi-join conjuncts that have one child fully bound by one of the
    // rhs table ref ids (the other child is not bound by that rhs table ref id).
    List<ExprId> conjunctIds = new ArrayList<>();
    for (TupleId rhsId: rhsTblRefIds) {
      List<ExprId> cids = globalState_.eqJoinConjuncts.get(rhsId);
      if (cids == null) continue;
      for (ExprId eid: cids) {
        if (!conjunctIds.contains(eid)) conjunctIds.add(eid);
      }
    }

    // Since we currently prevent join re-reordering across outer joins, we can never
    // have a bushy outer join with multiple rhs table ref ids. A busy outer join can
    // only be constructed with an inline view (which has a single table ref id).
    List<ExprId> ojClauseConjuncts = null;
    if (rhsTblRefIds.size() == 1) {
      ojClauseConjuncts = globalState_.conjunctsByOjClause.get(rhsTblRefIds.get(0));
    }

    // List of table ref ids that the join node will 'materialize'.
    List<TupleId> nodeTblRefIds = Lists.newArrayList(lhsTblRefIds);
    nodeTblRefIds.addAll(rhsTblRefIds);
    List<Expr> result = new ArrayList<>();
    for (ExprId conjunctId: conjunctIds) {
      Expr e = globalState_.conjuncts.get(conjunctId);
      Preconditions.checkState(e != null);
      if (!canEvalFullOuterJoinedConjunct(e, nodeTblRefIds) ||
          !canEvalAntiJoinedConjunct(e, nodeTblRefIds) ||
          !canEvalOuterJoinedConjunct(e, nodeTblRefIds)) {
        continue;
      }

      if (ojClauseConjuncts != null && !ojClauseConjuncts.contains(conjunctId)) continue;
      result.add(e);
    }
    return result;
  }

  /**
   * Returns false if 'e' references a full outer joined tuple and it is incorrect to
   * evaluate 'e' at a node materializing 'tids'. Returns true otherwise.
   */
  public boolean canEvalFullOuterJoinedConjunct(Expr e, List<TupleId> tids) {
    TableRef fullOjRef = getFullOuterJoinRef(e);
    if (fullOjRef == null) return true;
    // 'ojRef' represents the outer-join On-clause that 'e' originates from (if any).
    // Might be the same as 'fullOjRef'. If different from 'fullOjRef' it means that
    // 'e' should be assigned to the node materializing the 'ojRef' tuple ids.
    TableRef ojRef = getOjRef(e);
    TableRef targetRef = (ojRef != null && ojRef != fullOjRef) ? ojRef : fullOjRef;
    return tids.containsAll(targetRef.getAllTableRefIds());
  }

  /**
   * Returns false if 'e' originates from an outer-join On-clause and it is incorrect to
   * evaluate 'e' at a node materializing 'tids'. Returns true otherwise.
   */
  public boolean canEvalOuterJoinedConjunct(Expr e, List<TupleId> tids) {
    TableRef outerJoin = getOjRef(e);
    if (outerJoin == null) return true;
    return tids.containsAll(outerJoin.getAllTableRefIds());
  }

  /**
   * Returns true if predicate 'e' can be correctly evaluated by a tree materializing
   * 'tupleIds', otherwise false:
   * - The predicate needs to be bound by tupleIds.
   * - For On-clause predicates, see canEvalOnClauseConjunct() for more info.
   * - Otherwise, a predicate can only be correctly evaluated if for all outer-joined
   *   referenced tids the last join to outer-join this tid has been materialized.
   */
  public boolean canEvalPredicate(List<TupleId> tupleIds, Expr e) {
    if (!e.isBoundByTupleIds(tupleIds)) return false;
    List<TupleId> tids = new ArrayList<>();
    e.getIds(tids, null);
    if (tids.isEmpty()) return true;

    if (e.isOnClauseConjunct()) {
      return canEvalOnClauseConjunct(tupleIds, e);
    }
    return isLastOjMaterializedByTupleIds(tupleIds, e);
  }

  /**
   * Checks if for all outer-joined referenced tids of the predicate the last join
   * to outer-join this tid has been materialized by tupleIds.
   */
  public boolean isLastOjMaterializedByTupleIds(List<TupleId> tupleIds, Expr e) {
    List<TupleId> tids = new ArrayList<>();
    e.getIds(tids, null);
    for (TupleId tid: tids) {
      TableRef rhsRef = getLastOjClause(tid);
      // Ignore 'tid' because it is not outer-joined.
      if (rhsRef == null) continue;
      // Check whether the last join to outer-join 'tid' is materialized by tupleIds.
      if (!tupleIds.containsAll(rhsRef.getAllTableRefIds())) return false;
    }
    return true;
  }

  /**
   * Checks if a conjunct from the On-clause can be evaluated in a node that materializes
   * a given list of tuple ids.
   * - If the predicate is from an anti-join On-clause it must be evaluated by the
   * corresponding anti-join node.
   * - Predicates from the On-clause of an inner or semi join are evaluated at the node
   * that materializes the required tuple ids, unless they reference outer joined tuple
   * ids. In that case, the predicates are evaluated at the join node of the corresponding
   * On-clause.
   * - Predicates referencing full-outer joined tuples are assigned at the originating
   * join if it is a full-outer join, otherwise at the last full-outer join that does not
   * materialize the table ref ids of the originating join.
   * - Predicates from the On-clause of a left/right outer join are assigned at the
   * corresponding outer join node with the exception of simple predicates that only
   * reference a single tuple id. Those may be assigned below the outer join node if they
   * are from the same On-clause that makes the tuple id nullable.
   */
  public boolean canEvalOnClauseConjunct(List<TupleId> tupleIds, Expr e) {
    Preconditions.checkState(e.isOnClauseConjunct());
    if (isAntiJoinedConjunct(e)) return canEvalAntiJoinedConjunct(e, tupleIds);

    List<TupleId> exprTids = new ArrayList<>();
    e.getIds(exprTids, null);
    if (exprTids.isEmpty()) return true;

    if (isIjConjunct(e) || isSjConjunct(e)) {
      if (!containsOuterJoinedTid(exprTids)) return true;
      // If the predicate references an outer-joined tuple, then evaluate it at
      // the join that the On-clause belongs to.
      TableRef onClauseTableRef = null;
      if (isIjConjunct(e)) {
        onClauseTableRef = globalState_.ijClauseByConjunct.get(e.getId());
      } else {
        onClauseTableRef = globalState_.sjClauseByConjunct.get(e.getId());
      }
      Preconditions.checkNotNull(onClauseTableRef);
      return tupleIds.containsAll(onClauseTableRef.getAllTableRefIds());
    }

    if (isFullOuterJoined(e)) return canEvalFullOuterJoinedConjunct(e, tupleIds);
    if (isOjConjunct(e)) {
      // Force this predicate to be evaluated by the corresponding outer join node.
      // The join node will pick up the predicate later via getUnassignedOjConjuncts().
      if (exprTids.size() > 1) return false;
      // Optimization for single-tid predicates: Legal to assign below the outer join
      // if the predicate is from the same On-clause that makes tid nullable
      // (otherwise e needn't be true when that tuple is set).
      TupleId tid = exprTids.get(0);
      return globalState_.ojClauseByConjunct.get(e.getId()) == getLastOjClause(tid);
    }

    // Should have returned in one of the cases above.
    Preconditions.checkState(false);
    return false;
  }

  /**
   * Checks if a conjunct from the On-clause of an anti join can be evaluated in a node
   * that materializes a given list of tuple ids.
   */
  public boolean canEvalAntiJoinedConjunct(Expr e, List<TupleId> nodeTupleIds) {
    TableRef antiJoinRef = getAntiJoinRef(e);
    if (antiJoinRef == null) return true;
    List<TupleId> tids = new ArrayList<>();
    e.getIds(tids, null);
    if (tids.size() > 1) {
      return nodeTupleIds.containsAll(antiJoinRef.getAllTableRefIds())
          && antiJoinRef.getAllTableRefIds().containsAll(nodeTupleIds);
    }
    // A single tid conjunct that is anti-joined can be safely assigned to a
    // node below the anti join that specified it.
    return globalState_.semiJoinedTupleIds.containsKey(tids.get(0));
  }

  /**
   * Returns a list of predicates that are fully bound by destTid. The generated
   * predicates are for optimization purposes and not required for query correctness.
   * It is up to the caller to decide if a bound predicate should actually be used.
   * Predicates are derived by replacing the slots of a source predicate with slots of
   * the destTid, if every source slot has a value transfer to a slot in destTid.
   * In particular, the returned list contains predicates that must be evaluated
   * at a join node (bound to outer-joined tuple) but can also be safely evaluated by a
   * plan node materializing destTid. Such predicates are not marked as assigned.
   * All other inferred predicates are marked as assigned if 'markAssigned'
   * is true. This function returns bound predicates regardless of whether the source
   * predicates have been assigned.
   * Destination slots in destTid can be ignored by passing them in ignoreSlots.
   * Some bound predicates may be missed due to errors in backend expr evaluation
   * or expr substitution.
   * TODO: exclude UDFs from predicate propagation? their overloaded variants could
   * have very different semantics
   */
  public List<Expr> getBoundPredicates(TupleId destTid, Set<SlotId> ignoreSlots,
      boolean markAssigned) {
    // Map that tracks BinaryPredicates that derived from the same BetweenPredicate.
    Map<ExprId, List<BinaryPredicate>> betweenPredicates = new HashMap<>();
    List<Expr> result = new ArrayList<>();
    for (ExprId srcConjunctId: globalState_.singleTidConjuncts) {
      Expr srcConjunct = globalState_.conjuncts.get(srcConjunctId);
      if (srcConjunct instanceof SlotRef) continue;
      Preconditions.checkNotNull(srcConjunct);
      List<TupleId> srcTids = new ArrayList<>();
      List<SlotId> srcSids = new ArrayList<>();
      srcConjunct.getIds(srcTids, srcSids);
      Preconditions.checkState(srcTids.size() == 1);

      // Generate slot-mappings to bind srcConjunct to destTid.
      TupleId srcTid = srcTids.get(0);
      List<List<SlotId>> allDestSids =
          getValueTransferDestSlotIds(srcTid, srcSids, destTid, ignoreSlots);
      if (allDestSids.isEmpty()) continue;

      // Indicates whether there is value transfer from the source slots to slots that
      // belong to an outer-joined tuple.
      boolean hasOuterJoinedTuple = hasOuterJoinedValueTransferTarget(srcSids);

      // It is incorrect to propagate predicates into a plan subtree that is on the
      // nullable side of an outer join if the predicate evaluates to true when all
      // its referenced tuples are NULL. For example:
      // select * from (select A.a, B.b, B.col from A left join B on A.a=B.b) v
      // where v.col is null
      // In this query (v.col is null) should not be evaluated at the scanner of B.
      // The check below is conservative because the outer-joined tuple making
      // 'hasOuterJoinedTuple' true could be in a parent block of 'srcConjunct', in which
      // case it is safe to propagate 'srcConjunct' within child blocks of the
      // outer-joined parent block.
      // TODO: Make the check precise by considering the blocks (analyzers) where the
      // outer-joined tuples in the dest slot's equivalence classes appear
      // relative to 'srcConjunct'.
      try {
        if (hasOuterJoinedTuple && isTrueWithNullSlots(srcConjunct)) continue;
      } catch (InternalException e) {
        // Expr evaluation failed in the backend. Skip 'srcConjunct' since we cannot
        // determine whether propagation is safe.
        LOG.warn("Skipping propagation of conjunct because backend evaluation failed: "
            + srcConjunct.toSql(), e);
        continue;
      }

      // if srcConjunct comes out of an OJ's On clause, we need to make sure it's the
      // same as the one that makes destTid nullable
      // (otherwise srcConjunct needn't be true when destTid is set)
      if (globalState_.ojClauseByConjunct.containsKey(srcConjunct.getId())) {
        if (!globalState_.outerJoinedTupleIds.containsKey(destTid)) continue;
        if (globalState_.ojClauseByConjunct.get(srcConjunct.getId())
            != globalState_.outerJoinedTupleIds.get(destTid)) {
          continue;
        }
        // Do not propagate conjuncts from the on-clause of full-outer or anti-joins.
        TableRef tblRef = globalState_.ojClauseByConjunct.get(srcConjunct.getId());
        if (tblRef.getJoinOp().isFullOuterJoin()) continue;
      }

      // Conjuncts specified in the ON-clause of an anti-join must be evaluated at that
      // join node.
      if (isAntiJoinedConjunct(srcConjunct)) continue;

      // Generate predicates for all src-to-dest slot mappings.
      for (List<SlotId> destSids: allDestSids) {
        Preconditions.checkState(destSids.size() == srcSids.size());
        Expr p;
        if (srcSids.containsAll(destSids)) {
          p = srcConjunct;
        } else {
          // It is incorrect to propagate predicates inferred from equi-join conjuncts
          // into a plan subtree that is on the nullable side of an outer join if the
          // predicate is not null-filtering.
          // For example:
          // select * from (select id is not null and col is null as a from (select A.id,
          // B.col from A left join B on A.id = B.id) t ) t where a = 1
          // In this query the inferred predicate (B.id is not null and B.col is null = 1)
          // should not be evaluated at the scanner of B.
          // We use 'ojmap' to save the mapping from src to the outer join equal slot,
          // eg. B.id. We substitue the non-outer-join slots first and use
          // 'isNullableConjunct' to do a more strict check on the conjunct before the
          // final substitution.
          ExprSubstitutionMap smap = new ExprSubstitutionMap();
          ExprSubstitutionMap ojSmap = new ExprSubstitutionMap();
          for (int i = 0; i < srcSids.size(); ++i) {
            if (globalState_.ojNullableSlotsInEquiPreds.contains(destSids.get(i))) {
              ojSmap.put(
                new SlotRef(globalState_.descTbl.getSlotDesc(srcSids.get(i))),
                new SlotRef(globalState_.descTbl.getSlotDesc(destSids.get(i))));
            } else {
              smap.put(
                  new SlotRef(globalState_.descTbl.getSlotDesc(srcSids.get(i))),
                  new SlotRef(globalState_.descTbl.getSlotDesc(destSids.get(i))));
            }
          }
          try {
            p = srcConjunct.trySubstitute(smap, this, false);
            if (hasOuterJoinedTuple &&
                isNullableConjunct(p, Arrays.asList(destTid))) continue;
            if (ojSmap.size() != 0) {
              p = p.trySubstitute(ojSmap, this, false);
            }
          } catch (ImpalaException exc) {
            // not an executable predicate; ignore
            continue;
          }
          // Unset the id because this bound predicate itself is not registered, and
          // to prevent callers from inadvertently marking the srcConjunct as assigned.
          p.setId(null);
          if (p instanceof BinaryPredicate) ((BinaryPredicate) p).setIsInferred();
          if (LOG.isTraceEnabled()) {
            LOG.trace("new pred: " + p.toSql() + " " + p.debugString());
          }
        }

        if (markAssigned) {
          // predicate assignment doesn't hold if:
          // - the application against slotId doesn't transfer the value back to its
          //   originating slot
          // - the original predicate is on an OJ'd table but doesn't originate from
          //   that table's OJ clause's ON clause (if it comes from anywhere but that
          //   ON clause, it needs to be evaluated directly by the join node that
          //   materializes the OJ'd table)
          boolean reverseValueTransfer = true;
          for (int i = 0; i < srcSids.size(); ++i) {
            if (!hasValueTransfer(destSids.get(i), srcSids.get(i))) {
              reverseValueTransfer = false;
              break;
            }
          }

          // IMPALA-2018/4379: Check if srcConjunct or the generated predicate need to
          // be evaluated again at a later point in the plan, e.g., by a join that makes
          // referenced tuples nullable. The first condition is conservative but takes
          // into account that On-clause conjuncts can sometimes be legitimately assigned
          // below their originating join.
          boolean evalAfterJoin =
              (hasOuterJoinedTuple && !srcConjunct.isOnClauseConjunct_)
              || (evalAfterJoin(srcConjunct)
                  && (globalState_.ojClauseByConjunct.get(srcConjunct.getId())
                    != globalState_.outerJoinedTupleIds.get(srcTid)))
              || (evalAfterJoin(p)
                  && (globalState_.ojClauseByConjunct.get(p.getId())
                    != globalState_.outerJoinedTupleIds.get(destTid)));

          // mark all bound predicates including duplicate ones
          if (reverseValueTransfer && !evalAfterJoin) {
            markConjunctAssigned(srcConjunct);
            if (p != srcConjunct) markConjunctAssigned(p);
          }
        }

        if ((p instanceof BinaryPredicate)
            && ((BinaryPredicate) p).derivedFromBetween()) {
          BinaryPredicate b = (BinaryPredicate) p;
          betweenPredicates.computeIfAbsent(b.getBetweenExprId(), k -> new ArrayList<>());
          betweenPredicates.get(b.getBetweenExprId()).add(b);
        } else {
          // check if we already created this predicate
          if (!result.contains(p)) result.add(p);
        }
      }
    }

    if (!betweenPredicates.isEmpty()) {
      // Prioritize members of 'betweenPredicates' ahead of 'result'.
      // BinaryPredicates that derived from BetweenPredicates may have lower selectivity
      // estimate from BetweenToCompoundRule. Placing them in-front will ensure that
      // they are retained over other matching BinaryPredicates that do not come from
      // BetweenPredicates when passed through Expr.removeDuplicates().
      List<Expr> prioritizedExprs = new ArrayList<>();
      for (List<BinaryPredicate> predicates : betweenPredicates.values()) {
        prioritizedExprs.addAll(predicates);
      }
      prioritizedExprs.addAll(result);
      Expr.removeDuplicates(prioritizedExprs);
      result = prioritizedExprs;
    }
    return result;
  }

  public List<Expr> getBoundPredicates(TupleId destTid) {
    return getBoundPredicates(destTid, new HashSet<>(), true);
  }

  /**
   * Returns true if any of the given slot ids or their value-transfer targets belong
   * to an outer-joined tuple.
   */
  public boolean hasOuterJoinedValueTransferTarget(List<SlotId> sids) {
    for (SlotId srcSid: sids) {
      for (SlotId dstSid: getValueTransferTargets(srcSid)) {
        if (isOuterJoined(getTupleId(dstSid))) return true;
      }
    }
    return false;
  }

  /**
   * For each slot equivalence class, adds/removes predicates from conjuncts such that it
   * contains a minimum set of <lhsSlot> = <rhsSlot> predicates that establish the known
   * equivalences between slots in lhsTids and rhsTids which must be disjoint. Preserves
   * original conjuncts when possible. Assumes that predicates for establishing
   * equivalences among slots in only lhsTids and only rhsTids have already been
   * established. This function adds the remaining predicates to "connect" the disjoint
   * equivalent slot sets of lhsTids and rhsTids.
   * The intent of this function is to enable construction of a minimum spanning tree
   * to cover the known slot equivalences. This function should be called for join
   * nodes during plan generation to (1) remove redundant join predicates, and (2)
   * establish equivalences among slots materialized at that join node.
   * TODO: Consider optimizing for the cheapest minimum set of predicates.
   * TODO: Consider caching the DisjointSet during plan generation instead of
   * re-creating it here on every invocation.
   */
  public void createEquivConjuncts(List<TupleId> lhsTids,
      List<TupleId> rhsTids, List<BinaryPredicate> conjuncts) {
    Preconditions.checkState(Collections.disjoint(lhsTids, rhsTids));
    // A map from equivalence class IDs to equivalence classes. The equivalence classes
    // only contain slots in lhsTids/rhsTids.
    Map<Integer, List<SlotId>> lhsEquivClasses = getEquivClassesOnTuples(lhsTids);
    Map<Integer, List<SlotId>> rhsEquivClasses = getEquivClassesOnTuples(rhsTids);

    // Maps from a slot id to its set of equivalent slots. Used to track equivalences
    // that have been established by predicates assigned/generated to plan nodes
    // materializing lhsTids as well as the given conjuncts.
    DisjointSet<SlotId> partialEquivSlots = new DisjointSet<SlotId>();
    // Add the partial equivalences to the partialEquivSlots map. The equivalent-slot
    // sets of slots from lhsTids are disjoint from those of slots from rhsTids.
    // We need to 'connect' the disjoint slot sets by constructing a new predicate
    // for each equivalence class (unless there is already one in 'conjuncts').
    for (List<SlotId> partialEquivClass: lhsEquivClasses.values()) {
      partialEquivSlots.bulkUnion(partialEquivClass);
    }
    for (List<SlotId> partialEquivClass: rhsEquivClasses.values()) {
      partialEquivSlots.bulkUnion(partialEquivClass);
    }

    // Set of outer-joined slots referenced by conjuncts.
    Set<SlotId> outerJoinedSlots = new HashSet<>();

    // Update partialEquivSlots based on equality predicates in 'conjuncts'. Removes
    // redundant conjuncts, unless they reference outer-joined slots (see below).
    Iterator<BinaryPredicate> conjunctIter = conjuncts.iterator();
    while (conjunctIter.hasNext()) {
      Expr conjunct = conjunctIter.next();
      Pair<SlotId, SlotId> eqSlots = BinaryPredicate.getEqSlots(conjunct);
      if (eqSlots == null) continue;
      int firstEqClassId = getEquivClassId(eqSlots.first);
      int secondEqClassId = getEquivClassId(eqSlots.second);
      // slots may not be in the same eq class due to outer joins
      if (firstEqClassId != secondEqClassId) continue;

      // Retain an otherwise redundant predicate if it references a slot of an
      // outer-joined tuple that is not already referenced by another join predicate
      // to maintain that the rows must satisfy outer-joined-slot IS NOT NULL
      // (otherwise NULL tuples from outer joins could survive).
      // TODO: Consider better fixes for outer-joined slots: (1) Create IS NOT NULL
      // predicates and place them at the lowest possible plan node. (2) Convert outer
      // joins into inner joins (or full outer joins into left/right outer joins).
      boolean filtersOuterJoinNulls = false;
      if (isOuterJoined(eqSlots.first)
          && lhsTids.contains(getTupleId(eqSlots.first))
          && !outerJoinedSlots.contains(eqSlots.first)) {
        outerJoinedSlots.add(eqSlots.first);
        filtersOuterJoinNulls = true;
      }
      if (isOuterJoined(eqSlots.second)
          && lhsTids.contains(getTupleId(eqSlots.second))
          && !outerJoinedSlots.contains(eqSlots.second)) {
        outerJoinedSlots.add(eqSlots.second);
        filtersOuterJoinNulls = true;
      }
      // retain conjunct if it connects two formerly unconnected equiv classes or
      // it is required for outer-join semantics
      if (!partialEquivSlots.union(eqSlots.first, eqSlots.second)
          && !filtersOuterJoinNulls) {
        conjunctIter.remove();
      }
    }

    // For each equivalence class, construct a new predicate to 'connect' the disjoint
    // slot sets.
    for (Map.Entry<Integer, List<SlotId>> rhsEquivClass:
      rhsEquivClasses.entrySet()) {
      List<SlotId> lhsSlots = lhsEquivClasses.get(rhsEquivClass.getKey());
      if (lhsSlots == null) continue;
      List<SlotId> rhsSlots = rhsEquivClass.getValue();
      Preconditions.checkState(!lhsSlots.isEmpty() && !rhsSlots.isEmpty());

      if (!partialEquivSlots.union(lhsSlots.get(0), rhsSlots.get(0))) continue;
      // Do not create a new predicate from slots that are full outer joined because that
      // predicate may be incorrectly assigned to a node below the associated full outer
      // join.
      if (!isFullOuterJoined(lhsSlots.get(0)) && !isFullOuterJoined(rhsSlots.get(0))) {
        conjuncts.add(createInferredEqPred(lhsSlots.get(0), rhsSlots.get(0)));
      }
    }
  }

  /**
   * For each slot equivalence class, adds/removes predicates from conjuncts such that it
   * contains a minimum set of <slot> = <slot> predicates that establish the known
   * equivalences between slots belonging to tid. Preserves original
   * conjuncts when possible.
   * The intent of this function is to enable construction of a minimum spanning tree
   * to cover the known slot equivalences. This function should be called to add
   * conjuncts to plan nodes that materialize a new tuple, e.g., scans and aggregations.
   * Does not enforce equivalence between slots in ignoreSlots. Equivalences (if any)
   * among slots in ignoreSlots are assumed to have already been enforced.
   * TODO: Consider optimizing for the cheapest minimum set of predicates.
   */
  @SuppressWarnings("unchecked")
  public <T extends Expr> void createEquivConjuncts(TupleId tid, List<T> conjuncts,
      Set<SlotId> ignoreSlots) {
    if (LOG.isTraceEnabled()) {
      LOG.trace(String.format(
          "createEquivConjuncts: tid=%s, conjuncts=%s, ignoreSlots=%s", tid.toString(),
          Expr.debugString(conjuncts), ignoreSlots), new Exception("call trace"));
    }
    // Maps from a slot id to its set of equivalent slots. Used to track equivalences
    // that have been established by 'conjuncts' and the 'ignoredsSlots'.
    DisjointSet<SlotId> partialEquivSlots = new DisjointSet<SlotId>();

    // Treat ignored slots as already connected. Add the ignored slots at this point
    // such that redundant conjuncts are removed.
    partialEquivSlots.bulkUnion(ignoreSlots);
    partialEquivSlots.checkConsistency();

    // Update partialEquivSlots based on equality predicates in 'conjuncts'. Removes
    // redundant conjuncts, unless they reference outer-joined slots (see below).
    Iterator<T> conjunctIter = conjuncts.iterator();
    while (conjunctIter.hasNext()) {
      Expr conjunct = conjunctIter.next();
      Pair<SlotId, SlotId> eqSlots = BinaryPredicate.getEqSlots(conjunct);
      if (eqSlots == null) continue;
      int firstEqClassId = getEquivClassId(eqSlots.first);
      int secondEqClassId = getEquivClassId(eqSlots.second);
      // slots may not be in the same eq class due to outer joins
      if (firstEqClassId != secondEqClassId) continue;
      // update equivalences and remove redundant conjuncts
      if (!partialEquivSlots.union(eqSlots.first, eqSlots.second)) {
        conjunctIter.remove();
        if (LOG.isTraceEnabled()) {
          LOG.trace("Removed redundant conjunct: " + conjunct.debugString());
        }
      }
    }
    // For any assigned predicate, union its slots. So we can make sure that slot
    // equivalences are not enforced multiple times.
    if (globalState_.assignedConjunctsByTupleId.containsKey(tid)) {
      List<BinaryPredicate> inferredConjuncts =
          globalState_.assignedConjunctsByTupleId.get(tid);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Previously assigned predicates: " +
            Expr.debugString(inferredConjuncts));
      }
      for (BinaryPredicate conjunct : inferredConjuncts) {
        Pair<SlotId, SlotId> slots = conjunct.getEqSlots();
        if (slots == null) continue;
        partialEquivSlots.union(slots.first, slots.second);
      }
    }
    // Suppose conjuncts had these predicates belonging to equivalence classes e1 and e2:
    // e1: s1 = s2, s3 = s4, s3 = s5
    // e2: s10 = s11
    // The conjunctsEquivSlots should contain the following entries at this point:
    // s1 -> {s1, s2}
    // s2 -> {s1, s2}
    // s3 -> {s3, s4, s5}
    // s4 -> {s3, s4, s5}
    // s5 -> {s3, s4, s5}
    // s10 -> {s10, s11}
    // s11 -> {s10, s11}
    // Assuming e1 = {s1, s2, s3, s4, s5} we need to generate one additional equality
    // predicate to "connect" {s1, s2} and {s3, s4, s5}.

    // These are the equivalences that need to be established by constructing conjuncts
    // to form a minimum spanning tree.
    Map<Integer, List<SlotId>> targetEquivClasses =
        getEquivClassesOnTuples(Lists.newArrayList(tid));
    for (Map.Entry<Integer, List<SlotId>> targetEquivClass:
      targetEquivClasses.entrySet()) {
      // Loop over all pairs of equivalent slots and merge their disjoint slots sets,
      // creating missing equality predicates as necessary.
      List<SlotId> slotIds = targetEquivClass.getValue();
      boolean done = false;
      for (int i = 1; i < slotIds.size(); ++i) {
        SlotId rhs = slotIds.get(i);
        for (int j = 0; j < i; ++j) {
          SlotId lhs = slotIds.get(j);
          if (!partialEquivSlots.union(lhs, rhs)) continue;
          T pred = (T) createInferredEqPred(lhs, rhs);
          conjuncts.add(pred);
          if (LOG.isTraceEnabled()) {
            LOG.trace("Created inferred predicate: " + pred.debugString());
          }
          // Check for early termination.
          if (partialEquivSlots.get(lhs).size() == slotIds.size()) {
            done = true;
            break;
          }
        }
        if (done) break;
      }
    }
  }

  public <T extends Expr> void createEquivConjuncts(TupleId tid, List<T> conjuncts) {
    createEquivConjuncts(tid, conjuncts, new HashSet<>());
  }

  /**
   * Returns a map of slot equivalence classes on the set of slots in the given tuples.
   * Only contains equivalence classes with more than one member.
   */
  private Map<Integer, List<SlotId>> getEquivClassesOnTuples(List<TupleId> tids) {
    Map<Integer, List<SlotId>> result = new HashMap<>();
    SccCondensedGraph g = globalState_.valueTransferGraph;
    for (TupleId tid: tids) {
      for (SlotDescriptor slotDesc: getTupleDesc(tid).getSlots()) {
        if (slotDesc.getId().asInt() >= g.numVertices()) continue;
        int sccId = g.sccId(slotDesc.getId().asInt());
        // Ignore equivalence classes that are empty or only have a single member.
        if (g.sccMembersBySccId(sccId).length <= 1) continue;
        List<SlotId> slotIds = result.get(sccId);
        if (slotIds == null) {
          slotIds = new ArrayList<>();
          result.put(sccId, slotIds);
        }
        slotIds.add(slotDesc.getId());
        if (LOG.isTraceEnabled()) {
          LOG.trace(String.format("slot(%s) -> scc(%d)", slotDesc.getId(), sccId));
        }
      }
    }
    return result;
  }

  /**
   * Returns a list of slot mappings from srcTid to destTid for the purpose of predicate
   * propagation. Each mapping assigns every slot in srcSids to a slot in destTid which
   * has a value transfer from srcSid. Does not generate all possible mappings, but limits
   * the results to useful and/or non-redundant mappings, i.e., those mappings that would
   * improve the performance of query execution.
   */
  private List<List<SlotId>> getValueTransferDestSlotIds(TupleId srcTid,
      List<SlotId> srcSids, TupleId destTid, Set<SlotId> ignoreSlots) {
    List<List<SlotId>> allDestSids = new ArrayList<>();
    TupleDescriptor destTupleDesc = getTupleDesc(destTid);
    if (srcSids.size() == 1) {
      // Generate all mappings to propagate predicates of the form <slot> <op> <constant>
      // to as many destination slots as possible.
      // TODO: If srcTid == destTid we could limit the mapping to partition
      // columns because mappings to non-partition columns do not provide
      // a performance benefit.
      SlotId srcSid = srcSids.get(0);
      for (SlotDescriptor destSlot: destTupleDesc.getSlotsRecursively()) {
        if (ignoreSlots.contains(destSlot.getId())) continue;
        if (hasValueTransfer(srcSid, destSlot.getId())) {
          allDestSids.add(Lists.newArrayList(destSlot.getId()));
        }
      }
    } else if (srcTid.equals(destTid)) {
      // Multiple source slot ids and srcTid == destTid. Inter-tuple transfers are
      // already expressed by the original conjuncts. Any mapping would be redundant.
      // Still add srcSids to the result because we rely on getBoundPredicates() to
      // include predicates that can safely be evaluated below an outer join, but must
      // also be evaluated by the join itself (evalByJoin() == true).
      allDestSids.add(srcSids);
    } else {
      // Multiple source slot ids and srcTid != destTid. Pick the first mapping
      // where each srcSid is mapped to a different destSid to avoid generating
      // redundant and/or trivial predicates.
      // TODO: This approach is not guaranteed to find the best slot mapping
      // (e.g., against partition columns) or all non-redundant mappings.
      // The limitations are show in predicate-propagation.test.
      List<SlotId> destSids = new ArrayList<>();
      for (SlotId srcSid: srcSids) {
        for (SlotDescriptor destSlot: destTupleDesc.getSlotsRecursively()) {
          if (ignoreSlots.contains(destSlot.getId())) continue;
          if (hasValueTransfer(srcSid, destSlot.getId())
              && !destSids.contains(destSlot.getId())) {
            destSids.add(destSlot.getId());
            break;
          }
        }
      }
      if (destSids.size() == srcSids.size()) allDestSids.add(destSids);
    }
    return allDestSids;
  }

  /**
   * Returns true if 'p' evaluates to true when all its referenced slots are NULL,
   * returns false otherwise. Throws if backend expression evaluation fails.
   */
  public boolean isTrueWithNullSlots(Expr p) throws InternalException {
    Expr nullTuplePred = substituteNullSlots(p);
    if (globalState_.nullSlotsCache == null) {
      return FeSupport.EvalPredicate(nullTuplePred, getQueryCtx());
    }

    try {
      return globalState_.nullSlotsCache.get(nullTuplePred,
          () -> FeSupport.EvalPredicate(nullTuplePred, getQueryCtx()));
    } catch (ExecutionException e) {
      Preconditions.checkState(e.getCause() instanceof InternalException,
          "Internal error using null slots cache: %s\nDisable null slots cache with " +
          "the %s query option.", e, TImpalaQueryOptions.USE_NULL_SLOTS_CACHE.name());
      throw (InternalException) e.getCause();
    }
  }

  /**
   * Log hit rate and size of null slots cache.
   */
  public void logCacheStats() {
    if (!LOG.isDebugEnabled() || globalState_.nullSlotsCache == null) return;

    Histogram exprSize = new Histogram(new UniformReservoir());
    MemoryMeter meter = MemoryMeter.builder().build();
    for (Expr expr : globalState_.nullSlotsCache.asMap().keySet()) {
      try {
        exprSize.update(meter.measureDeep(expr));
      } catch (CannotAccessFieldException e) {
        // This may happen if we miss an add-opens call for lambdas in Java 17.
        LOG.warn("Unable to weigh cache entry, additional add-opens needed", e);
      }
    }
    Snapshot snap = exprSize.getSnapshot();
    LOG.debug("null slots cache size: {}, median entry: {}, 99th percentile entry: {}, "+
        "hit rate: {}", globalState_.nullSlotsCache.size(), snap.getMedian(),
        snap.get99thPercentile(), globalState_.nullSlotsCache.stats().hitRate());
  }

  /**
   * Try to evaluate 'p' with all NULL slots into a literal.
   * @return null if it could not be evaluated successfully, the literal otherwise.
   * @throws AnalysisException
   */
  public LiteralExpr evalWithNullSlots(Expr p) throws AnalysisException {
    Expr nullTuplePred = substituteNullSlots(p);
    // can't evaluate an expression that isn't constant.
    if (!nullTuplePred.isConstant()) {
      return null;
    }
    return LiteralExpr.createBounded(
            nullTuplePred, getQueryCtx(), StringLiteral.MAX_STRING_LEN);
  }

  /**
   * Replace all the SlotRefs in 'p' with null literals
   */
  private Expr substituteNullSlots(Expr p) {
    // Construct predicate with all SlotRefs substituted by NullLiterals.
    List<SlotRef> slotRefs = new ArrayList<>();
    p.collect(Predicates.instanceOf(SlotRef.class), slotRefs);

    // Map for substituting SlotRefs with NullLiterals.
    ExprSubstitutionMap nullSmap = new ExprSubstitutionMap();
    for (SlotRef slotRef: slotRefs) {
        // Preserve the original SlotRef type to ensure all substituted
        // subexpressions in the predicate have the same return type and
        // function signature as in the original predicate.
        nullSmap.put(slotRef.clone(), NullLiteral.create(slotRef.getType()));
    }
    return p.substitute(nullSmap, this, false);
  }

  public TupleId getTupleId(SlotId slotId) {
    return globalState_.descTbl.getSlotDesc(slotId).getParent().getId();
  }

  public void registerValueTransfer(SlotId id1, SlotId id2) {
    globalState_.registeredValueTransfers.add(new Pair<SlotId, SlotId>(id1, id2));
  }

  public boolean isOuterJoined(TupleId tid) {
    return globalState_.outerJoinedTupleIds.containsKey(tid);
  }

  public boolean isOuterJoined(SlotId sid) {
    return isOuterJoined(getTupleId(sid));
  }

  public boolean isSemiJoined(TupleId tid) {
    return globalState_.semiJoinedTupleIds.containsKey(tid);
  }

  public boolean isAntiJoinedConjunct(Expr e) {
    return getAntiJoinRef(e) != null;
  }

  public TableRef getAntiJoinRef(Expr e) {
    TableRef tblRef = globalState_.sjClauseByConjunct.get(e.getId());
    if (tblRef == null) return null;
    return (tblRef.getJoinOp().isAntiJoin()) ? tblRef : null;
  }

  public boolean isFullOuterJoined(TupleId tid) {
    return globalState_.fullOuterJoinedTupleIds.containsKey(tid);
  }

  public boolean isFullOuterJoined(SlotId sid) {
    return isFullOuterJoined(getTupleId(sid));
  }

  public boolean isVisible(TupleId tid) {
    return tid == visibleSemiJoinedTupleId_ || !isSemiJoined(tid);
  }

  public boolean containsOuterJoinedTid(List<TupleId> tids) {
    for (TupleId tid: tids) {
      if (isOuterJoined(tid)) return true;
    }
    return false;
  }

  /**
   * Compute the value transfer graph based on the registered value transfers and eq-join
   * predicates.
   */
  public void computeValueTransferGraph() {
    if (LOG.isTraceEnabled()) {
      LOG.trace("All slots: " + SlotDescriptor.debugString(
          globalState_.descTbl.getSlotDescs()));
    }
    WritableGraph directValueTransferGraph =
        new WritableGraph(globalState_.descTbl.getMaxSlotId().asInt() + 1);
    constructValueTransfersFromEqPredicates(directValueTransferGraph);
    for (Pair<SlotId, SlotId> p : globalState_.registeredValueTransfers) {
      directValueTransferGraph.addEdge(p.first.asInt(), p.second.asInt());
      if (LOG.isTraceEnabled()) {
        LOG.trace("value transfer: from " + p.first.toString() + " to " +
            p.second.toString());
      }
    }
    globalState_.valueTransferGraph =
        SccCondensedGraph.condensedReflexiveTransitiveClosure(directValueTransferGraph);
    // Validate the value-transfer graph in single-node planner tests.
    if (RuntimeEnv.INSTANCE.isTestEnv() && getQueryOptions().num_nodes == 1) {
      RandomAccessibleGraph reference =
          directValueTransferGraph.toRandomAccessible().reflexiveTransitiveClosure();
      if (!globalState_.valueTransferGraph.validate(reference)) {
        String tc = reference.print();
        String condensedTc = globalState_.valueTransferGraph.print();
        throw new IllegalStateException("Condensed transitive closure doesn't equal to "
            + "uncondensed transitive closure. Uncondensed Graph:\n" + tc +
            "\nCondensed Graph:\n" + condensedTc);
      }
    }
  }

  /**
   * Add value-transfer edges to 'g' based on the registered equi-join conjuncts.
   */
  private void constructValueTransfersFromEqPredicates(WritableGraph g) {
    for (ExprId id : globalState_.conjuncts.keySet()) {
      Expr e = globalState_.conjuncts.get(id);
      Pair<SlotId, SlotId> slotIds = BinaryPredicate.getEqSlots(e);
      if (slotIds == null) continue;

      TableRef sjTblRef = globalState_.sjClauseByConjunct.get(id);
      Preconditions.checkState(sjTblRef == null || sjTblRef.getJoinOp().isSemiJoin());
      boolean isAntiJoin = sjTblRef != null && sjTblRef.getJoinOp().isAntiJoin();

      TableRef ojTblRef = globalState_.ojClauseByConjunct.get(id);
      Preconditions.checkState(ojTblRef == null || ojTblRef.getJoinOp().isOuterJoin());
      if (ojTblRef == null && !isAntiJoin) {
        // this eq predicate doesn't involve any outer or anti join, ie, it is true for
        // each result row;
        // value transfer is not legal if the receiving slot is in an enclosed
        // scope of the source slot and the receiving slot's block has a limit
        Analyzer firstBlock = globalState_.blockBySlot.get(slotIds.first);
        Analyzer secondBlock = globalState_.blockBySlot.get(slotIds.second);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Considering value transfer between " + slotIds.first.toString() +
              " and " + slotIds.second.toString());
        }
        if (!(secondBlock.hasLimitOffsetClause_ &&
            secondBlock.ancestors_.contains(firstBlock))) {
          g.addEdge(slotIds.first.asInt(), slotIds.second.asInt());
          if (LOG.isTraceEnabled()) {
            LOG.trace("value transfer: from " + slotIds.first.toString() + " to " +
                slotIds.second.toString());
          }
        }
        if (!(firstBlock.hasLimitOffsetClause_ &&
            firstBlock.ancestors_.contains(secondBlock))) {
          g.addEdge(slotIds.second.asInt(), slotIds.first.asInt());
          if (LOG.isTraceEnabled()) {
            LOG.trace("value transfer: from " + slotIds.second.toString() + " to " +
                    slotIds.first.toString());
          }
        }
        continue;
      }
      // Outer or semi-joined table ref.
      TableRef tblRef = (ojTblRef != null) ? ojTblRef : sjTblRef;
      Preconditions.checkNotNull(tblRef);

      if (tblRef.getJoinOp() == JoinOperator.FULL_OUTER_JOIN) {
        // full outer joins don't guarantee any value transfer
        continue;
      }

      // this is some form of outer or anti join
      SlotId outerSlot, innerSlot;
      if (tblRef.getId() == getTupleId(slotIds.first)) {
        innerSlot = slotIds.first;
        outerSlot = slotIds.second;
      } else if (tblRef.getId() == getTupleId(slotIds.second)) {
        innerSlot = slotIds.second;
        outerSlot = slotIds.first;
      } else {
        // this eq predicate is part of an OJ/AJ clause but doesn't reference
        // the joined table -> ignore this, we can't reason about when it'll
        // actually be true
        continue;
      }
      // value transfer is always legal because the outer and inner slot must come from
      // the same block; transitive value transfers into inline views with a limit are
      // prevented because the inline view's aux predicates won't transfer values into
      // the inline view's block (handled in the 'tableRef == null' case above)
      // TODO: We could propagate predicates into anti-joined plan subtrees by
      // inverting the condition (paying special attention to NULLs).
      if (tblRef.getJoinOp() == JoinOperator.LEFT_OUTER_JOIN
          || tblRef.getJoinOp() == JoinOperator.LEFT_ANTI_JOIN
          || tblRef.getJoinOp() == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) {
        g.addEdge(outerSlot.asInt(), innerSlot.asInt());
      } else if (tblRef.getJoinOp() == JoinOperator.RIGHT_OUTER_JOIN
          || tblRef.getJoinOp() == JoinOperator.RIGHT_ANTI_JOIN) {
        g.addEdge(innerSlot.asInt(), outerSlot.asInt());
      }
      if (tblRef.getJoinOp() == JoinOperator.LEFT_OUTER_JOIN ||
          tblRef.getJoinOp() == JoinOperator.RIGHT_OUTER_JOIN) {
        globalState_.ojNullableSlotsInEquiPreds.add(innerSlot);
      }
    }
  }


  /**
   * Returns the equivalence class of the given slot id.
   * Time complexity: O(V) where V = number of slots
   */
  public List<SlotId> getEquivClass(SlotId sid) {
    SccCondensedGraph g = globalState_.valueTransferGraph;
    if (sid.asInt() >= g.numVertices()) return Collections.singletonList(sid);
    List<SlotId> result = new ArrayList<>();
    for (int dst: g.sccMembersByVid(sid.asInt())) {
      result.add(new SlotId(dst));
    }
    return result;
  }

  /**
   * Returns sorted slot IDs with value transfers from 'srcSid'.
   * Time complexity: O(V) where V = number of slots
   */
  public List<SlotId> getValueTransferTargets(SlotId srcSid) {
    SccCondensedGraph g = globalState_.valueTransferGraph;
    if (srcSid.asInt() >= g.numVertices()) return Collections.singletonList(srcSid);
    List<SlotId> result = new ArrayList<>();
    for (IntIterator dstIt = g.dstIter(srcSid.asInt()); dstIt.hasNext(); dstIt.next()) {
      result.add(new SlotId(dstIt.peek()));
    }
    // Unsorted result drastically changes the runtime filter assignment and results in
    // worse plan.
    // TODO: Investigate the call sites and remove this sort.
    Collections.sort(result);
    return result;
  }

  /** Get the id of the equivalence class of the given slot. */
  private int getEquivClassId(SlotId sid) {
    SccCondensedGraph g = globalState_.valueTransferGraph;
    return sid.asInt() >= g.numVertices() ?
        sid.asInt() : g.sccId(sid.asInt());
  }

  /**
   * Returns whether there is a value transfer between two SlotRefs.
   * It's used for {@link Expr#matches(Expr, SlotRef.Comparator)} )}
   */
  private final SlotRef.Comparator VALUE_TRANSFER_SLOTREF_CMP = new SlotRef.Comparator() {
      @Override
      public boolean matches(SlotRef a, SlotRef b) {
        return hasValueTransfer(a.getSlotId(), b.getSlotId());
      }
    };

  /**
   * Returns whether there is a mutual value transfer between two SlotRefs.
   * It's used for {@link Expr#matches(Expr, SlotRef.Comparator)} )}
   */
  private final SlotRef.Comparator MUTUAL_VALUE_TRANSFER_SLOTREF_CMP =
      new SlotRef.Comparator() {
        @Override
        public boolean matches(SlotRef a, SlotRef b) {
          return hasMutualValueTransfer(a.getSlotId(), b.getSlotId());
        }
      };

  /**
   * Returns if e1 has (mutual) value transfer to e2. An expr e1 has value transfer to e2
   * if the tree structure of the two exprs are the same ignoring implicit casts, and for
   * each pair of corresponding slots there is a value transfer from the slot in e1 to the
   * slot in e2.
   */
  public boolean exprsHaveValueTransfer(Expr e1, Expr e2, boolean mutual) {
    return e1.matches(e2, mutual ?
        MUTUAL_VALUE_TRANSFER_SLOTREF_CMP : VALUE_TRANSFER_SLOTREF_CMP);
  }

  /**
   * Return true if two sets of exprs have (mutual) value transfer. Set l1 has value
   * transfer to set s2 there is 1-to-1 value transfer between exprs in l1 and l2.
   */
  public boolean setsHaveValueTransfer(List<Expr> l1, List<Expr> l2, boolean mutual) {
    l1 = Expr.removeDuplicates(l1, MUTUAL_VALUE_TRANSFER_SLOTREF_CMP);
    l2 = Expr.removeDuplicates(l2, MUTUAL_VALUE_TRANSFER_SLOTREF_CMP);
    if (l1.size() != l2.size()) return false;
    for (Expr e2 : l2) {
      boolean foundInL1 = false;
      for (Expr e1 : l1) {
        if (e1.matches(e2, mutual ?
            MUTUAL_VALUE_TRANSFER_SLOTREF_CMP : VALUE_TRANSFER_SLOTREF_CMP)) {
          foundInL1 = true;
          break;
        }
      }
      if (!foundInL1) return false;
    }
    return true;
  }

  /**
   * Compute the intersection of l1 and l2. Two exprs are considered identical if they
   * have mutual value transfer. Return the intersecting l1 elements in i1 and the
   * intersecting l2 elements in i2.
   */
  public void exprIntersect(List<Expr> l1, List<Expr> l2, List<Expr> i1, List<Expr> i2) {
    i1.clear();
    i2.clear();
    for (Expr e1 : l1) {
      for (Expr e2 : l2) {
        if (e1.matches(e2, MUTUAL_VALUE_TRANSFER_SLOTREF_CMP)) {
          i1.add(e1);
          i2.add(e2);
          break;
        }
      }
    }
  }

  /**
   * Mark predicates as assigned.
   */
  public void markConjunctsAssigned(List<Expr> conjuncts) {
    if (conjuncts == null || conjuncts.isEmpty()) return;
    for (Expr p: conjuncts) markConjunctAssigned(p);
  }

  /**
   * Mark predicate as assigned.
   */
  public void markConjunctAssigned(Expr conjunct) {
    globalState_.assignedConjuncts.add(conjunct.getId());
    if (Predicate.isEquivalencePredicate(conjunct)) {
      BinaryPredicate binaryPred = (BinaryPredicate) conjunct;
      List<TupleId> tupleIds = new ArrayList<>();
      List<SlotId> slotIds = new ArrayList<>();
      binaryPred.getIds(tupleIds, slotIds);
      if (tupleIds.size() == 1 && slotIds.size() == 2
          && binaryPred.getEqSlots() != null) {
        // keep assigned predicates that bounds in a tuple
        TupleId tupleId = tupleIds.get(0);
        if (!globalState_.assignedConjunctsByTupleId.containsKey(tupleId)) {
          globalState_.assignedConjunctsByTupleId.put(tupleId, new ArrayList<>());
        }
        globalState_.assignedConjunctsByTupleId.get(tupleId).add(binaryPred);
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Assigned " + conjunct.debugString());
    }
  }

  public Set<ExprId> getAssignedConjuncts() {
    return Sets.newHashSet(globalState_.assignedConjuncts);
  }

  public void setAssignedConjuncts(Set<ExprId> assigned) {
    globalState_.assignedConjuncts = Sets.newHashSet(assigned);
  }

  /**
   * Mark all slots that are referenced in exprs as materialized.
   * Return the affected Tuples.
   */
  public Set<TupleDescriptor> materializeSlots(List<Expr> exprs) {
    List<SlotId> slotIds = new ArrayList<>();
    for (Expr e: exprs) {
      Preconditions.checkState(e.isAnalyzed());
      e.getIds(null, slotIds);
    }
    return globalState_.descTbl.markSlotsMaterialized(slotIds);
  }

  public Set<TupleDescriptor> materializeSlots(Expr e) {
    List<SlotId> slotIds = new ArrayList<>();
    Preconditions.checkState(e.isAnalyzed());
    e.getIds(null, slotIds);
    return globalState_.descTbl.markSlotsMaterialized(slotIds);
  }

  /**
   * Returns assignment-compatible type of expr.getType() and lastCompatibleType.
   * If lastCompatibleType is null, returns expr.getType() (if valid).
   * If types are not compatible throws an exception reporting
   * the incompatible types and their expr.toSql().
   *
   * lastCompatibleExpr is passed for error reporting purposes,
   * but note that lastCompatibleExpr may not yet have lastCompatibleType,
   * because it was not cast yet.
   */
  public Type getCompatibleType(Type lastCompatibleType,
      Expr lastCompatibleExpr, Expr expr)
      throws AnalysisException {
    return getCompatibleType(
        lastCompatibleType, lastCompatibleExpr, expr, getRegularCompatibilityLevel());
  }

  public static Type getCompatibleType(Type leftType, Expr leftExpr, Type rightType,
      Expr rightExpr, TypeCompatibility compatibility) throws AnalysisException {
    Type compatibleType =
        Type.getAssignmentCompatibleType(leftType, rightType, compatibility);
    checkCompatibility(
        leftType, leftExpr, rightType, rightExpr, compatibleType, compatibility);
    return compatibleType;
  }

  public static Type getCompatibleType(Type lastCompatibleType, Expr lastCompatibleExpr,
      Expr expr, TypeCompatibility compatibility) throws AnalysisException {
    Type newCompatibleType;
    if (lastCompatibleType == null) {
      newCompatibleType = expr.getType();
    } else {
      newCompatibleType = Type.getAssignmentCompatibleType(
          lastCompatibleType, expr.getType(), compatibility);
    }
    checkCompatibility(lastCompatibleType, lastCompatibleExpr, expr.getType(), expr,
        newCompatibleType, compatibility);
    return newCompatibleType;
  }

  private static void checkCompatibility(Type leftType, Expr leftExpr, Type rightType,
      Expr rightExpr, Type compatibleType, TypeCompatibility compatibility)
      throws AnalysisException {
    if (compatibility.isUnsafe()) {
      Optional<Expr> nonConstExpr = leftExpr.getFirstNonConstSourceExpr();
      if (!nonConstExpr.isPresent()) {
        nonConstExpr = rightExpr.getFirstNonConstSourceExpr();
      }
      if (nonConstExpr.isPresent()) {
        throw new AnalysisException(String.format(
            "Unsafe implicit cast is prohibited for non-const expression: %s",
            nonConstExpr.get().toSql()));
      }
    }
    if (!compatibleType.isValid()) {
      throw new AnalysisException(
          String.format("Incompatible return types '%s' and '%s' of exprs '%s' and '%s'.",
              leftType.toSql(), rightType.toSql(), leftExpr.toSql(), rightExpr.toSql()));
    }
  }

  /**
   * Determines compatible type for given exprs, and casts them to compatible type.
   * Calls analyze() on each of the exprs.
   * Throws an AnalysisException if the types are incompatible,
   */
  public void castAllToCompatibleType(List<Expr> exprs) throws AnalysisException {
    // Group all the decimal types together at the end of the list to avoid comparing
    // the decimals with each other first. For example, if we have the following list,
    // [decimal, decimal, double], we will end up casting everything to a double anyways,
    // so it does not matter if the decimals are not compatible with each other.
    //
    // We need to create a new sorted list instead of mutating it when sorting it because
    // mutating the original exprs will change the order of the original exprs.
    List<Expr> sortedExprs = new ArrayList<>(exprs);
    Collections.sort(sortedExprs, new Comparator<Expr>() {
      @Override
      public int compare(Expr expr1, Expr expr2) {
        if ((expr1.getType().isDecimal() && expr2.getType().isDecimal()) ||
            (!expr1.getType().isDecimal() && !expr2.getType().isDecimal())) {
          return 0;
        }
        return expr1.getType().isDecimal() ? 1 : -1;
      }
    });
    Expr lastCompatibleExpr = sortedExprs.get(0);
    Type compatibleType = null;
    for (int i = 0; i < sortedExprs.size(); ++i) {
      sortedExprs.get(i).analyze(this);
      compatibleType = getCompatibleType(compatibleType, lastCompatibleExpr,
          sortedExprs.get(i));
    }
    // Add implicit casts if necessary.
    for (int i = 0; i < exprs.size(); ++i) {
      if (!exprs.get(i).getType().equals(compatibleType)) {
        Expr castExpr = exprs.get(i).castTo(compatibleType);
        exprs.set(i, castExpr);
      }
    }
  }

  /**
   * Casts the exprs in the given lists position-by-position such that for every i,
   * the i-th expr among all expr lists is compatible.
   * Returns a list of exprs such that for every i-th expr in that list, it is the first
   * widest compatible expression encountered among all i-th exprs in the expr lists.
   * Returns null if an empty expression list or null is passed to it.
   * Throw an AnalysisException if the types are incompatible.
   *
   * If 'avoidLossyCharPadding' is true, then if a column contains only CHAR values but
   * the lengths are different, they are cast to VARCHAR so the common type will not be a
   * CHAR type, which would cause padding. See IMPALA-10753.
   */
  public List<Expr> castToSetOpCompatibleTypes(List<List<Expr>> exprLists,
      boolean avoidLossyCharPadding) throws AnalysisException {
    if (exprLists == null || exprLists.size() == 0) return null;
    if (exprLists.size() == 1) return exprLists.get(0);

    TypeCompatibility regularCompatibility = this.getRegularCompatibilityLevel();
    TypeCompatibility permissiveCompatibility = this.getPermissiveCompatibilityLevel();

    // Determine compatible types for exprs, position by position.
    List<Expr> firstList = exprLists.get(0);
    List<Expr> widestExprs = new ArrayList<>(firstList.size());
    for (int i = 0; i < firstList.size(); ++i) {
      TypeCompatibility compatibilityLevel = regularCompatibility;
      // Type compatible with the i-th exprs of all expr lists.
      // Initialize with type of i-th expr in first list.
      Type compatibleType = firstList.get(i).getType();
      if (firstList.get(i) instanceof SlotRef &&
          compatibleType.containsStruct()) {
        throw new AnalysisException(String.format(
            "Set operations don't support STRUCT types or types containing " +
            "STRUCT types. %s in %s.", compatibleType.toSql(), firstList.get(i).toSql()));
      }
      widestExprs.add(firstList.get(i));

      for (int j = 1; j < exprLists.size(); ++j) {
        Preconditions.checkState(exprLists.get(j).size() == firstList.size());
        Type preType = compatibleType;
        Expr expr = exprLists.get(j).get(i);

        try {
          compatibleType = getCompatibleType(
              compatibleType, widestExprs.get(i), expr, compatibilityLevel);
        } catch (AnalysisException e) {
          compatibilityLevel = permissiveCompatibility;
          if (permissiveCompatibility.isUnsafe()) {
            compatibleType = getCompatibleType(
                compatibleType, widestExprs.get(i), expr, compatibilityLevel);
          } else {
            throw e;
          }
        }

        // compatibleType will be updated if a new wider type is encountered
        if (preType != compatibleType) {
          if (avoidLossyCharPadding && differentLenCharTypes(preType, compatibleType)) {
            // ALLOW_UNSAFE_CASTS and VALUES_STMT_AVOID_LOSSY_CHAR_PADDING don't work well
            // together now. With ALLOW_UNSAFE_CASTS it is possible that 'compatibleType'
            // alternates between integer and CHAR types, so we don't detect that we
            // should change to VARCHAR (when VALUES_STMT_AVOID_LOSSY_CHAR_PADDING is
            // true). Having these two query options at the same time is disabled in
            // 'SetOperationStmt.analyze()'.
            Preconditions.checkState(!compatibilityLevel.isUnsafe());

            Preconditions.checkState(compatibleType.isChar());
            int length = ((ScalarType) compatibleType).getLength();
            compatibleType = ScalarType.createVarcharType(length);
            expr = expr.castTo(compatibleType, compatibilityLevel);
          }

          widestExprs.set(i, expr);
        }
      }
      // Now that we've found a compatible type, add implicit casts if necessary.
      for (int j = 0; j < exprLists.size(); ++j) {
        // Checking compatibility with every expression
        Expr expr = exprLists.get(j).get(i);
        Type commonType = getCompatibleType(
            expr.getType(), expr, compatibleType, widestExprs.get(i), compatibilityLevel);
        Preconditions.checkState(commonType.equals(compatibleType));
        if (!expr.getType().equals(compatibleType)) {
          Expr castExpr = expr.castTo(compatibleType, compatibilityLevel);
          exprLists.get(j).set(i, castExpr);
        }
      }
    }
    return widestExprs;
  }

  public String getDefaultDb() { return globalState_.queryCtx.session.database; }

  public String getFallbackDbForFunctions() {
    return getQueryCtx()
        .getClient_request()
        .getQuery_options()
        .getFallback_db_for_functions();
  }

  public User getUser() { return user_; }
  public String getUserShortName() throws AnalysisException {
    try {
      return getUser().getShortName();
    } catch (InternalException e) {
      throw new AnalysisException("Could not get the shortened name for user: " +
          getUser().getName(), e);
    }
  }

  public TQueryCtx getQueryCtx() { return globalState_.queryCtx; }
  public TQueryOptions getQueryOptions() {
    return globalState_.queryCtx.client_request.getQuery_options();
  }
  public boolean isDecimalV2() { return getQueryOptions().isDecimal_v2(); }

  /**
   * This method returns the compatibility level depending on Decimal V2 query option. The
   * unsafe compatibility level is not considered in this method. IMPALA-10173 introduces
   * UNSAFE type compatibility just for set operations and insert statements, therefore
   * any other unaffected parts must use this method to decide the compatibility level.
   * The method applies a transformation to 'compatibility', returns a compatibility level
   * that will carry the strict decimal meaning (isStrictDecimal() returns true for it)
   * considering the Decimal V2 query option.
   */
  public TypeCompatibility getRegularCompatibilityLevel(TypeCompatibility compatibility) {
    TQueryOptions queryOptions = getQueryOptions();
    if (queryOptions.isDecimal_v2()) {
      return TypeCompatibility.applyStrictDecimal(compatibility);
    }
    return compatibility;
  }

  public TypeCompatibility getRegularCompatibilityLevel() {
    return getRegularCompatibilityLevel(TypeCompatibility.DEFAULT);
  }

  /**
   * This method returns compatibility level depending on both Decimal V2 and unsafe casts
   * query option. It allows UNSAFE type compatibility for set operation and insert
   * statements.
   */
  public TypeCompatibility getPermissiveCompatibilityLevel() {
    TQueryOptions queryOptions = getQueryOptions();
    if (queryOptions.isAllow_unsafe_casts()) {
      return TypeCompatibility.UNSAFE;
    }
    return getRegularCompatibilityLevel(TypeCompatibility.DEFAULT);
  }
  public AuthorizationFactory getAuthzFactory() { return globalState_.authzFactory; }
  public AuthorizationConfig getAuthzConfig() {
    return getAuthzFactory().getAuthorizationConfig();
  }
  public AuthorizationContext getAuthzCtx() { return globalState_.authzCtx; }
  public boolean isAuthzEnabled() { return getAuthzConfig().isEnabled(); }
  public ListMap<TNetworkAddress> getHostIndex() { return globalState_.hostIndex; }
  public ColumnLineageGraph getColumnLineageGraph() { return globalState_.lineageGraph; }
  public TLineageGraph getThriftSerializedLineageGraph() {
    Preconditions.checkNotNull(globalState_.lineageGraph);
    return globalState_.lineageGraph.toThrift();
  }

  public ImmutableList<PrivilegeRequest> getPrivilegeReqs() {
    return ImmutableList.copyOf(globalState_.privilegeReqs);
  }

  public ImmutableList<Pair<PrivilegeRequest, String>> getMaskedPrivilegeReqs() {
    return ImmutableList.copyOf(globalState_.maskedPrivilegeReqs);
  }

  /**
   * Returns a list of the successful catalog object access events. Does not include
   * accesses that failed due to AuthorizationExceptions. In general, if analysis
   * fails for any reason this list may be incomplete.
   */
  public Set<TAccessEvent> getAccessEvents() { return globalState_.accessEvents; }
  public void addAccessEvent(TAccessEvent event) {
    // We convert 'event.name' to lowercase to avoid duplicate access events since a
    // statement could possibly result in two calls to addAccessEvent(), e.g.,
    // COMPUTE STATS.
    event.name = event.name.toLowerCase();
    globalState_.accessEvents.add(event);
  }

  /**
   * Returns the Table for the given TableName from the 'stmtTableCache' in the global
   * analysis state.
   * Throws an AnalysisException if the database or table does not exist.
   * Throws a TableLoadingException if the registered table failed to load.
   * Does not register authorization requests or access events.
   */
  public FeTable getTable(TableName tblName, boolean mustExist)
      throws AnalysisException, TableLoadingException {
    if (IcebergMetadataTable.isIcebergMetadataTable(tblName.toPath(), this)) {
      return getMetadataVirtualTable(tblName.toPath());
    }
    FeTable table = globalState_.stmtTableCache.tables.get(tblName);
    if (table == null) {
      if (!mustExist) {
        return null;
      }
      if (!globalState_.stmtTableCache.dbs.contains(tblName.getDb())) {
        throw new AnalysisException(DB_DOES_NOT_EXIST_ERROR_MSG + tblName.getDb());
      } else {
        throw new AnalysisException(TBL_DOES_NOT_EXIST_ERROR_MSG + tblName.toString());
      }
    }
    Preconditions.checkState(table.isLoaded(), "table: %s should be loaded", tblName);
    if (table instanceof FeIncompleteTable) {
      // If there were problems loading this table's metadata, throw an exception
      // when it is accessed.
      ImpalaException cause = ((FeIncompleteTable) table).getCause();
      if (cause instanceof TableLoadingException) throw (TableLoadingException) cause;
      throw new TableLoadingException("Missing metadata for table: " + tblName, cause);
    }
    return table;
  }

  /**
   * Wrapper around {@link #getTable(TableName tblName, boolean mustExist)}.
   */
  public FeTable getTable(String dbName, String tableName, boolean mustExist)
      throws AnalysisException, TableLoadingException {
    TableName tblName = new TableName(dbName, tableName);
    return getTable(tblName, mustExist);
  }

  /**
   * Adds auxiliary virtual table for a query.
   */
  public void addVirtualTable(VirtualTable virtTable) {
    TableName tblName = virtTable.getTableName();
    globalState_.stmtTableCache.tables.put(tblName, virtTable);
  }

  /**
   * Retrieves the Iceberg metadata table from the stmtTableCache if the Iceberg metadata
   * table exists or creates and adds it to the stmtTableCache if it does not exist. At
   * this point it is unknown if the base table is loaded for scanning as well, therefore
   * the original table is kept. The metadata table will have its vTbl field filled, while
   * the original table gets a new key without the vTbl field. 'tblRefPath' parameter has
   * to be an IcebergMetadataTable reference path. Returns the the Iceberg metadata table.
   */
  public FeTable getMetadataVirtualTable(List<String> tblRefPath)
      throws AnalysisException {
    Preconditions.checkArgument(
        IcebergMetadataTable.canBeIcebergMetadataTable(tblRefPath));
    try {
      TableName catalogTableName = new TableName(tblRefPath.get(0),
          tblRefPath.get(1));
      TableName virtualTableName = new TableName(tblRefPath.get(0),
          tblRefPath.get(1), tblRefPath.get(2));
      // The catalog table (the base of the virtual table) has been loaded and cached
      // under the name of the virtual table.
      FeTable catalogTable = getStmtTableCache().tables.get(virtualTableName);
      if (catalogTable instanceof IcebergMetadataTable || catalogTable == null) {
        return catalogTable;
      }
      Preconditions.checkState(catalogTable instanceof FeIcebergTable);
      FeIcebergTable catalogIceTable = (FeIcebergTable) catalogTable;
      IcebergMetadataTable virtualTable =
          new IcebergMetadataTable(catalogIceTable, tblRefPath.get(2));
      getStmtTableCache().tables.put(catalogTableName, catalogTable);
      getStmtTableCache().tables.put(virtualTableName, virtualTable);
      return virtualTable;
    } catch (ImpalaRuntimeException e) {
      throw new AnalysisException("Could not create metadata table for table "
          + "reference: " + StringUtils.join(tblRefPath, "."), e);
    }
  }

  public org.apache.kudu.client.KuduTable getKuduTable(FeKuduTable feKuduTable)
      throws AnalysisException {
    String tableName = feKuduTable.getFullName();

    // Use the kuduTable from the global state cache if it exists.
    org.apache.kudu.client.KuduTable kuduTable = globalState_.kuduTables.get(tableName);

    // Otherwise try use the KuduTable from the FeKuduTable if it exists and
    // add it to the global state state cache future use.
    if (kuduTable == null &&
        feKuduTable instanceof LocalKuduTable &&
        ((LocalKuduTable) feKuduTable).getKuduTable() != null) {
      kuduTable = ((LocalKuduTable) feKuduTable).getKuduTable();
      globalState_.kuduTables.put(tableName, kuduTable);
    }

    // Last, get the KuduTable via a request to the Kudu server using openTable and
    // add it to the global state state cache for future use.
    if (kuduTable == null) {
      try {
        KuduClient client = KuduUtil.getKuduClient(feKuduTable.getKuduMasterHosts());
        kuduTable = client.openTable(feKuduTable.getKuduTableName());
        globalState_.kuduTables.put(tableName, kuduTable);
      } catch (Exception ex) {
        throw new AnalysisException("Unable to open the Kudu table: " + tableName, ex);
      }
    }

    return kuduTable;
  }

  /**
   * Returns the table by looking it up in the local Catalog. Returns null if the db/table
   * does not exist. Does *not* force-load the table.
   */
  public FeTable getTableNoThrow(String dbName, String tableName) {
    FeDb db = getCatalog().getDb(dbName);
    if (db == null) return null;
    return db.getTableIfCached(tableName);
  }

  /**
   * Checks if a table exists without registering privileges.
   */
  public boolean tableExists(TableName tblName) {
    Preconditions.checkNotNull(tblName);
    TableName fqTableName = getFqTableName(tblName);
    return globalState_.stmtTableCache.tables.containsKey(fqTableName);
  }

  /**
   * Checks if a database exists without registering privileges.
   */
  public boolean dbExists(String dbName) {
    Preconditions.checkNotNull(dbName);
    return getCatalog().getDb(dbName) != null;
  }

  /**
   * Returns the Table with the given name from the 'loadedTables' map in the global
   * analysis state. Throws an AnalysisException if the table or the db does not exist.
   * Throws a TableLoadingException if the registered table failed to load.
   * When addColumnLevelPrivilege is set to true, always registers privilege request(s)
   * for the columns at the given table.
   * When addColumnLevelPrivilege is set to false, always registers privilege request(s)
   * for the table at the given privilege level(s),
   * regardless of the state of the table (i.e. whether it exists, is loaded, etc.).
   * If addAccessEvent is true adds access event(s) for successfully loaded tables. When
   * multiple privileges are specified, all those privileges will be required for the
   * authorization check.
   */
  public FeTable getTable(TableName tableName, boolean addAccessEvent,
      boolean addColumnPrivilege, Privilege... privilege)
      throws AnalysisException, TableLoadingException {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(privilege);
    TableName fqTableName = getFqTableName(tableName);
    // Get the ownership information if the table exists. We do not want it to throw
    // without registering privileges.
    FeTable table = getTableNoThrow(fqTableName.getDb(), fqTableName.getTbl());
    final String tableOwner = table == null ? null : table.getOwnerUser();
    for (Privilege priv : privilege) {
      if (priv == Privilege.ANY || addColumnPrivilege) {
        registerPrivReq(builder ->
            builder.allOf(priv)
                .onAnyColumn(fqTableName.getDb(), fqTableName.getTbl(), tableOwner)
                .build());
      } else {
        registerPrivReq(builder ->
            builder.allOf(priv)
                .onTable(fqTableName.getDb(), fqTableName.getTbl(), tableOwner)
                .build());
      }
    }
    // Propagate the AnalysisException if the table/db does not exist.
    table = getTable(fqTableName.getDb(), fqTableName.getTbl(), /* must_exist */ true);
    Preconditions.checkNotNull(table);
    if (addAccessEvent) {
      // Add an audit event for this access
      TCatalogObjectType objectType = TCatalogObjectType.TABLE;
      if (table instanceof FeView) objectType = TCatalogObjectType.VIEW;
      for (Privilege priv : privilege) {
        addAccessEvent(new TAccessEvent(fqTableName.toString(), objectType,
            priv.toString()));
      }
    }
    return table;
  }

  /**
   * Returns the Catalog Table object for the TableName at the given Privilege level and
   * adds an audit event if the access was successful.
   *
   * If the user does not have sufficient privileges to access the table an
   * AuthorizationException is thrown.
   * If the table or the db does not exist in the Catalog, an AnalysisError is thrown.
   */
  public FeTable getTable(TableName tableName, Privilege... privilege)
      throws AnalysisException {
    try {
      return getTable(tableName, true, false, privilege);
    } catch (TableLoadingException e) {
      throw new AnalysisException(e);
    }
  }

  /**
   * Sets the addColumnPrivilege to true to add column-level privilege(s) for a given
   * table instead of table-level privilege(s).
   */
  public FeTable getTable(TableName tableName, boolean addColumnPrivilege,
      Privilege... privilege) throws AnalysisException {
    try {
      return getTable(tableName, true, addColumnPrivilege, privilege);
    } catch (TableLoadingException e) {
      throw new AnalysisException(e);
    }
  }

  /**
   * If the database does not exist in the catalog an AnalysisError is thrown.
   * This method does not require the grant option permission.
   */
  public FeDb getDb(String dbName, Privilege privilege) throws AnalysisException {
    return getDb(dbName, privilege, true);
  }

  /**
   * This method does not require the grant option permission.
   */
  public FeDb getDb(String dbName, Privilege privilege, boolean throwIfDoesNotExist)
      throws AnalysisException {
    return getDb(dbName, privilege, throwIfDoesNotExist, false);
  }

  /**
   * Returns the Catalog Db object for the given database at the given
   * Privilege level. The privilege request is tracked in the analyzer
   * and authorized post-analysis.
   *
   * Registers a new access event if the catalog lookup was successful.
   *
   * If throwIfDoesNotExist is set to true and the database does not exist in the catalog
   * an AnalysisError is thrown.
   * If requireGrantOption is set to true, the grant option permission is required for
   * the specified privilege.
   */
  public FeDb getDb(String dbName, Privilege privilege, boolean throwIfDoesNotExist,
      boolean requireGrantOption) throws AnalysisException {
    // Do not throw until the privileges are registered.
    FeDb db = getDb(dbName, /*throwIfDoesNotExist*/ false);
    registerPrivReq(builder -> {
      if (requireGrantOption) {
        builder.grantOption();
      }
      if (privilege == Privilege.ANY) {
        String dbOwner = db == null ? null : db.getOwnerUser();
        return builder.any().onAnyColumn(dbName, dbOwner).build();
      } else if (db == null) {
        // Db does not exist, register a privilege request based on the DB name.
        return builder.allOf(privilege).onDb(dbName, null).build();
      }
      return builder.allOf(privilege).onDb(db).build();
    });
    // Propagate the exception if needed.
    FeDb retDb = getDb(dbName, throwIfDoesNotExist);
    addAccessEvent(new TAccessEvent(dbName, TCatalogObjectType.DATABASE,
        privilege.toString()));
    return retDb;
  }

  /**
   * Returns a Catalog Db object without checking for privileges.
   */
  public FeDb getDb(String dbName, boolean throwIfDoesNotExist)
      throws AnalysisException {
    FeDb db = getCatalog().getDb(dbName);
    if (db == null && throwIfDoesNotExist) {
      throw new AnalysisException(DB_DOES_NOT_EXIST_ERROR_MSG + dbName);
    }
    return db;
  }

  /**
   * Checks if the given database contains the given table for the given Privilege
   * level. If the table exists in the database, true is returned. Otherwise false.
   *
   * If the user does not have sufficient privileges to access the table an
   * AuthorizationException is thrown.
   * If the database does not exist in the catalog an AnalysisError is thrown.
   */
  public boolean dbContainsTable(String dbName, String tableName, Privilege privilege)
      throws AnalysisException {
    try {
      FeDb db = getCatalog().getDb(dbName);
      FeTable table = db == null ? null: db.getTable(tableName);
      if (table != null) {
        // Table exists, register the privilege and pass the right ownership information.
        // Table owners are expected to have ALL privileges on the table object.
        registerPrivReq(builder ->
            builder.allOf(privilege)
                .onTable(table)
                .build());
      } else if (privilege == Privilege.CREATE) {
        // Table does not exist and hence the owner information cannot be deduced.
        // For creating something under this db, we translate the db ownership into
        // having CREATE privilege on tables under it.
        String dbOwnerUser = db == null? null : db.getOwnerUser();
        registerPrivReq(builder ->
          builder.allOf(privilege)
              .onTable(dbName, tableName, dbOwnerUser)
              .build());
      } else {
        // All non-CREATE privileges are checked directly on the table object.
        Preconditions.checkState(table == null && privilege != Privilege.CREATE);
        registerPrivReq(builder ->
          builder.allOf(privilege).onTableUnknownOwner(dbName, tableName).build());
      }
      if (db == null) {
        throw new DatabaseNotFoundException("Database not found: " + dbName);
      }
      return table != null;
    } catch (DatabaseNotFoundException e) {
      throw new AnalysisException(DB_DOES_NOT_EXIST_ERROR_MSG + dbName);
    }
  }

  /**
   * If the table name is fully qualified, the database from the TableName object will
   * be returned. Otherwise the default analyzer database will be returned.
   */
  public String getTargetDbName(TableName tableName) {
    return tableName.isFullyQualified() ? tableName.getDb() : getDefaultDb();
  }

  public String getTargetDbName(FunctionName functionName) {
    return functionName.isFullyQualified() ? functionName.getDb() : getDefaultDb();
  }

  /**
   * Returns the fully-qualified table name of tableName. If tableName
   * is already fully qualified, returns tableName.
   */
  public TableName getFqTableName(TableName tableName) {
    if (tableName.isFullyQualified()) return tableName;
    return new TableName(getDefaultDb(), tableName.getTbl());
  }

  public void setMaskPrivChecks(String errMsg) {
    maskPrivChecks_ = true;
    authErrorMsg_ = errMsg;
  }

  public void setEnablePrivChecks(boolean value) { enablePrivChecks_ = value; }
  public void setIsStraightJoin() { isStraightJoin_ = true; }
  public boolean isStraightJoin() { return isStraightJoin_; }
  public void setIsExplain() { globalState_.isExplain = true; }
  public boolean isExplain() { return globalState_.isExplain; }
  public void setUseHiveColLabels(boolean useHiveColLabels) {
    useHiveColLabels_ = useHiveColLabels;
  }
  public boolean useHiveColLabels() { return useHiveColLabels_; }

  public void setHasLimitOffsetClause(boolean hasLimitOffset) {
    this.hasLimitOffsetClause_ = hasLimitOffset;
  }

  public List<Expr> getConjuncts() {
    return new ArrayList<>(globalState_.conjuncts.values());
  }

  public int incrementCallDepth() { return ++callDepth_; }
  public int decrementCallDepth() { return --callDepth_; }
  public int getCallDepth() { return callDepth_; }

  public int incrementNumStmtExprs() { return globalState_.numStmtExprs_++; }
  public int getNumStmtExprs() { return globalState_.numStmtExprs_; }
  public void checkStmtExprLimit() throws AnalysisException {
    int statementExpressionLimit = getQueryOptions().getStatement_expression_limit();
    if (getNumStmtExprs() > statementExpressionLimit) {
      String errorStr = String.format("Exceeded the statement expression limit (%d)\n" +
          "Statement has %d expressions.", statementExpressionLimit, getNumStmtExprs());
      throw new AnalysisException(errorStr);
    }
  }

  public boolean hasMutualValueTransfer(SlotId a, SlotId b) {
    return hasValueTransfer(a, b) && hasValueTransfer(b, a);
  }

  public boolean hasValueTransfer(SlotId a, SlotId b) {
    SccCondensedGraph g = globalState_.valueTransferGraph;
    return a.equals(b) || (a.asInt() < g.numVertices() && b.asInt() < g.numVertices()
        && g.hasEdge(a.asInt(), b.asInt()));
  }

  public Map<String, FeView> getLocalViews() { return localViews_; }

  /**
   * Add a warning that will be displayed to the user. Ignores null messages. Once
   * getWarnings() has been called, no warning may be added to the Analyzer anymore.
   */
  public void addWarning(String msg) {
    if (checkWarningsRetrieved(msg)) return;

    if (msg == null) return;
    Integer count = globalState_.warnings.get(msg);
    if (count == null) count = 0;
    globalState_.warnings.put(msg, count + 1);
  }

  /**
   * 'addWarning' method may be called after the warnings are retrieved, e.g. in
   * analyzing some substituted/cloned predicates (IMPALA-11021). We need to make sure
   * no new warnings are added after retrieved.
   */
  private boolean checkWarningsRetrieved(String msg) {
    if (globalState_.warningsRetrieved) {
      // Make sure that substituted/cloned predicates' warnings already exists in map.
      Preconditions.checkState(globalState_.warnings.containsKey(msg));
      return true;
    }
    return false;
  }

  /**
   * Registers a new PrivilegeRequest in the analyzer.
   */
  public void registerPrivReq(PrivilegeRequest privReq) {
    if (!enablePrivChecks_) return;
    if (maskPrivChecks_) {
      globalState_.maskedPrivilegeReqs.add(Pair.create(privReq, authErrorMsg_));
    } else {
      globalState_.privilegeReqs.add(privReq);
    }
  }

  /**
   * Registers a new PrivilegeRequest in the analyzer. The given function is used to
   * create a PrivilegeRequest.
   */
  public void registerPrivReq(
      Function<PrivilegeRequestBuilder, PrivilegeRequest> function) {
    registerPrivReq(function.apply(new PrivilegeRequestBuilder(
        getAuthzFactory().getAuthorizableFactory())));
  }

  /**
   * This method does not require the grant option permission.
   */
  public void registerAuthAndAuditEvent(FeTable table, Privilege priv) {
    registerAuthAndAuditEvent(table, priv, false);
  }

  /**
   * Registers a table-level privilege request and an access event for auditing
   * for the given table and privilege. The table must be a base table or a
   * catalog view (not a local view). If requireGrantOption is set to true, the
   * the grant option permission is required for the specified privilege.
   */
  public void registerAuthAndAuditEvent(FeTable table, Privilege priv,
      boolean requireGrantOption) {
    // Add access event for auditing.
    if (table instanceof FeView) {
      FeView view = (FeView) table;
      Preconditions.checkState(!view.isLocalView());
      addAccessEvent(new TAccessEvent(
          table.getFullName(), TCatalogObjectType.VIEW,
          priv.toString()));
    } else {
      addAccessEvent(new TAccessEvent(
          table.getFullName(), TCatalogObjectType.TABLE,
          priv.toString()));
    }
    // Add privilege request.
    registerPrivReq(builder -> {
      builder.onTable(table).allOf(priv);
      if (requireGrantOption) {
        builder.grantOption();
      }
      return builder.build();
    });
  }

  /**
   * Returns the server name if authorization is enabled. Returns null when authorization
   * is not enabled.
   */
  public String getServerName() {
    return isAuthzEnabled() ? getAuthzConfig().getServerName().intern() : null;
  }

  /**
   * Get the where or having conjuncts of the specified table from globalState_.conjuncts
   */
  private List<Expr> getTableConjuncts(TupleId id) {
    List<Expr> result = new ArrayList<>();
    for (Map.Entry<ExprId, Expr> conjunct : globalState_.conjuncts.entrySet()) {
      Expr expr = conjunct.getValue();
      if (expr != null && !expr.isOnClauseConjunct() && !expr.isAuxExpr()) {
        List<TupleId> tids = new ArrayList<>();
        expr.getIds(tids, null);
        if (tids.contains(id)) {
          result.add(expr);
        }
      }
    }
    return result;
  }

  /**
   * Collect the tuple id of the disjunctive conjunct's children which is not disjunctive
   * eg. a > 10 and b > 10 or d < 20, one inner set is the tuple id of 'a > 10
   * and b > 10' and another is the tuple id of 'd < 20'
   */
  private Set<Set<TupleId>> collectTupleIdForDisjunctiveConjuncts(Expr e) {
    Set<Set<TupleId>> set = new HashSet<Set<TupleId>>();
    if (Expr.IS_OR_PREDICATE.apply(e)) {
      set.addAll(collectTupleIdForDisjunctiveConjuncts(e.getChild(0)));
      set.addAll(collectTupleIdForDisjunctiveConjuncts(e.getChild(1)));
    } else {
      List<TupleId> tids = new ArrayList<>();
      e.getIds(tids, null);
      set.add(new HashSet<TupleId>(tids));
    }
    return set;
  }

  /**
   * Return true if the result can have null values when apply e on tupleIds
   */
  private boolean isNullableConjunct(Expr e, List<TupleId> tupleIds) {
    // A clause like "t1.v1 IS NOT NULL OR t2.v2 IS NOT NULL" and t1 in 'tupleIds' does
    // not prove that t1.v1 can't be NULL, because when t2.v2 IS NOT NULL, t1.v1 can be
    // null. But a clause like "t1.v1 IS NOT NULL OR t1.v2 IS NOT NULL" proves that the
    // t1 row as a whole can't be all-NULL.
    List<Expr> orConjuncts = new ArrayList<>();
    e.collectAll(Expr.IS_OR_PREDICATE, orConjuncts);
    for (Expr expr : orConjuncts) {
      Set<Set<TupleId>> childrenTis = collectTupleIdForDisjunctiveConjuncts(expr);
      for (Set<TupleId> tids : childrenTis) {
        tids.retainAll(tupleIds);
        if (tids.isEmpty()) {
          return true;
        }
      }
    }

    // Simply assume that a conjunct contains a UDF, is distinct from/ is not distinct
    // from operator, nondeterministic buitin functions or is null operator, it is not
    // null-rejecting predicate.
    if (e.contains(Predicates.or(Expr.IS_DISTINCT_FROM_OR_NOT_DISTINCT_PREDICATE,
        Expr.IS_NONDETERMINISTIC_BUILTIN_FN_PREDICATE,
        Expr.IS_UDF_PREDICATE, Expr.IS_IS_NULL_PREDICATE))) {
      return true;
    }

    // Predicate contains conditional function, case expr may not null-rejecting.
    List<Expr> maybeNullableExprs = new ArrayList<>();
    e.collectAll(Predicates.or(Expr.IS_CONDITIONAL_BUILTIN_FN_PREDICATE,
        Expr.IS_CASE_EXPR_PREDICATE), maybeNullableExprs);
    if (!maybeNullableExprs.isEmpty()) {
      if (!Expr.IS_BINARY_PREDICATE.apply(e)) return true;
      // For t1 left join t2 on t1.a = t2.a where t2.b > coalesce(t1.c, t2.c) can
      // simplify to an inner join. Simply support the case that one child does not
      // contain conditional builtin function or case expr and has tuple id in outer
      // joined tuples.
      for (Expr operand : e.getChildren()) {
        if (operand instanceof ArithmeticExpr) {
          // 't1.id + coalesce(t1.c, t2.c) > coalesce(t2.c, t1.c)' is null-rejecting
          // predicate for t1
          for (Expr expr : operand.getChildren()) {
            if (noConditionalBuiltinFnOrCaseExpr(expr, tupleIds)) return false;
          }
        } else {
          if (noConditionalBuiltinFnOrCaseExpr(operand, tupleIds)) return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * If the 'e' does not contain conditional builtin function or case expr and has
   * tupleId in 'tupleIds', return true, return false otherwise.
   */
  private boolean noConditionalBuiltinFnOrCaseExpr(Expr e, List<TupleId> tupleIds) {
    List<Expr> nullableExprs = new ArrayList<>();
    e.collectAll(Predicates.or(Expr.IS_CONDITIONAL_BUILTIN_FN_PREDICATE,
        Expr.IS_CASE_EXPR_PREDICATE), nullableExprs);
    if (!nullableExprs.isEmpty()) return false;
    List<TupleId> tids = new ArrayList<>();
    e.getIds(tids, null);
    if (TupleId.intersect(tupleIds, new HashSet<>(tids))) {
      return true;
    }
    return false;
  }

  /**
   * Returns true if at least one of the conjuncts of the tuple 'tupleIds' evaluates to
   * false when all its referenced slots are NULL, returns false otherwise.
   * This method identifies null-rejecting predicates which are the requirements to
   * convert an outer-join to an inner join.
   * For example, t1 left join t2 on t1.id=t2.id where t2.v2>10, 't2.v2>10' is the
   * null-rejecting predicate.
   */
  private boolean hasNullRejectingConjucts(List<TupleId> tupleIds) {
    for (TupleId id : tupleIds) {
      List<Expr> conjuncts = getTableConjuncts(id);
      for (Expr e : conjuncts) {
        // Skip not null-rejecting conjunct
        if (isNullableConjunct(e, tupleIds)) continue;

        try {
          // Check whether 'e' evaluates to true when all its referenced slots are NULL,
          // The false result indicates that 'e' is null-rejecting conjunct.
          if (!isTrueWithNullSlots(e)) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Tuple " + id + " has null rejecting conjunct: "
                  + e.debugString());
            }
            return true;
          }
        } catch (InternalException ex) {
          // Expr evaluation failed in the backend. Skip 'e' since we cannot
          // determine whether it is null-rejecting conjunct.
          LOG.warn("Skipping to check whether the conjunct is null-rejecting because"
              + "backend evaluation failed: " + e.toSql(), ex);
        }
      }
    }
    return false;
  }

  private void removeOuterJoinedTupleIds(List<TupleId> ids) {
    for (TupleId id : ids) {
      globalState_.outerJoinedTupleIds.remove(id);
    }
  }

  /**
   * When transforming an outer join to an inner join is feasible, we need to re-register
   * conjunct to its containing join On clause.
   */
  private void ojToIjOnClauseConjucts(TableRef tblRef) {
    List<ExprId> ojConjuncts = globalState_.conjunctsByOjClause.get(tblRef.getId());
    for (ExprId eid : ojConjuncts) {
      globalState_.ojClauseByConjunct.remove(eid);
      globalState_.ijClauseByConjunct.put(eid, tblRef);
    }
    globalState_.conjunctsByOjClause.remove(tblRef.getId());
  }

  /**
   * When transforming an outer join to a left/right/inner join is feasible,
   * we need to remove outer joined tuple id from globalState_.fullOuterJoinedTupleIds
   * and the conjunct of the last full outer join table from
   * globalState_.fullOuterJoinedConjuncts.
   */
  private void removeFullOuterJoinedTupleIdsAndConjuncts(List<TupleId> ids) {
    for (TupleId id : ids) {
      TableRef ref = globalState_.fullOuterJoinedTupleIds.get(id);
      Iterator<Map.Entry<ExprId, TableRef>> it =
          globalState_.fullOuterJoinedConjuncts.entrySet().iterator();
      while(it.hasNext()){
          Map.Entry<ExprId, TableRef> entry = it.next();
          if(entry.getValue() == ref) it.remove();
      }
      globalState_.fullOuterJoinedTupleIds.remove(id);
    }
  }

  /**
   * When transforming an outer join to an inner join is feasible, we need re-register
   * !empty() predicates for CollectionTableRef.
   * For more info see SelectStmt.registerIsNotEmptyPredicates().
   */
  private void reRegisterIsNotEmptyPredicates(TableRef tblRef) {
    Preconditions.checkState(tblRef.getJoinOp().isInnerJoin());
    if (hasWithClause()) return;
    if (!(tblRef instanceof CollectionTableRef)) return;
    CollectionTableRef ref = (CollectionTableRef) tblRef;
    if (!ref.isRelative() || ref.isCorrelated()) return;
    // Do not generate a predicate if the parent tuple is outer joined.
    if (isOuterJoined(ref.getResolvedPath().getRootDesc().getId())) return;

    try {
      IsNotEmptyPredicate isNotEmptyPred =
          new IsNotEmptyPredicate(ref.getCollectionExpr().clone());
      isNotEmptyPred.analyze(this);
      registerOnClauseConjuncts(Lists.<Expr>newArrayList(isNotEmptyPred), ref);
    } catch (AnalysisException ex) {
      LOG.warn("Re-register the !empty() predicates failed.", ex);
    }
  }

  public boolean hasOuterJoined(List<TableRef> tableRefs) {
    for (TableRef tblRef : tableRefs) {
      if (tblRef.getJoinOp().isOuterJoin()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the outer joined tuple ids that satisfy null-rejecting from the inner join on
   * clause.
   * TODO: Use value transfer graph to get the base table tuple id of the slot is not null
   * and register in globalState_. We use this to simplify outer joins of inline view.
   * eg: t1, (select t3.id c from t2 left join t3 on t1.id = t2.id) t4 where t1.id = t4.c
   */
  private void getNullRejectingOjTidsFromIjOnClause(TableRef tblRef,
      Set<TupleId> nullRejectingTids) {
    List<Expr> onConjuncts = new ArrayList<>();
    for (Map.Entry<ExprId, TableRef> entry : globalState_.ijClauseByConjunct.entrySet()) {
      if (entry.getValue() == tblRef) {
        Expr e = globalState_.conjuncts.get(entry.getKey());
        if (e.isOnClauseConjunct()) {
          onConjuncts.add(e);
        }
      }
    }
    List<TupleId> tids = tblRef.getLeftTblRef().getAllTableRefIds();
    for (Expr e : onConjuncts) {
      if (isNullableConjunct(e, tids)) continue;
      try {
        // Check whether 'e' evaluates to true when all its referenced slots are NULL,
        // The false result indicates that 'e' is null-rejecting conjunct.
        if (!isTrueWithNullSlots(e)) {
          List<TupleId> ids = new ArrayList<>();
          e.getIds(ids, null);
          for (TupleId id : ids) {
            if (isOuterJoined(id)) nullRejectingTids.add(id);
          }
        }
      } catch (InternalException ex) {
        LOG.warn("Skipping the conjunct of on clause because backend evaluation failed: "
            + e.toSql(), ex);
      }
    }
  }

  /**
   * Simplify outer join executing before inner joined table by inner join on clause.
   * For example, t1 left join t2 on t1.id = t2.id join t3 on t2.id = t3.id, we can
   * convert this as t1 join t2 on t1.id = t2.id join t3 on t2.id =  t3.id
   */
  private boolean simplifyOuterJoinsByIjOnClause(List<TableRef> tableRefs,
      TableRef ijTableRef) {
    Set<TupleId> nullRejectingTidSet = new HashSet<>();
    getNullRejectingOjTidsFromIjOnClause(ijTableRef, nullRejectingTidSet);
    if (!nullRejectingTidSet.isEmpty()) {
      return simplifyOuterJoins(tableRefs, nullRejectingTidSet);
    }
    return false;
  }

  /**
   * Attempt to transform outer joins into inner joins.
   * Return true, if has outer join simplification.
   *
   * As a general rule, an outer join can be converted to an inner join if there is a
   * condition on the null-filling table that filters out non‑matching rows. In a left
   * outer join, the right table is the null-filling table, while it is the left table
   * in a right outer join.
   * In a full outer join, both tables are null-filling tables. Conditions that are FALSE
   * for nulls are referred to as null rejecting conditions, and these are the conditions
   * that enable the outer‑to‑inner join conversion to be made.
   *
   * An outer join can be converted to an inner join if the WHERE clause contains at
   * least one null rejecting condition on the inner table.
   */
  public boolean simplifyOuterJoins(List<TableRef> tableRefs,
      Set<TupleId> nullRejectingTids) {
    boolean isSimplified = false;
    List<TableRef> processedTblRefs = new ArrayList<>();
    for (TableRef tableRef : tableRefs) {
      switch (tableRef.getJoinOp()) {
        case INNER_JOIN: {
          if (tableRef.getLeftTblRef() != null) {
            boolean ret = simplifyOuterJoinsByIjOnClause(processedTblRefs, tableRef);
            isSimplified = isSimplified ? true : ret;
          }
          break;
        }
        case LEFT_OUTER_JOIN: {
          TupleId id = tableRef.getId();
          if (nullRejectingTids.contains(id) || hasNullRejectingConjucts(id.asList())) {
            tableRef.setJoinOp(JoinOperator.INNER_JOIN);
            removeOuterJoinedTupleIds(id.asList());
            ojToIjOnClauseConjucts(tableRef);
            reRegisterIsNotEmptyPredicates(tableRef);
            simplifyOuterJoinsByIjOnClause(processedTblRefs, tableRef);
            isSimplified = true;
          }
          break;
        }
        case RIGHT_OUTER_JOIN: {
          List<TupleId> ids = tableRef.getLeftTblRef().getAllTableRefIds();
          // find out all null-rejecting TupleIds in 'ids'
          boolean hasNullRejectingTid = gatherNullRejectingTids(ids, nullRejectingTids);
          if (hasNullRejectingTid || TupleId.intersect(ids, nullRejectingTids) ||
              hasNullRejectingConjucts(ids)) {
            tableRef.setJoinOp(JoinOperator.INNER_JOIN);
            removeOuterJoinedTupleIds(new ArrayList<TupleId>(nullRejectingTids));
            ojToIjOnClauseConjucts(tableRef);
            reRegisterIsNotEmptyPredicates(tableRef);
            simplifyOuterJoinsByIjOnClause(processedTblRefs, tableRef);
            isSimplified = true;
          }
          break;
        }
        case FULL_OUTER_JOIN: {
          List<TupleId> ids = tableRef.getLeftTblRef().getAllTableRefIds();
          // find out all null-rejecting TupleIds in 'ids'
          boolean hasNullRejectingTid = gatherNullRejectingTids(ids, nullRejectingTids);
          if (hasNullRejectingTid || TupleId.intersect(ids, nullRejectingTids) ||
              hasNullRejectingConjucts(ids)) {
            removeFullOuterJoinedTupleIdsAndConjuncts(ids);
            removeFullOuterJoinedTupleIdsAndConjuncts(tableRef.getId().asList());
            if (nullRejectingTids.contains(tableRef.getId()) ||
                hasNullRejectingConjucts(tableRef.getId().asList())) {
              tableRef.setJoinOp(JoinOperator.INNER_JOIN);
              nullRejectingTids.add(tableRef.getId());
              removeOuterJoinedTupleIds(new ArrayList<TupleId>(nullRejectingTids));
              ojToIjOnClauseConjucts(tableRef);
              reRegisterIsNotEmptyPredicates(tableRef);
              simplifyOuterJoinsByIjOnClause(processedTblRefs, tableRef);
            } else {
              tableRef.setJoinOp(JoinOperator.LEFT_OUTER_JOIN);
              removeOuterJoinedTupleIds(new ArrayList<TupleId>(nullRejectingTids));
            }
            isSimplified = true;
          } else if (nullRejectingTids.contains(tableRef.getId()) ||
              hasNullRejectingConjucts(tableRef.getId().asList())) {
            tableRef.setJoinOp(JoinOperator.RIGHT_OUTER_JOIN);
            removeOuterJoinedTupleIds(tableRef.getId().asList());
            isSimplified = true;
          }
          break;
        }
        default:
          break;
      }
      processedTblRefs.add(tableRef);
    }
    return isSimplified;
  }

   /**
   * Get the tuple ids that satisfy null-rejecting from the where or having onjuncts.
   * Return true if has null rejecting tid in tupleIds.
   */
  private boolean gatherNullRejectingTids(List<TupleId> tupleIds,
      Set<TupleId> nullRejectingTids) {
    boolean hasNullRejectingTid = false;
    for (TupleId id : tupleIds) {
      List<Expr> conjuncts = getTableConjuncts(id);
      for (Expr e : conjuncts) {
        // Skip not null-rejecting conjunct
        if (isNullableConjunct(e, tupleIds)) continue;

        try {
          // Check whether 'e' evaluates to true when all its referenced slots are NULL,
          // The false result indicates that 'e' is null-rejecting conjunct.
          if (!isTrueWithNullSlots(e)) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Tuple " + id + " has null rejecting conjunct: "
                  + e.debugString());
            }
            nullRejectingTids.add(id);
            hasNullRejectingTid = true;
            break;
          }
        } catch (InternalException ex) {
          // Expr evaluation failed in the backend. Skip 'e' since we cannot
          // determine whether it is null-rejecting conjunct.
          LOG.warn("Fail to verify " + e.toSql() + " being null-rejecting because of the"
              + " backend evaluation failure", ex);
        }
      }
    }
    return hasNullRejectingTid;
  }

  private static boolean differentLenCharTypes(Type t1, Type t2) {
    if (!t1.isChar() || !t2.isChar()) return false;
    ScalarType t1Scalar = (ScalarType) t1;
    ScalarType t2Scalar = (ScalarType) t2;
    return t1Scalar.getLength() != t2Scalar.getLength();
  }
}
