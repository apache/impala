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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.analysis.StmtMetadataLoader.StmtTableCache;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizeableTable;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.PrivilegeRequestBuilder;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.ImpaladCatalog;
import org.apache.impala.catalog.IncompleteTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.View;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.IdGenerator;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.rewrite.BetweenToCompoundRule;
import org.apache.impala.rewrite.EqualityDisjunctsToInRule;
import org.apache.impala.rewrite.ExprRewriteRule;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.rewrite.ExtractCommonConjunctRule;
import org.apache.impala.rewrite.FoldConstantsRule;
import org.apache.impala.rewrite.NormalizeBinaryPredicatesRule;
import org.apache.impala.rewrite.NormalizeCountStarRule;
import org.apache.impala.rewrite.NormalizeExprsRule;
import org.apache.impala.rewrite.RemoveRedundantStringCast;
import org.apache.impala.rewrite.SimplifyConditionalsRule;
import org.apache.impala.rewrite.SimplifyDistinctFromRule;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TAccessEvent;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TLineageGraph;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.DisjointSet;
import org.apache.impala.util.Graph.RandomAccessibleGraph;
import org.apache.impala.util.Graph.SccCondensedGraph;
import org.apache.impala.util.Graph.WritableGraph;
import org.apache.impala.util.IntIterator;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.TSessionStateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
  // Common analysis error messages
  public final static String DB_DOES_NOT_EXIST_ERROR_MSG = "Database does not exist: ";
  public final static String DB_ALREADY_EXISTS_ERROR_MSG = "Database already exists: ";
  public final static String TBL_DOES_NOT_EXIST_ERROR_MSG = "Table does not exist: ";
  public final static String TBL_ALREADY_EXISTS_ERROR_MSG = "Table already exists: ";
  public final static String FN_DOES_NOT_EXIST_ERROR_MSG = "Function does not exist: ";
  public final static String FN_ALREADY_EXISTS_ERROR_MSG = "Function already exists: ";
  public final static String DATA_SRC_DOES_NOT_EXIST_ERROR_MSG =
      "Data source does not exist: ";
  public final static String DATA_SRC_ALREADY_EXISTS_ERROR_MSG =
      "Data source already exists: ";

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

  // Flag indicating whether this analyzer belongs to a WITH clause view.
  private boolean isWithClause_ = false;

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

  public void setIsSubquery() {
    isSubquery_ = true;
    globalState_.containsSubquery = true;
  }
  public boolean setHasPlanHints() { return globalState_.hasPlanHints = true; }
  public boolean hasPlanHints() { return globalState_.hasPlanHints; }
  public void setIsWithClause() { isWithClause_ = true; }
  public boolean isWithClause() { return isWithClause_; }

  // State shared between all objects of an Analyzer tree. We use LinkedHashMap and
  // LinkedHashSet where applicable to preserve the iteration order and make the class
  // behave identical across different implementations of the JVM.
  // TODO: Many maps here contain properties about tuples, e.g., whether
  // a tuple is outer/semi joined, etc. Remove the maps in favor of making
  // them properties of the tuple descriptor itself.
  private static class GlobalState {
    public final TQueryCtx queryCtx;
    public final AuthorizationConfig authzConfig;
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

    // all registered conjuncts (map from expr id to conjunct). We use a LinkedHashMap to
    // preserve the order in which conjuncts are added.
    public final LinkedHashMap<ExprId, Expr> conjuncts = Maps.newLinkedHashMap();

    // all registered conjuncts bound by a single tuple id; used in getBoundPredicates()
    public final ArrayList<ExprId> singleTidConjuncts = Lists.newArrayList();

    // eqJoinConjuncts[tid] contains all conjuncts of the form
    // "<lhs> = <rhs>" in which either lhs or rhs is fully bound by tid
    // and the other side is not bound by tid (ie, predicates that express equi-join
    // conditions between two tablerefs).
    // A predicate such as "t1.a = t2.b" has two entries, one for 't1' and
    // another one for 't2'.
    public final Map<TupleId, List<ExprId>> eqJoinConjuncts = Maps.newHashMap();

    // set of conjuncts that have been assigned to some PlanNode
    public Set<ExprId> assignedConjuncts =
        Collections.newSetFromMap(new IdentityHashMap<ExprId, Boolean>());

    // map from outer-joined tuple id, i.e., one that is nullable,
    // to the last Join clause (represented by its rhs table ref) that outer-joined it
    public final Map<TupleId, TableRef> outerJoinedTupleIds = Maps.newHashMap();

    // Map of registered conjunct to the last full outer join (represented by its
    // rhs table ref) that outer joined it.
    public final Map<ExprId, TableRef> fullOuterJoinedConjuncts = Maps.newHashMap();

    // Map of full-outer-joined tuple id to the last full outer join that outer-joined it
    public final Map<TupleId, TableRef> fullOuterJoinedTupleIds = Maps.newHashMap();

    // Map from semi-joined tuple id, i.e., one that is invisible outside the join's
    // On-clause, to its Join clause (represented by its rhs table ref). An anti-join is
    // a kind of semi-join, so anti-joined tuples are also registered here.
    public final Map<TupleId, TableRef> semiJoinedTupleIds = Maps.newHashMap();

    // Map from right-hand side table-ref id of an outer join to the list of
    // conjuncts in its On clause. There is always an entry for an outer join, but the
    // corresponding value could be an empty list. There is no entry for non-outer joins.
    public final Map<TupleId, List<ExprId>> conjunctsByOjClause = Maps.newHashMap();

    // map from registered conjunct to its containing outer join On clause (represented
    // by its right-hand side table ref); this is limited to conjuncts that can only be
    // correctly evaluated by the originating outer join, including constant conjuncts
    public final Map<ExprId, TableRef> ojClauseByConjunct = Maps.newHashMap();

    // map from registered conjunct to its containing semi join On clause (represented
    // by its right-hand side table ref)
    public final Map<ExprId, TableRef> sjClauseByConjunct = Maps.newHashMap();

    // map from registered conjunct to its containing inner join On clause (represented
    // by its right-hand side table ref)
    public final Map<ExprId, TableRef> ijClauseByConjunct = Maps.newHashMap();

    // map from slot id to the analyzer/block in which it was registered
    public final Map<SlotId, Analyzer> blockBySlot = Maps.newHashMap();

    // Tracks all privilege requests on catalog objects.
    private final Set<PrivilegeRequest> privilegeReqs = Sets.newLinkedHashSet();

    // List of PrivilegeRequest to custom authorization failure error message.
    // Tracks all privilege requests on catalog objects that need a custom
    // error message returned to avoid exposing existence of catalog objects.
    private final List<Pair<PrivilegeRequest, String>> maskedPrivilegeReqs =
        Lists.newArrayList();

    // accesses to catalog objects
    // TODO: This can be inferred from privilegeReqs. They should be coalesced.
    public Set<TAccessEvent> accessEvents = Sets.newHashSet();

    // Tracks all warnings (e.g. non-fatal errors) that were generated during analysis.
    // These are passed to the backend and eventually propagated to the shell. Maps from
    // warning message to the number of times that warning was logged (in order to avoid
    // duplicating the same warning over and over).
    public final LinkedHashMap<String, Integer> warnings =
        new LinkedHashMap<String, Integer>();

    // Tracks whether the warnings have been retrieved from this analyzer. If set to true,
    // adding new warnings will result in an error. This helps to make sure that no
    // warnings are added which will not be displayed.
    public boolean warningsRetrieved = false;

    // The SCC-condensed graph representation of all slot value transfers.
    private SccCondensedGraph valueTransferGraph;

    private final List<Pair<SlotId, SlotId>> registeredValueTransfers =
        Lists.newArrayList();

    // Bidirectional map between Integer index and TNetworkAddress.
    // Decreases the size of the scan range locations.
    private final ListMap<TNetworkAddress> hostIndex = new ListMap<TNetworkAddress>();

    // Cache of statement-relevant table metadata populated before analysis.
    private final StmtTableCache stmtTableCache;

    // Expr rewriter for folding constants.
    private final ExprRewriter constantFolder_ =
        new ExprRewriter(FoldConstantsRule.INSTANCE);

    // Expr rewriter for normalizing and rewriting expressions.
    private final ExprRewriter exprRewriter_;

    public GlobalState(StmtTableCache stmtTableCache, TQueryCtx queryCtx,
        AuthorizationConfig authzConfig) {
      this.stmtTableCache = stmtTableCache;
      this.queryCtx = queryCtx;
      this.authzConfig = authzConfig;
      this.lineageGraph = new ColumnLineageGraph();
      List<ExprRewriteRule> rules = Lists.newArrayList();
      // BetweenPredicates must be rewritten to be executable. Other non-essential
      // expr rewrites can be disabled via a query option. When rewrites are enabled
      // BetweenPredicates should be rewritten first to help trigger other rules.
      rules.add(BetweenToCompoundRule.INSTANCE);
      // Binary predicates must be rewritten to a canonical form for both Kudu predicate
      // pushdown and Parquet row group pruning based on min/max statistics.
      rules.add(NormalizeBinaryPredicatesRule.INSTANCE);
      if (queryCtx.getClient_request().getQuery_options().enable_expr_rewrites) {
        rules.add(FoldConstantsRule.INSTANCE);
        rules.add(NormalizeExprsRule.INSTANCE);
        rules.add(ExtractCommonConjunctRule.INSTANCE);
        // Relies on FoldConstantsRule and NormalizeExprsRule.
        rules.add(SimplifyConditionalsRule.INSTANCE);
        rules.add(EqualityDisjunctsToInRule.INSTANCE);
        rules.add(NormalizeCountStarRule.INSTANCE);
        rules.add(SimplifyDistinctFromRule.INSTANCE);
        rules.add(RemoveRedundantStringCast.INSTANCE);
      }
      exprRewriter_ = new ExprRewriter(rules);
    }
  };

  private final GlobalState globalState_;

  public boolean containsSubquery() { return globalState_.containsSubquery; }

  // An analyzer stores analysis state for a single select block. A select block can be
  // a top level select statement, or an inline view select block.
  // ancestors contains the Analyzers of the enclosing select blocks of 'this'
  // (ancestors[0] contains the immediate parent, etc.).
  private final ArrayList<Analyzer> ancestors_;

  // map from lowercase table alias to a view definition in this analyzer's scope
  private final Map<String, View> localViews_ = Maps.newHashMap();

  // Map from lowercase table alias to descriptor. Tables without an explicit alias
  // are assigned two implicit aliases: the unqualified and fully-qualified table name.
  // Such tables have two entries pointing to the same descriptor. If an alias is
  // ambiguous, then this map retains the first entry with that alias to simplify error
  // checking (duplicate vs. ambiguous alias).
  private final Map<String, TupleDescriptor> aliasMap_ = Maps.newHashMap();

  // Map from tuple id to its corresponding table ref.
  private final Map<TupleId, TableRef> tableRefMap_ = Maps.newHashMap();

  // Set of lowercase ambiguous implicit table aliases.
  private final Set<String> ambiguousAliases_ = Sets.newHashSet();

  // Map from lowercase fully-qualified path to its slot descriptor. Only contains paths
  // that have a scalar type as destination (see registerSlotRef()).
  private final Map<String, SlotDescriptor> slotPathMap_ = Maps.newHashMap();

  // Indicates whether this analyzer/block is guaranteed to have an empty result set
  // due to a limit 0 or constant conjunct evaluating to false.
  private boolean hasEmptyResultSet_ = false;

  // Indicates whether the select-project-join (spj) portion of this query block
  // is guaranteed to return an empty result set. Set due to a constant non-Having
  // conjunct evaluating to false.
  private boolean hasEmptySpjResultSet_ = false;

  public Analyzer(StmtTableCache stmtTableCache, TQueryCtx queryCtx,
      AuthorizationConfig authzConfig) {
    ancestors_ = Lists.newArrayList();
    globalState_ = new GlobalState(stmtTableCache, queryCtx, authzConfig);
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
    isWithClause_ = parentAnalyzer.isWithClause_;
  }

  /**
   * Returns a new analyzer with the specified parent analyzer but with a new
   * global state.
   */
  public static Analyzer createWithNewGlobalState(Analyzer parentAnalyzer) {
    GlobalState globalState = new GlobalState(parentAnalyzer.globalState_.stmtTableCache,
        parentAnalyzer.getQueryCtx(), parentAnalyzer.getAuthzConfig());
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
    List<String> result = new ArrayList<String>();
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
  public void registerLocalView(View view) throws AnalysisException {
    Preconditions.checkState(view.isLocalView());
    if (view.hasColLabels()) {
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
    tableRefMap_.put(result.getId(), ref);
    return result;
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
  public TableRef resolveTableRef(TableRef tableRef) throws AnalysisException {
    // Return the table if it is already resolved.
    if (tableRef.isResolved()) return tableRef;
    // Try to find a matching local view.
    if (tableRef.getPath().size() == 1) {
      // Searches the hierarchy of analyzers bottom-up for a registered local view with
      // a matching alias.
      String viewAlias = tableRef.getPath().get(0).toLowerCase();
      Analyzer analyzer = this;
      do {
        View localView = analyzer.localViews_.get(viewAlias);
        if (localView != null) return new InlineViewRef(localView, tableRef);
        analyzer = (analyzer.ancestors_.isEmpty() ? null : analyzer.ancestors_.get(0));
      } while (analyzer != null);
    }

    // Resolve the table ref's path and determine what resolved table ref
    // to replace it with.
    List<String> rawPath = tableRef.getPath();
    Path resolvedPath = null;
    try {
      resolvedPath = resolvePath(tableRef.getPath(), PathType.TABLE_REF);
    } catch (AnalysisException e) {
      // Register privilege requests to prefer reporting an authorization error over
      // an analysis error. We should not accidentally reveal the non-existence of a
      // table/database if the user is not authorized.
      if (rawPath.size() > 1) {
        registerPrivReq(new PrivilegeRequestBuilder()
            .onTable(rawPath.get(0), rawPath.get(1))
            .allOf(tableRef.getPrivilege()).toRequest());
      }
      registerPrivReq(new PrivilegeRequestBuilder()
          .onTable(getDefaultDb(), rawPath.get(0))
          .allOf(tableRef.getPrivilege()).toRequest());
      throw e;
    } catch (TableLoadingException e) {
      throw new AnalysisException(String.format(
          "Failed to load metadata for table: '%s'", Joiner.on(".").join(rawPath)), e);
    }

    Preconditions.checkNotNull(resolvedPath);
    if (resolvedPath.destTable() != null) {
      Table table = resolvedPath.destTable();
      if (table instanceof View) return new InlineViewRef((View) table, tableRef);
      // The table must be a base table.
      Preconditions.checkState(table instanceof HdfsTable ||
          table instanceof KuduTable ||
          table instanceof HBaseTable ||
          table instanceof DataSourceTable);
      return new BaseTableRef(tableRef, resolvedPath);
    } else {
      return new CollectionTableRef(tableRef, resolvedPath);
    }
  }

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
    List<TupleId> tids = Lists.newArrayList();
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

  public int getNumTableRefs() { return tableRefMap_.size(); }
  public TableRef getTableRef(TupleId tid) { return tableRefMap_.get(tid); }
  public ExprRewriter getConstantFolder() { return globalState_.constantFolder_; }
  public ExprRewriter getExprRewriter() { return globalState_.exprRewriter_; }

  /**
   * Given a "table alias"."column alias", return the SlotDescriptor
   */
  public SlotDescriptor getSlotDescriptor(String qualifiedColumnName) {
    return slotPathMap_.get(qualifiedColumnName);
  }

  /**
   * Return true if this analyzer has no ancestors. (i.e. false for the analyzer created
   * for inline views/ union operands, etc.)
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
    // We only allow correlated references in predicates of a subquery.
    boolean resolveInAncestors = false;
    if (pathType == PathType.TABLE_REF || pathType == PathType.ANY) {
      resolveInAncestors = true;
    } else if (pathType == PathType.SLOT_REF) {
      resolveInAncestors = isSubquery_;
    }
    // Convert all path elements to lower case.
    ArrayList<String> lcRawPath = Lists.newArrayListWithCapacity(rawPath.size());
    for (String s: rawPath) lcRawPath.add(s.toLowerCase());
    return resolvePath(lcRawPath, pathType, resolveInAncestors);
  }

  private Path resolvePath(List<String> rawPath, PathType pathType,
      boolean resolveInAncestors) throws AnalysisException, TableLoadingException {
    // List of all candidate paths with different roots. Paths in this list are initially
    // unresolved and may be illegal with respect to the pathType.
    List<Path> candidates = getTupleDescPaths(rawPath);

    LinkedList<String> errors = Lists.newLinkedList();
    if (pathType == PathType.SLOT_REF || pathType == PathType.STAR) {
      // Paths rooted at all of the unique registered tuple descriptors.
      for (TableRef tblRef: tableRefMap_.values()) {
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
        Table tbl = null;
        try {
          tbl = getTable(tblName.getDb(), tblName.getTbl());
        } catch (AnalysisException e) {
          // Ignore to allow path resolution to continue.
        }
        if (tbl != null) {
          candidates.add(new Path(tbl, rawPath.subList(tblNameIdx + 1, rawPath.size())));
        }
      }
    }

    Path result = resolvePaths(rawPath, candidates, pathType, errors);
    if (result == null && resolveInAncestors && hasAncestors()) {
      result = getParentAnalyzer().resolvePath(rawPath, pathType, true);
    }
    if (result == null) {
      Preconditions.checkState(!errors.isEmpty());
      throw new AnalysisException(errors.getFirst());
    }
    return result;
  }

  /**
   * Returns a list of unresolved Paths that are rooted at a registered tuple
   * descriptor matching a prefix of the given raw path.
   */
  public List<Path> getTupleDescPaths(List<String> rawPath)
      throws AnalysisException {
    ArrayList<Path> result = Lists.newArrayList();

    // Path rooted at a tuple desc with an explicit or implicit unqualified alias.
    TupleDescriptor rootDesc = getDescriptor(rawPath.get(0));
    if (rootDesc != null) {
      result.add(new Path(rootDesc, rawPath.subList(1, rawPath.size())));
    }

    // Path rooted at a tuple desc with an implicit qualified alias.
    if (rawPath.size() > 1) {
      rootDesc = getDescriptor(rawPath.get(0) + "." + rawPath.get(1));
      if (rootDesc != null) {
        result.add(new Path(rootDesc, rawPath.subList(2, rawPath.size())));
      }
    }
    return result;
  }

  /**
   * Resolves the given paths and checks them for legality and ambiguity. Returns the
   * single legal path resolution if it exists, null otherwise.
   * Populates 'errors' with a a prioritized list of error messages starting with the
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

    List<Path> legalPaths = Lists.newArrayList();
    for (Path p: paths) {
      if (!p.resolve()) continue;

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
    return legalPaths.get(0);
  }

  /**
   * Returns an existing or new SlotDescriptor for the given path. Always returns
   * a new empty SlotDescriptor for paths with a collection-typed destination.
   */
  public SlotDescriptor registerSlotRef(Path slotPath) throws AnalysisException {
    Preconditions.checkState(slotPath.isRootedAtTuple());
    // Always register a new slot descriptor for collection types. The BE currently
    // relies on this behavior for setting unnested collection slots to NULL.
    if (slotPath.destType().isCollectionType()) {
      SlotDescriptor result = addSlotDescriptor(slotPath.getRootDesc());
      result.setPath(slotPath);
      registerColumnPrivReq(result);
      return result;
    }
    // SlotRefs with a scalar type are registered against the slot's
    // fully-qualified lowercase path.
    String key = slotPath.toString();
    Preconditions.checkState(key.equals(key.toLowerCase()),
        "Slot paths should be lower case: " + key);
    SlotDescriptor existingSlotDesc = slotPathMap_.get(key);
    if (existingSlotDesc != null) return existingSlotDesc;
    SlotDescriptor result = addSlotDescriptor(slotPath.getRootDesc());
    result.setPath(slotPath);
    slotPathMap_.put(key, result);
    registerColumnPrivReq(result);
    return result;
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
        registerPrivReq(new PrivilegeRequestBuilder().
            allOf(Privilege.SELECT).onColumn(tupleDesc.getTableName().getDb(),
            tupleDesc.getTableName().getTbl(), column.getName()).toRequest());
      }
    }
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
        ojConjuncts = Lists.newArrayList();
        globalState_.conjunctsByOjClause.put(rhsRef.getId(), ojConjuncts);
      }
    }
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
      markConstantConjunct(conjunct, false);
    }
  }

  /**
   * Register all conjuncts that make up 'e'. If fromHavingClause is false, this conjunct
   * is assumed to originate from a WHERE or ON clause.
   */
  public void registerConjuncts(Expr e, boolean fromHavingClause)
      throws AnalysisException {
    for (Expr conjunct: e.getConjuncts()) {
      registerConjunct(conjunct);
      markConstantConjunct(conjunct, fromHavingClause);
    }
  }

  /**
   * If the given conjunct is a constant non-oj conjunct, marks it as assigned, and
   * evaluates the conjunct. If the conjunct evaluates to false, marks this query
   * block as having an empty result set or as having an empty select-project-join
   * portion, if fromHavingClause is true or false, respectively.
   * No-op if the conjunct is not constant or is outer joined.
   * Throws an AnalysisException if there is an error evaluating `conjunct`
   */
  private void markConstantConjunct(Expr conjunct, boolean fromHavingClause)
      throws AnalysisException {
    if (!conjunct.isConstant() || isOjConjunct(conjunct)) return;
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
        }
      } catch (InternalException ex) {
        throw new AnalysisException("Error evaluating \"" + conjunct.toSql() + "\"", ex);
      }
    }
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

    ArrayList<TupleId> tupleIds = Lists.newArrayList();
    ArrayList<SlotId> slotIds = Lists.newArrayList();
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
      tupleIds = Lists.newArrayList();
      binaryPred.getChild(i).getIds(tupleIds, null);
      if (tupleIds.size() == 1) {
        if (!globalState_.eqJoinConjuncts.containsKey(tupleIds.get(0))) {
          List<ExprId> conjunctIds = Lists.newArrayList();
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
    if (lhs instanceof NullLiteral || rhs instanceof NullLiteral ||
        lhs.getType().isNull() || rhs.getType().isNull()) {
      return;
    }
    // create an eq predicate between lhs and rhs
    BinaryPredicate p = new BinaryPredicate(BinaryPredicate.Operator.EQ, lhs, rhs);
    p.setIsAuxExpr();
    if (LOG.isTraceEnabled()) {
      LOG.trace("register eq predicate: " + p.toSql() + " " + p.debugString());
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
    List<Expr> result = Lists.newArrayList();
    for (Expr e: globalState_.conjuncts.values()) {
      if (e.isBoundByTupleIds(tupleIds)
          && !e.isAuxExpr()
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
  public List<Expr> getUnassignedConjuncts(PlanNode node) {
    return getUnassignedConjuncts(node.getTblRefIds());
  }

  /**
   * Return all unassigned registered conjuncts that are fully bound by the given
   * (logical) tuple ids, can be evaluated by 'tupleIds' and are not tied to an
   * Outer Join clause.
   */
  public List<Expr> getUnassignedConjuncts(List<TupleId> tupleIds) {
    List<Expr> result = Lists.newArrayList();
    for (Expr e: getUnassignedConjuncts(tupleIds, true)) {
      if (canEvalPredicate(tupleIds, e)) result.add(e);
    }
    return result;
  }

  /**
   * Returns true if 'e' must be evaluated after or by a join node. Note that it may
   * still be safe to evaluate 'e' elsewhere as well, but in any case 'e' must be
   * evaluated again by or after a join.
   */
  public boolean evalAfterJoin(Expr e) {
    List<TupleId> tids = Lists.newArrayList();
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
    List<Expr> result = Lists.newArrayList();
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
  public ImpaladCatalog getCatalog() { return globalState_.stmtTableCache.catalog; }
  public StmtTableCache getStmtTableCache() { return globalState_.stmtTableCache; }
  public Set<String> getAliases() { return aliasMap_.keySet(); }

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
    List<ExprId> conjunctIds = Lists.newArrayList();
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
    List<Expr> result = Lists.newArrayList();
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
   * - For On-clause predicates:
   *   - If the predicate is from an anti-join On-clause it must be evaluated by the
   *     corresponding anti-join node.
   *   - Predicates from the On-clause of an inner or semi join are evaluated at the
   *     node that materializes the required tuple ids, unless they reference outer
   *     joined tuple ids. In that case, the predicates are evaluated at the join node
   *     of the corresponding On-clause.
   *   - Predicates referencing full-outer joined tuples are assigned at the originating
   *     join if it is a full-outer join, otherwise at the last full-outer join that does
   *     not materialize the table ref ids of the originating join.
   *   - Predicates from the On-clause of a left/right outer join are assigned at
   *     the corresponding outer join node with the exception of simple predicates
   *     that only reference a single tuple id. Those may be assigned below the
   *     outer join node if they are from the same On-clause that makes the tuple id
   *     nullable.
   * - Otherwise, a predicate can only be correctly evaluated if for all outer-joined
   *   referenced tids the last join to outer-join this tid has been materialized.
   */
  public boolean canEvalPredicate(List<TupleId> tupleIds, Expr e) {
    if (!e.isBoundByTupleIds(tupleIds)) return false;
    ArrayList<TupleId> tids = Lists.newArrayList();
    e.getIds(tids, null);
    if (tids.isEmpty()) return true;

    if (e.isOnClauseConjunct()) {
      if (isAntiJoinedConjunct(e)) return canEvalAntiJoinedConjunct(e, tupleIds);

      if (isIjConjunct(e) || isSjConjunct(e)) {
        if (!containsOuterJoinedTid(tids)) return true;
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
        if (tids.size() > 1) return false;
        // Optimization for single-tid predicates: Legal to assign below the outer join
        // if the predicate is from the same On-clause that makes tid nullable
        // (otherwise e needn't be true when that tuple is set).
        TupleId tid = tids.get(0);
        return globalState_.ojClauseByConjunct.get(e.getId()) == getLastOjClause(tid);
      }

      // Should have returned in one of the cases above.
      Preconditions.checkState(false);
    }

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
   * Checks if a conjunct from the On-clause of an anti join can be evaluated in a node
   * that materializes a given list of tuple ids.
   */
  public boolean canEvalAntiJoinedConjunct(Expr e, List<TupleId> nodeTupleIds) {
    TableRef antiJoinRef = getAntiJoinRef(e);
    if (antiJoinRef == null) return true;
    List<TupleId> tids = Lists.newArrayList();
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
  public ArrayList<Expr> getBoundPredicates(TupleId destTid, Set<SlotId> ignoreSlots,
      boolean markAssigned) {
    ArrayList<Expr> result = Lists.newArrayList();
    for (ExprId srcConjunctId: globalState_.singleTidConjuncts) {
      Expr srcConjunct = globalState_.conjuncts.get(srcConjunctId);
      if (srcConjunct instanceof SlotRef) continue;
      Preconditions.checkNotNull(srcConjunct);
      List<TupleId> srcTids = Lists.newArrayList();
      List<SlotId> srcSids = Lists.newArrayList();
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
          ExprSubstitutionMap smap = new ExprSubstitutionMap();
          for (int i = 0; i < srcSids.size(); ++i) {
            smap.put(
                new SlotRef(globalState_.descTbl.getSlotDesc(srcSids.get(i))),
                new SlotRef(globalState_.descTbl.getSlotDesc(destSids.get(i))));
          }
          try {
            p = srcConjunct.trySubstitute(smap, this, false);
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
          if (reverseValueTransfer && !evalAfterJoin) markConjunctAssigned(srcConjunct);
        }

        // check if we already created this predicate
        if (!result.contains(p)) result.add(p);
      }
    }
    return result;
  }

  public ArrayList<Expr> getBoundPredicates(TupleId destTid) {
    return getBoundPredicates(destTid, new HashSet<SlotId>(), true);
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
    Set<SlotId> outerJoinedSlots = Sets.newHashSet();

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
  public <T extends Expr> void createEquivConjuncts(TupleId tid, List<T> conjuncts,
      Set<SlotId> ignoreSlots) {
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
      if (!partialEquivSlots.union(eqSlots.first, eqSlots.second)) conjunctIter.remove();
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
          conjuncts.add((T) createInferredEqPred(lhs, rhs));
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
    createEquivConjuncts(tid, conjuncts, new HashSet<SlotId>());
  }

  /**
   * Returns a map of slot equivalence classes on the set of slots in the given tuples.
   * Only contains equivalence classes with more than one member.
   */
  private Map<Integer, List<SlotId>> getEquivClassesOnTuples(List<TupleId> tids) {
    Map<Integer, List<SlotId>> result = Maps.newHashMap();
    SccCondensedGraph g = globalState_.valueTransferGraph;
    for (TupleId tid: tids) {
      for (SlotDescriptor slotDesc: getTupleDesc(tid).getSlots()) {
        if (slotDesc.getId().asInt() >= g.numVertices()) continue;
        int sccId = g.sccId(slotDesc.getId().asInt());
        // Ignore equivalence classes that are empty or only have a single member.
        if (g.sccMembersBySccId(sccId).length <= 1) continue;
        List<SlotId> slotIds = result.get(sccId);
        if (slotIds == null) {
          slotIds = Lists.newArrayList();
          result.put(sccId, slotIds);
        }
        slotIds.add(slotDesc.getId());
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
    List<List<SlotId>> allDestSids = Lists.newArrayList();
    TupleDescriptor destTupleDesc = getTupleDesc(destTid);
    if (srcSids.size() == 1) {
      // Generate all mappings to propagate predicates of the form <slot> <op> <constant>
      // to as many destination slots as possible.
      // TODO: If srcTid == destTid we could limit the mapping to partition
      // columns because mappings to non-partition columns do not provide
      // a performance benefit.
      SlotId srcSid = srcSids.get(0);
      for (SlotDescriptor destSlot: destTupleDesc.getSlots()) {
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
      List<SlotId> destSids = Lists.newArrayList();
      for (SlotId srcSid: srcSids) {
        for (SlotDescriptor destSlot: destTupleDesc.getSlots()) {
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
    // Construct predicate with all SlotRefs substituted by NullLiterals.
    List<SlotRef> slotRefs = Lists.newArrayList();
    p.collect(Predicates.instanceOf(SlotRef.class), slotRefs);

    // Map for substituting SlotRefs with NullLiterals.
    ExprSubstitutionMap nullSmap = new ExprSubstitutionMap();
    for (SlotRef slotRef: slotRefs) {
        // Preserve the original SlotRef type to ensure all substituted
        // subexpressions in the predicate have the same return type and
        // function signature as in the original predicate.
        nullSmap.put(slotRef.clone(), NullLiteral.create(slotRef.getType()));
    }
    Expr nullTuplePred = p.substitute(nullSmap, this, false);
    return FeSupport.EvalPredicate(nullTuplePred, getQueryCtx());
  }

  public TupleId getTupleId(SlotId slotId) {
    return globalState_.descTbl.getSlotDesc(slotId).getParent().getId();
  }

  public void registerValueTransfer(SlotId id1, SlotId id2) {
    globalState_.registeredValueTransfers.add(new Pair(id1, id2));
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
    WritableGraph directValueTransferGraph =
        new WritableGraph(globalState_.descTbl.getMaxSlotId().asInt() + 1);
    constructValueTransfersFromEqPredicates(directValueTransferGraph);
    for (Pair<SlotId, SlotId> p : globalState_.registeredValueTransfers) {
      directValueTransferGraph.addEdge(p.first.asInt(), p.second.asInt());
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
          LOG.trace("value transfer: from " + slotIds.first.toString());
        }
        if (!(secondBlock.hasLimitOffsetClause_ &&
            secondBlock.ancestors_.contains(firstBlock))) {
          g.addEdge(slotIds.first.asInt(), slotIds.second.asInt());
        }
        if (!(firstBlock.hasLimitOffsetClause_ &&
            firstBlock.ancestors_.contains(secondBlock))) {
          g.addEdge(slotIds.second.asInt(), slotIds.first.asInt());
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
    }
  }


  /**
   * Returns the equivalence class of the given slot id.
   * Time complexity: O(V) where V = number of slots
   */
  public List<SlotId> getEquivClass(SlotId sid) {
    SccCondensedGraph g = globalState_.valueTransferGraph;
    if (sid.asInt() >= g.numVertices()) return Collections.singletonList(sid);
    ArrayList<SlotId> result = new ArrayList<>();
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
    ArrayList<SlotId> result = new ArrayList<>();
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
    if (conjuncts == null) return;
    for (Expr p: conjuncts) {
      globalState_.assignedConjuncts.add(p.getId());
    }
  }

  /**
   * Mark predicate as assigned.
   */
  public void markConjunctAssigned(Expr conjunct) {
    globalState_.assignedConjuncts.add(conjunct.getId());
  }

  public Set<ExprId> getAssignedConjuncts() {
    return Sets.newHashSet(globalState_.assignedConjuncts);
  }

  public void setAssignedConjuncts(Set<ExprId> assigned) {
    globalState_.assignedConjuncts = Sets.newHashSet(assigned);
  }

  /**
   * Mark all slots that are referenced in exprs as materialized.
   */
  public void materializeSlots(List<Expr> exprs) {
    List<SlotId> slotIds = Lists.newArrayList();
    for (Expr e: exprs) {
      Preconditions.checkState(e.isAnalyzed());
      e.getIds(null, slotIds);
    }
    globalState_.descTbl.markSlotsMaterialized(slotIds);
  }

  public void materializeSlots(Expr e) {
    List<SlotId> slotIds = Lists.newArrayList();
    Preconditions.checkState(e.isAnalyzed());
    e.getIds(null, slotIds);
    globalState_.descTbl.markSlotsMaterialized(slotIds);
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
    Type newCompatibleType;
    if (lastCompatibleType == null) {
      newCompatibleType = expr.getType();
    } else {
      newCompatibleType = Type.getAssignmentCompatibleType(
          lastCompatibleType, expr.getType(), false);
    }
    if (!newCompatibleType.isValid()) {
      throw new AnalysisException(String.format(
          "Incompatible return types '%s' and '%s' of exprs '%s' and '%s'.",
          lastCompatibleType.toSql(), expr.getType().toSql(),
          lastCompatibleExpr.toSql(), expr.toSql()));
    }
    return newCompatibleType;
  }

  /**
   * Determines compatible type for given exprs, and casts them to compatible type.
   * Calls analyze() on each of the exprs.
   * Throw an AnalysisException if the types are incompatible,
   * returns compatible type otherwise.
   */
  public Type castAllToCompatibleType(List<Expr> exprs) throws AnalysisException {
    // Determine compatible type of exprs.
    Expr lastCompatibleExpr = exprs.get(0);
    Type compatibleType = null;
    for (int i = 0; i < exprs.size(); ++i) {
      exprs.get(i).analyze(this);
      compatibleType = getCompatibleType(compatibleType, lastCompatibleExpr,
          exprs.get(i));
    }
    // Add implicit casts if necessary.
    for (int i = 0; i < exprs.size(); ++i) {
      if (!exprs.get(i).getType().equals(compatibleType)) {
        Expr castExpr = exprs.get(i).castTo(compatibleType);
        exprs.set(i, castExpr);
      }
    }
    return compatibleType;
  }

  /**
   * Casts the exprs in the given lists position-by-position such that for every i,
   * the i-th expr among all expr lists is compatible.
   * Throw an AnalysisException if the types are incompatible.
   */
  public void castToUnionCompatibleTypes(List<List<Expr>> exprLists)
      throws AnalysisException {
    if (exprLists == null || exprLists.size() < 2) return;

    // Determine compatible types for exprs, position by position.
    List<Expr> firstList = exprLists.get(0);
    for (int i = 0; i < firstList.size(); ++i) {
      // Type compatible with the i-th exprs of all expr lists.
      // Initialize with type of i-th expr in first list.
      Type compatibleType = firstList.get(i).getType();
      // Remember last compatible expr for error reporting.
      Expr lastCompatibleExpr = firstList.get(i);
      for (int j = 1; j < exprLists.size(); ++j) {
        Preconditions.checkState(exprLists.get(j).size() == firstList.size());
        compatibleType = getCompatibleType(compatibleType,
            lastCompatibleExpr, exprLists.get(j).get(i));
        lastCompatibleExpr = exprLists.get(j).get(i);
      }
      // Now that we've found a compatible type, add implicit casts if necessary.
      for (int j = 0; j < exprLists.size(); ++j) {
        if (!exprLists.get(j).get(i).getType().equals(compatibleType)) {
          Expr castExpr = exprLists.get(j).get(i).castTo(compatibleType);
          exprLists.get(j).set(i, castExpr);
        }
      }
    }
  }

  public String getDefaultDb() { return globalState_.queryCtx.session.database; }
  public User getUser() { return user_; }
  public TQueryCtx getQueryCtx() { return globalState_.queryCtx; }
  public TQueryOptions getQueryOptions() {
    return globalState_.queryCtx.client_request.getQuery_options();
  }
  public AuthorizationConfig getAuthzConfig() { return globalState_.authzConfig; }
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
    globalState_.accessEvents.add(event);
  }

  /**
   * Returns the Table for the given database and table name from the 'stmtTableCache'
   * in the global analysis state.
   * Throws an AnalysisException if the database or table does not exist.
   * Throws a TableLoadingException if the registered table failed to load.
   * Does not register authorization requests or access events.
   */
  public Table getTable(String dbName, String tableName)
      throws AnalysisException, TableLoadingException {
    TableName tblName = new TableName(dbName, tableName);
    Table table = globalState_.stmtTableCache.tables.get(tblName);
    if (table == null) {
      if (!globalState_.stmtTableCache.dbs.contains(tblName.getDb())) {
        throw new AnalysisException(DB_DOES_NOT_EXIST_ERROR_MSG + tblName.getDb());
      } else {
        throw new AnalysisException(TBL_DOES_NOT_EXIST_ERROR_MSG + tblName.toString());
      }
    }
    Preconditions.checkState(table.isLoaded());
    if (table instanceof IncompleteTable) {
      // If there were problems loading this table's metadata, throw an exception
      // when it is accessed.
      ImpalaException cause = ((IncompleteTable) table).getCause();
      if (cause instanceof TableLoadingException) throw (TableLoadingException) cause;
      throw new TableLoadingException("Missing metadata for table: " + tableName, cause);
    }
    return table;
  }

  /**
   * Returns the Table with the given name from the 'loadedTables' map in the global
   * analysis state. Throws an AnalysisException if the table or the db does not exist.
   * Throws a TableLoadingException if the registered table failed to load.
   * Always registers a privilege request for the table at the given privilege level,
   * regardless of the state of the table (i.e. whether it exists, is loaded, etc.).
   * If addAccessEvent is true adds an access event for successfully loaded tables.
   */
  public Table getTable(TableName tableName, Privilege privilege, boolean addAccessEvent)
      throws AnalysisException, TableLoadingException {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(privilege);
    tableName = getFqTableName(tableName);
    if (privilege == Privilege.ANY) {
      registerPrivReq(new PrivilegeRequestBuilder()
          .any().onAnyColumn(tableName.getDb(), tableName.getTbl()).toRequest());
    } else {
      registerPrivReq(new PrivilegeRequestBuilder()
          .allOf(privilege).onTable(tableName.getDb(), tableName.getTbl()).toRequest());
    }
    Table table = getTable(tableName.getDb(), tableName.getTbl());
    Preconditions.checkNotNull(table);
    if (addAccessEvent) {
      // Add an audit event for this access
      TCatalogObjectType objectType = TCatalogObjectType.TABLE;
      if (table instanceof View) objectType = TCatalogObjectType.VIEW;
      globalState_.accessEvents.add(new TAccessEvent(
          tableName.toString(), objectType, privilege.toString()));
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
  public Table getTable(TableName tableName, Privilege privilege)
      throws AnalysisException {
    try {
      return getTable(tableName, privilege, true);
    } catch (TableLoadingException e) {
      throw new AnalysisException(e);
    }
  }

  /**
   * Returns the Catalog Db object for the given database at the given
   * Privilege level. The privilege request is tracked in the analyzer
   * and authorized post-analysis.
   *
   * Registers a new access event if the catalog lookup was successful.
   *
   * If the database does not exist in the catalog an AnalysisError is thrown.
   */
  public Db getDb(String dbName, Privilege privilege) throws AnalysisException {
    return getDb(dbName, privilege, true);
  }

  public Db getDb(String dbName, Privilege privilege, boolean throwIfDoesNotExist)
      throws AnalysisException {
    PrivilegeRequestBuilder pb = new PrivilegeRequestBuilder();
    if (privilege == Privilege.ANY) {
      registerPrivReq(
          pb.any().onAnyColumn(dbName, AuthorizeableTable.ANY_TABLE_NAME).toRequest());
    } else {
      registerPrivReq(pb.allOf(privilege).onDb(dbName).toRequest());
    }

    Db db = getDb(dbName, throwIfDoesNotExist);
    globalState_.accessEvents.add(new TAccessEvent(
        dbName, TCatalogObjectType.DATABASE, privilege.toString()));
    return db;
  }

  /**
   * Returns a Catalog Db object without checking for privileges.
   */
  public Db getDb(String dbName, boolean throwIfDoesNotExist)
      throws AnalysisException {
    Db db = getCatalog().getDb(dbName);
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
    registerPrivReq(new PrivilegeRequestBuilder().allOf(privilege)
        .onTable(dbName,  tableName).toRequest());
    try {
      Db db = getCatalog().getDb(dbName);
      if (db == null) {
        throw new DatabaseNotFoundException("Database not found: " + dbName);
      }
      return db.containsTable(tableName);
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
    return new ArrayList<Expr>(globalState_.conjuncts.values());
  }

  public int incrementCallDepth() { return ++callDepth_; }
  public int decrementCallDepth() { return --callDepth_; }
  public int getCallDepth() { return callDepth_; }

  public boolean hasMutualValueTransfer(SlotId a, SlotId b) {
    return hasValueTransfer(a, b) && hasValueTransfer(b, a);
  }

  public boolean hasValueTransfer(SlotId a, SlotId b) {
    SccCondensedGraph g = globalState_.valueTransferGraph;
    return a.equals(b) || (a.asInt() < g.numVertices() && b.asInt() < g.numVertices()
        && g.hasEdge(a.asInt(), b.asInt()));
  }

  public Map<String, View> getLocalViews() { return localViews_; }

  /**
   * Add a warning that will be displayed to the user. Ignores null messages. Once
   * getWarnings() has been called, no warning may be added to the Analyzer anymore.
   */
  public void addWarning(String msg) {
    Preconditions.checkState(!globalState_.warningsRetrieved);
    if (msg == null) return;
    Integer count = globalState_.warnings.get(msg);
    if (count == null) count = 0;
    globalState_.warnings.put(msg, count + 1);
  }

  /**
   * Registers a new PrivilegeRequest in the analyzer.
   */
  public void registerPrivReq(PrivilegeRequest privReq) {
    if (!enablePrivChecks_) return;
    if (maskPrivChecks_) {
      globalState_.maskedPrivilegeReqs.add(
          Pair.<PrivilegeRequest, String>create(privReq, authErrorMsg_));
    } else {
      globalState_.privilegeReqs.add(privReq);
    }
  }

  /**
   * Registers a table-level privilege request and an access event for auditing
   * for the given table and privilege. The table must be a base table or a
   * catalog view (not a local view).
   */
  public void registerAuthAndAuditEvent(Table table, Privilege priv) {
    // Add access event for auditing.
    if (table instanceof View) {
      View view = (View) table;
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
    TableName tableName = table.getTableName();
    registerPrivReq(new PrivilegeRequestBuilder()
        .onTable(tableName.getDb(), tableName.getTbl())
        .allOf(priv).toRequest());
  }
}
