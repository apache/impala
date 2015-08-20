// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Path.PathType;
import com.cloudera.impala.authorization.AuthorizationConfig;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.PrivilegeRequest;
import com.cloudera.impala.authorization.PrivilegeRequestBuilder;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.DataSourceTable;
import com.cloudera.impala.catalog.DatabaseNotFoundException;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.ImpaladCatalog;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.TableLoadingException;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Id;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.common.PrintUtils;
import com.cloudera.impala.planner.PlanNode;
import com.cloudera.impala.service.FeSupport;
import com.cloudera.impala.thrift.TAccessEvent;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TNetworkAddress;
import com.cloudera.impala.thrift.TQueryCtx;
import com.cloudera.impala.util.DisjointSet;
import com.cloudera.impala.util.EventSequence;
import com.cloudera.impala.util.ListMap;
import com.cloudera.impala.util.TSessionStateUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
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
 * Slot equivalence classes:
 * Equivalence of individual slots is computed based on registered equality predicates;
 * those predicates are either present directly in the query or are implied by the
 * syntactic elements used in query (example: a GROUP BY clause has implied equality
 * predicates between the grouping exprs and the grouping slots of the aggregation
 * output tuple).
 * Implied equality predicates are registered with createAuxEquivPredicate(); they are
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

  // true if the corresponding select block has a limit and/or offset clause
  private boolean hasLimitOffsetClause_ = false;

  // Current depth of nested analyze() calls. Used for enforcing a
  // maximum expr-tree depth. Needs to be manually maintained by the user
  // of this Analyzer with incrementCallDepth() and decrementCallDepth().
  private int callDepth_ = 0;

  // Flag indicating if this analyzer instance belongs to a subquery.
  private boolean isSubquery_ = false;

  // If set, when privilege requests are registered they will use this error
  // error message.
  private String authErrorMsg_;

  // If false, privilege requests will not be registered in the analyzer.
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
  public boolean isSubquery() { return isSubquery_; }
  public boolean setHasPlanHints() { return globalState_.hasPlanHints = true; }
  public boolean hasPlanHints() { return globalState_.hasPlanHints; }

  // state shared between all objects of an Analyzer tree
  // TODO: Many maps here contain properties about tuples, e.g., whether
  // a tuple is outer/semi joined, etc. Remove the maps in favor of making
  // them properties of the tuple descriptor itself.
  private static class GlobalState {
    // TODO: Consider adding an "exec-env"-like global singleton that contains the
    // catalog and authzConfig.
    public final ImpaladCatalog catalog;
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

    // whether to use Hive's auto-generated column labels
    public boolean useHiveColLabels = false;

    // all registered conjuncts (map from expr id to conjunct)
    public final Map<ExprId, Expr> conjuncts = Maps.newHashMap();

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
    // conjuncts in its On clause.
    public final Map<TupleId, List<ExprId>> conjunctsByOjClause = Maps.newHashMap();

    // map from registered conjunct to its containing outer join On clause (represented
    // by its right-hand side table ref); this is limited to conjuncts that can only be
    // correctly evaluated by the originating outer join, including constant conjuncts
    public final Map<ExprId, TableRef> ojClauseByConjunct = Maps.newHashMap();

    // map from registered conjunct to its containing semi join On clause (represented
    // by its right-hand side table ref)
    public final Map<ExprId, TableRef> sjClauseByConjunct = Maps.newHashMap();

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

    public final IdGenerator<EquivalenceClassId> equivClassIdGenerator =
        EquivalenceClassId.createGenerator();

    // map from equivalence class id to the list of its member slots
    private final Map<EquivalenceClassId, ArrayList<SlotId>> equivClassMembers =
        Maps.newHashMap();

    // map from slot id to its equivalence class id;
    // only visible at the root Analyzer
    private final Map<SlotId, EquivalenceClassId> equivClassBySlotId = Maps.newHashMap();

    // map for each slot to the canonical slot of its equivalence class
    private final ExprSubstitutionMap equivClassSmap = new ExprSubstitutionMap();

    // represents the direct and transitive value transfers between slots
    private ValueTransferGraph valueTransferGraph;

    private final List<Pair<SlotId, SlotId>> registeredValueTransfers =
        Lists.newArrayList();

    // Bidirectional map between Integer index and TNetworkAddress.
    // Decreases the size of the scan range locations.
    private final ListMap<TNetworkAddress> hostIndex = new ListMap<TNetworkAddress>();

    // Timeline of important events in the planning process, used for debugging /
    // profiling
    private final EventSequence timeline = new EventSequence("Planner Timeline");

    public GlobalState(ImpaladCatalog catalog, TQueryCtx queryCtx,
        AuthorizationConfig authzConfig) {
      this.catalog = catalog;
      this.queryCtx = queryCtx;
      this.authzConfig = authzConfig;
      this.lineageGraph = new ColumnLineageGraph();
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

  // Tracks the all tables/views found during analysis that were missing metadata.
  private Set<TableName> missingTbls_ = new HashSet<TableName>();

  // Indicates whether this analyzer/block is guaranteed to have an empty result set
  // due to a limit 0 or constant conjunct evaluating to false.
  private boolean hasEmptyResultSet_ = false;

  // Indicates whether the select-project-join (spj) portion of this query block
  // is guaranteed to return an empty result set. Set due to a constant non-Having
  // conjunct evaluating to false.
  private boolean hasEmptySpjResultSet_ = false;

  public Analyzer(ImpaladCatalog catalog, TQueryCtx queryCtx,
      AuthorizationConfig authzConfig) {
    ancestors_ = Lists.newArrayList();
    globalState_ = new GlobalState(catalog, queryCtx, authzConfig);
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
    missingTbls_ = parentAnalyzer.missingTbls_;
    user_ = parentAnalyzer.getUser();
    authErrorMsg_ = parentAnalyzer.authErrorMsg_;
    enablePrivChecks_ = parentAnalyzer.enablePrivChecks_;
  }

  /**
   * Returns a new analyzer with the specified parent analyzer but with a new
   * global state.
   */
  public static Analyzer createWithNewGlobalState(Analyzer parentAnalyzer) {
    GlobalState globalState = new GlobalState(parentAnalyzer.globalState_.catalog,
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

  public Set<TableName> getMissingTbls() { return missingTbls_; }
  public boolean hasMissingTbls() { return !missingTbls_.isEmpty(); }
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
   */
  public List<String> getWarnings() {
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
   * ref if it is already resolved. Adds audit events and privilege requests for
   * the database and/or table.
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
    tableRef.analyze(this);
    Path resolvedPath = tableRef.getResolvedPath();
    if (resolvedPath.destTable() != null) {
      Table table = resolvedPath.destTable();
      Preconditions.checkNotNull(table);
      if (table instanceof View) return new InlineViewRef((View) table, tableRef);
      // The table must be a base table.
      Preconditions.checkState(table instanceof HdfsTable ||
          table instanceof HBaseTable || table instanceof DataSourceTable);
      return new BaseTableRef(tableRef);
    } else {
      return new CollectionTableRef(tableRef);
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
    LOG.trace("registerFullOuterJoinedConjunct: " +
        globalState_.fullOuterJoinedConjuncts.toString());
  }

  /**
   * Register tids as being outer-joined by a full outer join clause represented by
   * rhsRef.
   */
  public void registerFullOuterJoinedTids(List<TupleId> tids, TableRef rhsRef) {
    for (TupleId tid: tids) {
      globalState_.fullOuterJoinedTupleIds.put(tid, rhsRef);
    }
    LOG.trace("registerFullOuterJoinedTids: " +
        globalState_.fullOuterJoinedTupleIds.toString());
  }

  /**
   * Register tids as being outer-joined by Join clause represented by rhsRef.
   */
  public void registerOuterJoinedTids(List<TupleId> tids, TableRef rhsRef) {
    for (TupleId tid: tids) {
      globalState_.outerJoinedTupleIds.put(tid, rhsRef);
    }
    LOG.trace("registerOuterJoinedTids: " + globalState_.outerJoinedTupleIds.toString());
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

  public TableRef getTableRef(TupleId tid) { return tableRefMap_.get(tid); }

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
    if (pathType == PathType.TABLE_REF) {
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
    ArrayList<Path> candidates = Lists.newArrayList();

    // Path rooted at a tuple desc with an explicit or implicit unqualified alias.
    TupleDescriptor rootDesc = getDescriptor(rawPath.get(0));
    if (rootDesc != null) {
      candidates.add(new Path(rootDesc, rawPath.subList(1, rawPath.size())));
    }

    // Path rooted at a tuple desc with an implicit qualified alias.
    if (rawPath.size() > 1) {
      rootDesc = getDescriptor(rawPath.get(0) + "." + rawPath.get(1));
      if (rootDesc != null) {
        candidates.add(new Path(rootDesc, rawPath.subList(2, rawPath.size())));
      }
    }

    LinkedList<String> errors = Lists.newLinkedList();
    if (pathType == PathType.SLOT_REF || pathType == PathType.STAR) {
      // Paths rooted at all of the unique registered tuple descriptors.
      for (TableRef tblRef: tableRefMap_.values()) {
        candidates.add(new Path(tblRef.getDesc(), rawPath));
      }
    } else {
      // Always prefer table ref paths rooted at a registered tuples descriptor.
      Preconditions.checkState(pathType == PathType.TABLE_REF);
      Path result = resolvePaths(rawPath, candidates, pathType, errors);
      if (result != null) return result;
      candidates.clear();

      // Add paths rooted at a table with an unqualified and fully-qualified table name.
      int end = Math.min(2, rawPath.size());
      for (int tblNameIdx = 0; tblNameIdx < end; ++tblNameIdx) {
        String dbName = (tblNameIdx == 0) ? getDefaultDb() : rawPath.get(0);
        String tblName = rawPath.get(tblNameIdx);
        Table tbl = null;
        try {
          tbl = getTable(dbName, tblName);
        } catch (AnalysisException e) {
          if (hasMissingTbls()) throw e;
          // Ignore other exceptions to allow path resolution to continue.
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
    } else {
      pathTypeStr = "Star expression";
      pathStr += ".*";
    }

    List<Path> legalPaths = Lists.newArrayList();
    for (Path p: paths) {
      if (!p.resolve()) continue;

      // Check legality of the resolved path.
      if (p.getRootDesc() != null && !isVisible(p.getRootDesc().getId())) {
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
    Preconditions.checkNotNull(slotPath.getRootDesc());
    // Always register a new slot descriptor for collection types.
    if (slotPath.destType().isCollectionType()) {
      SlotDescriptor result = addSlotDescriptor(slotPath.getRootDesc());
      result.setPath(slotPath);
      registerColumnPrivReq(result);
      return result;
    }
    // SlotRefs with a scalar type are registered against the slot's
    // fully-qualified lowercase path.
    String key = slotPath.toString();
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
   * Register all conjuncts that make up the On-clause 'e' of the given right-hand side
   * of a join. Assigns each conjunct a unique id. If rhsRef is the right-hand side of
   * an outer join, then the conjuncts conjuncts are registered such that they can only
   * be evaluated by the node implementing that join.
   */
  public void registerOnClauseConjuncts(Expr e, TableRef rhsRef)
      throws AnalysisException {
    Preconditions.checkNotNull(rhsRef);
    Preconditions.checkNotNull(e);
    List<ExprId> ojConjuncts = null;
    if (rhsRef.getJoinOp().isOuterJoin()) {
      ojConjuncts = globalState_.conjunctsByOjClause.get(rhsRef.getId());
      if (ojConjuncts == null) {
        ojConjuncts = Lists.newArrayList();
        globalState_.conjunctsByOjClause.put(rhsRef.getId(), ojConjuncts);
      }
    }
    for (Expr conjunct: e.getConjuncts()) {
      registerConjunct(conjunct);
      if (rhsRef.getJoinOp().isOuterJoin()) {
        globalState_.ojClauseByConjunct.put(conjunct.getId(), rhsRef);
        ojConjuncts.add(conjunct.getId());
      }
      if (rhsRef.getJoinOp().isSemiJoin()) {
        globalState_.sjClauseByConjunct.put(conjunct.getId(), rhsRef);
      }
      markConstantConjunct(conjunct, false);
    }
  }

  /**
   * Register all conjuncts that make up 'e'. If fromHavingClause is false, this conjunct
   * is assumed to originate from a Where clause.
   */
  public void registerConjuncts(Expr e, boolean fromHavingClause)
      throws AnalysisException {
    for (Expr conjunct: e.getConjuncts()) {
      registerConjunct(conjunct);
      if (!fromHavingClause) conjunct.setIsWhereClauseConjunct();
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
    if (tupleIds.size() == 1) globalState_.singleTidConjuncts.add(e.getId());

    LOG.trace("register tuple/slotConjunct: " + Integer.toString(e.getId().asInt())
        + " " + e.toSql() + " " + e.debugString());

    if (!(e instanceof BinaryPredicate)) return;
    BinaryPredicate binaryPred = (BinaryPredicate) e;

    // check whether this is an equi-join predicate, ie, something of the
    // form <expr1> = <expr2> where at least one of the exprs is bound by
    // exactly one tuple id
    if (binaryPred.getOp() != BinaryPredicate.Operator.EQ &&
       binaryPred.getOp() != BinaryPredicate.Operator.NULL_MATCHING_EQ) {
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
   * Create and register an auxiliary predicate to express an equivalence between two
   * exprs (BinaryPredicate with EQ); this predicate does not need to be assigned, but
   * it's used for equivalence class computation.
   * Does nothing if the lhs or rhs expr are NULL. Registering an equivalence with NULL
   * would be incorrect, because <expr> = NULL is false (even NULL = NULL).
   */
  public void createAuxEquivPredicate(Expr lhs, Expr rhs) {
    // Check the expr type as well as the class because  NullLiteral could have been
    // implicitly cast to a type different than NULL.
    if (lhs instanceof NullLiteral || rhs instanceof NullLiteral ||
        lhs.getType().isNull() || rhs.getType().isNull()) {
      return;
    }
    // create an eq predicate between lhs and rhs
    BinaryPredicate p = new BinaryPredicate(BinaryPredicate.Operator.EQ, lhs, rhs);
    p.setIsAuxExpr();
    LOG.trace("register equiv predicate: " + p.toSql() + " " + p.debugString());
    registerConjunct(p);
  }

  /**
   * Creates an analyzed equality predicate between the given slots.
   */
  public BinaryPredicate createEqPredicate(SlotId lhsSlotId, SlotId rhsSlotId) {
    BinaryPredicate pred = new BinaryPredicate(BinaryPredicate.Operator.EQ,
        new SlotRef(globalState_.descTbl.getSlotDesc(lhsSlotId)),
        new SlotRef(globalState_.descTbl.getSlotDesc(rhsSlotId)));
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
    LOG.trace("getUnassignedConjuncts for " + Id.printIds(tupleIds));
    List<Expr> result = Lists.newArrayList();
    for (Expr e: globalState_.conjuncts.values()) {
      if (e.isBoundByTupleIds(tupleIds)
          && !e.isAuxExpr()
          && !globalState_.assignedConjuncts.contains(e.getId())
          && ((inclOjConjuncts && !e.isConstant())
              || !globalState_.ojClauseByConjunct.containsKey(e.getId()))) {
        result.add(e);
        LOG.trace("getUnassignedConjunct: " + e.toSql());
      }
    }
    return result;
  }

  public boolean isOjConjunct(Expr e) {
    return globalState_.ojClauseByConjunct.containsKey(e.getId());
  }

  public TableRef getFullOuterJoinRef(Expr e) {
    return globalState_.fullOuterJoinedConjuncts.get(e.getId());
  }

  public boolean isFullOuterJoined(Expr e) {
    return globalState_.fullOuterJoinedConjuncts.containsKey(e.getId());
  }

  /**
   * Return all unassigned registered conjuncts that are fully bound by node's
   * (logical) tuple ids, can be evaluated by 'node' and are not tied to an Outer Join
   * clause.
   */
  public List<Expr> getUnassignedConjuncts(PlanNode node) {
    List<TupleId> tupleIds = node.getTblRefIds();
    LOG.trace("getUnassignedConjuncts for node with " + Id.printIds(tupleIds));
    List<Expr> result = Lists.newArrayList();
    for (Expr e: getUnassignedConjuncts(tupleIds, true)) {
      if (canEvalPredicate(node, e)) {
        result.add(e);
        LOG.trace("getUnassignedConjunct: " + e.toSql());
      }
    }
    return result;
  }

  /**
   * Returns true if e must be evaluated by a join node. Note that it may still be
   * safe to evaluate e elsewhere as well, but in any case the join must evaluate e.
   */
  public boolean evalByJoin(Expr e) {
    List<TupleId> tids = Lists.newArrayList();
    e.getIds(tids, null);
    if (tids.isEmpty()) return false;
    if (tids.size() > 1 || isOjConjunct(e) || isFullOuterJoined(e)
        || (isOuterJoined(tids.get(0)) && e.isWhereClauseConjunct())
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
        LOG.trace("getUnassignedOjConjunct: " + e.toSql());
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

  public DescriptorTable getDescTbl() {
    return globalState_.descTbl;
  }

  public ImpaladCatalog getCatalog() throws AnalysisException {
    if (!globalState_.catalog.isReady()) {
      throw new AnalysisException("This Impala daemon is not ready to accept user " +
          "requests. Status: Waiting for catalog update from the StateStore.");
    }
    return globalState_.catalog;
  }

  public Set<String> getAliases() {
    return aliasMap_.keySet();
  }

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
          !canEvalAntiJoinedConjunct(e, nodeTblRefIds)) {
        continue;
      }
      if (ojClauseConjuncts != null && !ojClauseConjuncts.contains(conjunctId)) continue;
      result.add(e);
    }
    return result;
  }

  /**
   * Checks if a conjunct can be evaluated at a node materializing a list of tuple ids
   * 'tids'.
   */
  public boolean canEvalFullOuterJoinedConjunct(Expr e, List<TupleId> tids) {
    TableRef fullOuterJoin = getFullOuterJoinRef(e);
    if (fullOuterJoin == null) return true;
    return tids.containsAll(fullOuterJoin.getAllTupleIds());
  }

  /**
   * Returns true if predicate 'e' can be correctly evaluated by a tree materializing
   * 'tupleIds', otherwise false:
   * - the predicate needs to be bound by tupleIds
   * - a Where clause predicate can only be correctly evaluated if for all outer-joined
   *   referenced tids the last join to outer-join this tid has been materialized
   * - an On clause predicate against the non-nullable side of an Outer Join clause
   *   can only be correctly evaluated by the join node that materializes the
   *   Outer Join clause
   */
  public boolean canEvalPredicate(List<TupleId> tupleIds, Expr e) {
    LOG.trace("canEval: " + e.toSql() + " " + e.debugString() + " "
        + Id.printIds(tupleIds));
    if (!e.isBoundByTupleIds(tupleIds)) return false;
    ArrayList<TupleId> tids = Lists.newArrayList();
    e.getIds(tids, null);
    if (tids.isEmpty()) return true;

    if (!e.isWhereClauseConjunct()) {
      if (tids.size() > 1) {
        // If the conjunct is from the ON-clause of an anti join, check if we can
        // assign it to this node.
        if (isAntiJoinedConjunct(e)) return canEvalAntiJoinedConjunct(e, tupleIds);
        // bail if this is from an OJ On clause; the join node will pick
        // it up later via getUnassignedOjConjuncts()
        if (globalState_.ojClauseByConjunct.containsKey(e.getId())) return false;
        // If this is not from an OJ On clause (e.g. where clause or On clause of an
        // inner join) and is full-outer joined, we need to make sure it is not
        // assigned below the full outer join node that outer-joined it.
        return canEvalFullOuterJoinedConjunct(e, tupleIds);
      }

      TupleId tid = tids.get(0);
      if (globalState_.ojClauseByConjunct.containsKey(e.getId())) {
        // OJ On-clause predicate: okay if it's from
        // the same On clause that makes tid nullable
        // (otherwise e needn't be true when that tuple is set)
        if (!globalState_.outerJoinedTupleIds.containsKey(tid)) return false;
        if (globalState_.ojClauseByConjunct.get(e.getId())
            != globalState_.outerJoinedTupleIds.get(tid)) {
          return false;
        }
        // Single tuple id conjuncts specified in the FOJ On-clause are not allowed to be
        // assigned below that full outer join in the operator tree.
        TableRef tblRef = globalState_.ojClauseByConjunct.get(e.getId());
        if (tblRef.getJoinOp().isFullOuterJoin()) return false;
      } else {
        // non-OJ On-clause predicate: not okay if tid is nullable and the
        // predicate tests for null
        if (globalState_.outerJoinedTupleIds.containsKey(tid)
            && isTrueWithNullSlots(e)) {
          return false;
        }
        // If this single tid conjunct is from the On-clause of an anti-join, check if we
        // can assign it to this node.
        if (isAntiJoinedConjunct(e)) return canEvalAntiJoinedConjunct(e, tupleIds);
      }
      // Single tid predicate that is not from an OJ On-clause and is outer-joined by a
      // full outer join cannot be assigned below that full outer join in the
      // operator tree.
      return canEvalFullOuterJoinedConjunct(e, tupleIds);
    }
    if (isAntiJoinedConjunct(e)) return canEvalAntiJoinedConjunct(e, tupleIds);

    for (TupleId tid: tids) {
      LOG.trace("canEval: checking tid " + tid.toString());
      TableRef rhsRef = getLastOjClause(tid);
      // this is not outer-joined; ignore
      if (rhsRef == null) continue;
      // check whether the last join to outer-join 'tid' is materialized by tupleIds
      boolean contains = tupleIds.containsAll(rhsRef.getAllTupleIds());
      LOG.trace("canEval: contains=" + (contains ? "true " : "false ")
          + Id.printIds(tupleIds) + " " + Id.printIds(rhsRef.getAllTupleIds()));
      if (!tupleIds.containsAll(rhsRef.getAllTupleIds())) return false;
    }
    return true;
  }

  private boolean canEvalPredicate(PlanNode node, Expr e) {
    return canEvalPredicate(node.getTblRefIds(), e);
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
      return nodeTupleIds.containsAll(antiJoinRef.getAllTupleIds())
          && antiJoinRef.getAllTupleIds().containsAll(nodeTupleIds);
    }
    // A single tid conjunct that is anti-joined can be safely assigned to a
    // node below the anti join that specified it.
    return globalState_.semiJoinedTupleIds.containsKey(tids.get(0));
  }

  /**
   * Returns a list of predicates that are fully bound by destTid. Predicates are derived
   * by replacing the slots of a source predicate with slots of the destTid, if for each
   * source slot there is an equivalent slot in destTid.
   * In particular, the returned list contains predicates that must be evaluated
   * at a join node (bound to outer-joined tuple) but can also be safely evaluated by a
   * plan node materializing destTid. Such predicates are not marked as assigned.
   * All other inferred predicates are marked as assigned if 'markAssigned'
   * is true. This function returns bound predicates regardless of whether the source
   * predicated have been assigned. It is up to the caller to decide if a bound predicate
   * should actually be used.
   * Destination slots in destTid can be ignored by passing them in ignoreSlots.
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
          getEquivDestSlotIds(srcTid, srcSids, destTid, ignoreSlots);
      if (allDestSids.isEmpty()) continue;

      // Indicates whether the source slots have equivalent slots that belong
      // to an outer-joined tuple.
      boolean hasOuterJoinedTuple = false;
      for (SlotId srcSid: srcSids) {
        if (hasOuterJoinedTuple(globalState_.equivClassBySlotId.get(srcSid))) {
          hasOuterJoinedTuple = true;
          break;
        }
      }

      // It is incorrect to propagate a Where-clause predicate into a plan subtree that
      // is on the nullable side of an outer join if the predicate evaluates to true
      // when all its referenced tuples are NULL. The check below is conservative
      // because the outer-joined tuple making 'hasOuterJoinedTuple' true could be in a
      // parent block of 'srcConjunct', in which case it is safe to propagate
      // 'srcConjunct' within child blocks of the outer-joined parent block.
      // TODO: Make the check precise by considering the blocks (analyzers) where the
      // outer-joined tuples in the dest slot's equivalence classes appear
      // relative to 'srcConjunct'.
      if (srcConjunct.isWhereClauseConjunct_ && hasOuterJoinedTuple &&
          isTrueWithNullSlots(srcConjunct)) {
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
          LOG.trace("new pred: " + p.toSql() + " " + p.debugString());
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

          // Check if either srcConjunct or the generated predicate needs to be evaluated
          // at a join node (IMPALA-2018).
          boolean evalByJoin =
              (evalByJoin(srcConjunct)
               && (globalState_.ojClauseByConjunct.get(srcConjunct.getId())
                != globalState_.outerJoinedTupleIds.get(srcTid)))
              || (evalByJoin(p)
                  && (globalState_.ojClauseByConjunct.get(p.getId())
                   != globalState_.outerJoinedTupleIds.get(destTid)));

          // mark all bound predicates including duplicate ones
          if (reverseValueTransfer && !evalByJoin) markConjunctAssigned(srcConjunct);
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
   * Modifies the analysis state associated with the rhs table ref of an outer join
   * to accomodate a join inversion that changes the rhs table ref of the join from
   * oldRhsTbl to newRhsTbl.
   * TODO: Revisit this function and how outer joins are inverted. This function
   * should not be necessary because the semantics of an inverted outer join do
   * not change. This function will naturally become obsolete when we can transform
   * outer joins with otherPredicates into inner joins.
   */
  public void invertOuterJoinState(TableRef oldRhsTbl, TableRef newRhsTbl) {
    Preconditions.checkState(oldRhsTbl.getJoinOp().isOuterJoin());
    // Invert analysis state for an outer join.
    List<ExprId> conjunctIds =
        globalState_.conjunctsByOjClause.remove(oldRhsTbl.getId());
    Preconditions.checkNotNull(conjunctIds);
    globalState_.conjunctsByOjClause.put(newRhsTbl.getId(), conjunctIds);
    for (ExprId eid: conjunctIds) {
      globalState_.ojClauseByConjunct.put(eid, newRhsTbl);
    }
    for (Map.Entry<TupleId, TableRef> e: globalState_.outerJoinedTupleIds.entrySet()) {
      if (e.getValue() == oldRhsTbl) e.setValue(newRhsTbl);
    }
  }

  /**
   * For each equivalence class, adds/removes predicates from conjuncts such that it
   * contains a minimum set of <lhsSlot> = <rhsSlot> predicates that establish the known
   * equivalences between slots in lhsTids and rhsTids which must be disjoint.
   * Preserves original conjuncts when possible. Assumes that predicates for establishing
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
  public <T extends Expr> void createEquivConjuncts(List<TupleId> lhsTids,
      List<TupleId> rhsTids, List<T> conjuncts) {
    Preconditions.checkState(Collections.disjoint(lhsTids, rhsTids));

    // Equivalence classes only containing slots belonging to lhsTids.
    Map<EquivalenceClassId, List<SlotId>> lhsEquivClasses =
        getEquivClasses(lhsTids);

    // Equivalence classes only containing slots belonging to rhsTids.
    Map<EquivalenceClassId, List<SlotId>> rhsEquivClasses =
        getEquivClasses(rhsTids);

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
    Iterator<T> conjunctIter = conjuncts.iterator();
    while (conjunctIter.hasNext()) {
      Expr conjunct = conjunctIter.next();
      Pair<SlotId, SlotId> eqSlots = BinaryPredicate.getEqSlots(conjunct);
      if (eqSlots == null) continue;
      EquivalenceClassId firstEqClassId = getEquivClassId(eqSlots.first);
      EquivalenceClassId secondEqClassId = getEquivClassId(eqSlots.second);
      // slots may not be in the same eq class due to outer joins
      if (!firstEqClassId.equals(secondEqClassId)) continue;

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
    for (Map.Entry<EquivalenceClassId, List<SlotId>> rhsEquivClass:
      rhsEquivClasses.entrySet()) {
      List<SlotId> lhsSlots = lhsEquivClasses.get(rhsEquivClass.getKey());
      if (lhsSlots == null) continue;
      List<SlotId> rhsSlots = rhsEquivClass.getValue();
      Preconditions.checkState(!lhsSlots.isEmpty() && !rhsSlots.isEmpty());

      if (!partialEquivSlots.union(lhsSlots.get(0), rhsSlots.get(0))) continue;
      // Do not create a new predicate from slots that are full outer joined because that
      // predicate may be incorrectly assigned to a node below the associated full outer
      // join.
      if (isFullOuterJoined(lhsSlots.get(0)) || isFullOuterJoined(rhsSlots.get(0))) {
        continue;
      }
      T newEqPred = (T) createEqPredicate(lhsSlots.get(0), rhsSlots.get(0));
      newEqPred.analyzeNoThrow(this);
      if (!hasMutualValueTransfer(lhsSlots.get(0), rhsSlots.get(0))) continue;
      conjuncts.add(newEqPred);
    }
  }

  /**
   * For each equivalence class, adds/removes predicates from conjuncts such that
   * it contains a minimum set of <slot> = <slot> predicates that establish
   * the known equivalences between slots belonging to tid. Preserves original
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
      EquivalenceClassId firstEqClassId = getEquivClassId(eqSlots.first);
      EquivalenceClassId secondEqClassId = getEquivClassId(eqSlots.second);
      // slots may not be in the same eq class due to outer joins
      if (!firstEqClassId.equals(secondEqClassId)) continue;
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
    Map<EquivalenceClassId, List<SlotId>> targetEquivClasses =
        getEquivClasses(Lists.newArrayList(tid));
    for (Map.Entry<EquivalenceClassId, List<SlotId>> targetEquivClass:
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
          if (!hasMutualValueTransfer(lhs, rhs)) continue;
          conjuncts.add((T) createEqPredicate(lhs, rhs));
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
   * Returns a map of partial equivalence classes that only contains slot ids belonging
   * to the given tuple ids. Only contains equivalence classes with more than one member.
   */
  private Map<EquivalenceClassId, List<SlotId>> getEquivClasses(List<TupleId> tids) {
    Map<EquivalenceClassId, List<SlotId>> result = Maps.newHashMap();
    for (TupleId tid: tids) {
      for (SlotDescriptor slotDesc: getTupleDesc(tid).getSlots()) {
        EquivalenceClassId eqClassId = getEquivClassId(slotDesc.getId());
        // Ignore equivalence classes that are empty or only have a single member.
        if (globalState_.equivClassMembers.get(eqClassId).size() <= 1) continue;
        List<SlotId> slotIds = result.get(eqClassId);
        if (slotIds == null) {
          slotIds = Lists.newArrayList();
          result.put(eqClassId, slotIds);
        }
        slotIds.add(slotDesc.getId());
      }
    }
    return result;
  }

  /**
   * Returns a list of slot mappings from srcTid to destTid for the purpose of predicate
   * propagation. Each mapping assigns every slot in srcSids to an equivalent slot in
   * destTid. Does not generate all possible mappings, but limits the results to
   * useful and/or non-redundant mappings, i.e., those mappings that would improve
   * the performance of query execution.
   */
  private List<List<SlotId>> getEquivDestSlotIds(TupleId srcTid, List<SlotId> srcSids,
      TupleId destTid, Set<SlotId> ignoreSlots) {
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
   * Returns true if the equivalence class identified by 'eqClassId' contains
   * a slot belonging to an outer-joined tuple.
   */
  private boolean hasOuterJoinedTuple(EquivalenceClassId eqClassId) {
    ArrayList<SlotId> eqClass = globalState_.equivClassMembers.get(eqClassId);
    for (SlotId s: eqClass) {
      if (isOuterJoined(getTupleId(s))) return true;
    }
    return false;
  }

  /**
   * Returns true if 'p' evaluates to true when all its referenced slots are NULL,
   * false otherwise.
   * TODO: Can we avoid dealing with the exceptions thrown by analysis and eval?
   */
  private boolean isTrueWithNullSlots(Expr p) {
    // Construct predicate with all SlotRefs substituted by NullLiterals.
    List<SlotRef> slotRefs = Lists.newArrayList();
    p.collect(Predicates.instanceOf(SlotRef.class), slotRefs);

    Expr nullTuplePred = null;
    // Map for substituting SlotRefs with NullLiterals.
    ExprSubstitutionMap nullSmap = new ExprSubstitutionMap();
    NullLiteral nullLiteral = new NullLiteral();
    nullLiteral.analyzeNoThrow(this);
    for (SlotRef slotRef: slotRefs) {
      nullSmap.put(slotRef.clone(), nullLiteral.clone());
    }
    nullTuplePred = p.substitute(nullSmap, this, false);
    try {
      return FeSupport.EvalPredicate(nullTuplePred, getQueryCtx());
    } catch (InternalException e) {
      Preconditions.checkState(false, "Failed to evaluate generated predicate: "
          + nullTuplePred.toSql() + "." + e.getMessage());
    }
    return true;
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
   * Populate globalState.valueTransfer based on the registered equi-join predicates
   * of the form <slotref> = <slotref>.
   */
  public void computeEquivClasses() {
    globalState_.valueTransferGraph = new ValueTransferGraph();
    globalState_.valueTransferGraph.computeValueTransfers();

    // we start out by assigning each slot to its own equiv class
    int numSlots = globalState_.descTbl.getMaxSlotId().asInt() + 1;
    for (int i = 0; i < numSlots; ++i) {
      EquivalenceClassId id = globalState_.equivClassIdGenerator.getNextId();
      globalState_.equivClassMembers.put(id, Lists.newArrayList(new SlotId(i)));
    }

    // merge two classes if there is a value transfer between all members of the
    // combined class; do this until there's nothing left to merge
    boolean merged;
    do {
      merged = false;
      for (Map.Entry<EquivalenceClassId, ArrayList<SlotId>> e1:
          globalState_.equivClassMembers.entrySet()) {
        for (Map.Entry<EquivalenceClassId, ArrayList<SlotId>> e2:
            globalState_.equivClassMembers.entrySet()) {
          if (e1.getKey() == e2.getKey()) continue;
          List<SlotId> class1Members = e1.getValue();
          if (class1Members.isEmpty()) continue;
          List<SlotId> class2Members = e2.getValue();
          if (class2Members.isEmpty()) continue;

          // check whether we can transfer values between all members
          boolean canMerge = true;
          for (SlotId class1Slot: class1Members) {
            for (SlotId class2Slot: class2Members) {
              if (!hasValueTransfer(class1Slot, class2Slot)
                  && !hasValueTransfer(class2Slot, class1Slot)) {
                canMerge = false;
                break;
              }
            }
            if (!canMerge) break;
          }
          if (!canMerge) continue;

          // merge classes 1 and 2 by transfering 2 into 1
          class1Members.addAll(class2Members);
          class2Members.clear();
          merged = true;
        }
      }
    } while (merged);

    // populate equivClassSmap
    for (EquivalenceClassId id: globalState_.equivClassMembers.keySet()) {
      List<SlotId> members = globalState_.equivClassMembers.get(id);
      if (members.isEmpty()) continue;
      SlotDescriptor canonicalSlotDesc =
          globalState_.descTbl.getSlotDesc(members.get(0));
      for (SlotId slotId: globalState_.equivClassMembers.get(id)) {
        SlotDescriptor slotDesc = globalState_.descTbl.getSlotDesc(slotId);
        globalState_.equivClassSmap.put(
            new SlotRef(slotDesc), new SlotRef(canonicalSlotDesc));
      }
    }

    // populate equivClassBySlotId
    for (EquivalenceClassId id: globalState_.equivClassMembers.keySet()) {
      for (SlotId slotId: globalState_.equivClassMembers.get(id)) {
        globalState_.equivClassBySlotId.put(slotId, id);
      }
    }
  }

  /**
   * Returns the ids of slots that are in the same equivalence class as slotId
   * and are part of a tuple in tupleIds.
   */
  public List<SlotId> getEquivSlots(SlotId slotId, List<TupleId> tupleIds) {
    List<SlotId> result = Lists.newArrayList();
    LOG.trace("getequivslots: slotid=" + Integer.toString(slotId.asInt()));
    EquivalenceClassId classId = globalState_.equivClassBySlotId.get(slotId);
    for (SlotId memberId: globalState_.equivClassMembers.get(classId)) {
      if (tupleIds.contains(
          globalState_.descTbl.getSlotDesc(memberId).getParent().getId())) {
        result.add(memberId);
      }
    }
    return result;
  }

  public EquivalenceClassId getEquivClassId(SlotId slotId) {
    return globalState_.equivClassBySlotId.get(slotId);
  }

  public ExprSubstitutionMap getEquivClassSmap() { return globalState_.equivClassSmap; }

  /**
   * Return true if l1 is equivalent to l2 when both lists are interpreted as sets.
   * For this check, each SlotRefs in both l1 and l2 is replaced with its canonical
   * equivalence-class representative, and then duplicate exprs are removed.
   */
  public boolean equivSets(List<Expr> l1, List<Expr> l2) {
    List<Expr> substL1 =
        Expr.substituteList(l1, globalState_.equivClassSmap, this, false);
    Expr.removeDuplicates(substL1);
    List<Expr> substL2 =
        Expr.substituteList(l2, globalState_.equivClassSmap, this, false);
    Expr.removeDuplicates(substL2);
    return Expr.equalSets(substL1, substL2);
  }

  /**
   * Returns true if e1 and e2 are equivalent expressions.
   */
  public boolean equivExprs(Expr e1, Expr e2) {
    Expr substE1 = e1.substitute(globalState_.equivClassSmap, this, false);
    Expr substE2 = e2.substitute(globalState_.equivClassSmap, this, false);
    return substE1.equals(substE2);
  }

  /**
   * Removes redundant expressions from exprs based on equivalence classes, as follows:
   * First, normalizes the exprs using the canonical SlotRef representative of each
   * equivalence class. Then retains the first original element of exprs that is
   * non-redundant in the normalized exprs. Returns a new list with the unique exprs.
   */
  public List<Expr> removeRedundantExprs(List<Expr> exprs) {
    List<Expr> result = Lists.newArrayList();
    List<Expr> normalizedExprs =
        Expr.substituteList(exprs, globalState_.equivClassSmap, this, false);
    Preconditions.checkState(exprs.size() == normalizedExprs.size());
    List<Expr> uniqueExprs = Lists.newArrayList();
    for (int i = 0; i < normalizedExprs.size(); ++i) {
      if (!uniqueExprs.contains(normalizedExprs.get(i))) {
        uniqueExprs.add(normalizedExprs.get(i));
        result.add(exprs.get(i).clone());
      }
    }
    return result;
  }

  /**
   * Mark predicates as assigned.
   */
  public void markConjunctsAssigned(List<Expr> conjuncts) {
    if (conjuncts == null) return;
    for (Expr p: conjuncts) {
      globalState_.assignedConjuncts.add(p.getId());
      LOG.trace("markAssigned " + p.toSql() + " " + p.debugString());
    }
  }

  /**
   * Mark predicate as assigned.
   */
  public void markConjunctAssigned(Expr conjunct) {
    LOG.trace("markAssigned " + conjunct.toSql() + " " + conjunct.debugString());
    globalState_.assignedConjuncts.add(conjunct.getId());
  }

  public boolean isConjunctAssigned(Expr conjunct) {
    return globalState_.assignedConjuncts.contains(conjunct.getId());
  }

  public Set<ExprId> getAssignedConjuncts() {
    return Sets.newHashSet(globalState_.assignedConjuncts);
  }

  public void setAssignedConjuncts(Set<ExprId> assigned) {
    globalState_.assignedConjuncts = Sets.newHashSet(assigned);
  }

  /**
   * Return true if there's at least one unassigned non-auxiliary conjunct.
   */
  public boolean hasUnassignedConjuncts() {
    for (ExprId id: globalState_.conjuncts.keySet()) {
      if (globalState_.assignedConjuncts.contains(id)) continue;
      Expr e = globalState_.conjuncts.get(id);
      if (e.isAuxExpr()) continue;
      LOG.trace("unassigned: " + e.toSql() + " " + e.debugString());
      return true;
    }
    return false;
  }

  /**
   * Mark all slots that are referenced in exprs as materialized.
   */
  public void materializeSlots(List<Expr> exprs) {
    List<SlotId> slotIds = Lists.newArrayList();
    for (Expr e: exprs) {
      Preconditions.checkState(e.isAnalyzed_);
      e.getIds(null, slotIds);
    }
    globalState_.descTbl.markSlotsMaterialized(slotIds);
  }

  public void materializeSlots(Expr e) {
    List<SlotId> slotIds = Lists.newArrayList();
    Preconditions.checkState(e.isAnalyzed_);
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
          lastCompatibleType, expr.getType());
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
  public AuthorizationConfig getAuthzConfig() { return globalState_.authzConfig; }
  public ListMap<TNetworkAddress> getHostIndex() { return globalState_.hostIndex; }
  public ColumnLineageGraph getColumnLineageGraph() { return globalState_.lineageGraph; }
  public String getSerializedLineageGraph() {
    Preconditions.checkNotNull(globalState_.lineageGraph);
    return globalState_.lineageGraph.toJson();
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
   * Returns the Catalog Table object for the given database and table name.
   * Adds the table to this analyzer's "missingTbls_" and throws an AnalysisException if
   * the table has not yet been loaded in the local catalog cache.
   * Throws an AnalysisException if the table or the db does not exist in the Catalog.
   * This function does not register authorization requests and does not log access events.
   */
  public Table getTable(String dbName, String tableName)
      throws AnalysisException, TableLoadingException {
    Table table = null;
    try {
      table = getCatalog().getTable(dbName, tableName);
    } catch (DatabaseNotFoundException e) {
      throw new AnalysisException(DB_DOES_NOT_EXIST_ERROR_MSG + dbName);
    } catch (CatalogException e) {
      String errMsg = String.format("Failed to load metadata for table: %s", tableName);
      // We don't want to log all AnalysisExceptions as ERROR, only failures due to
      // TableLoadingExceptions.
      LOG.error(String.format("%s\n%s", errMsg, e.getMessage()));
      if (e instanceof TableLoadingException) throw (TableLoadingException) e;
      throw new TableLoadingException(errMsg, e);
    }
    if (table == null) {
      throw new AnalysisException(
          TBL_DOES_NOT_EXIST_ERROR_MSG + dbName + "." + tableName);
    }
    if (!table.isLoaded()) {
      missingTbls_.add(new TableName(table.getDb().getName(), table.getName()));
      throw new AnalysisException(
          "Table/view is missing metadata: " + table.getFullName());
    }
    return table;
  }

  /**
   * Returns the Catalog Table object for the TableName.
   * Adds the table to this analyzer's "missingTbls_" and throws an AnalysisException if
   * the table has not yet been loaded in the local catalog cache.
   * Throws an AnalysisException if the table or the db does not exist in the Catalog.
   * Always registers a privilege request for the table at the given privilege level,
   * regardless of the state of the table (i.e. whether it exists, is loaded, etc.).
   * If addAccessEvent is true, adds an access event if the catalog access succeeded.
   */
  public Table getTable(TableName tableName, Privilege privilege, boolean addAccessEvent)
      throws AnalysisException {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(privilege);
    Table table = null;
    tableName = new TableName(getTargetDbName(tableName), tableName.getTbl());

    if (privilege == Privilege.ANY) {
      registerPrivReq(new PrivilegeRequestBuilder()
          .any().onAnyColumn(tableName.getDb(), tableName.getTbl()).toRequest());
    } else {
      registerPrivReq(new PrivilegeRequestBuilder()
          .allOf(privilege).onTable(tableName.getDb(), tableName.getTbl()).toRequest());
    }
    // This may trigger a metadata load, in which case we want to return the errors as
    // AnalysisExceptions.
    try {
      table = getTable(tableName.getDb(), tableName.getTbl());
    } catch (TableLoadingException e) {
      throw new AnalysisException(e.getMessage(), e);
    }
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
    return getTable(tableName, privilege, true);
  }

  /**
   * Returns the Catalog Db object for the given database at the given
   * Privilege level. The privilege request is tracked in the analyzer
   * and authorized post-analysis.
   *
   * If the database does not exist in the catalog an AnalysisError is thrown.
   *
   * If addAccessEvent is true, this call will add a new entry to accessEvents if the
   * catalog access was successful. If false, no accessEvent will be added.
   */
  public Db getDb(String dbName, Privilege privilege) throws AnalysisException {
    return getDb(dbName, privilege, true);
  }

  public Db getDb(String dbName, Privilege privilege, boolean throwIfDoesNotExist)
      throws AnalysisException {
    PrivilegeRequestBuilder pb = new PrivilegeRequestBuilder();
    if (privilege == Privilege.ANY) {
      registerPrivReq(pb.any().onAnyTable(dbName).toRequest());
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

  public String getTargetDbName(FunctionName fnName) {
    return fnName.isFullyQualified() ? fnName.getDb() : getDefaultDb();
  }

  /**
   * Returns the fully-qualified table name of tableName. If tableName
   * is already fully qualified, returns tableName.
   */
  public TableName getFqTableName(TableName tableName) {
    if (tableName.isFullyQualified()) return tableName;
    return new TableName(getDefaultDb(), tableName.getTbl());
  }

  public void setEnablePrivChecks(boolean value) {
    enablePrivChecks_ = value;
  }
  public void setAuthErrMsg(String errMsg) { authErrorMsg_ = errMsg; }
  public void setIsExplain() { globalState_.isExplain = true; }
  public boolean isExplain() { return globalState_.isExplain; }
  public void setUseHiveColLabels(boolean useHiveColLabels) {
    globalState_.useHiveColLabels = useHiveColLabels;
  }
  public boolean useHiveColLabels() { return globalState_.useHiveColLabels; }

  public void setHasLimitOffsetClause(boolean hasLimitOffset) {
    this.hasLimitOffsetClause_ = hasLimitOffset;
  }

  public List<Expr> getConjuncts() {
    return new ArrayList<Expr>(globalState_.conjuncts.values());
  }
  public Expr getConjunct(ExprId exprId) {
    return globalState_.conjuncts.get(exprId);
  }

  public int incrementCallDepth() { return ++callDepth_; }
  public int decrementCallDepth() { return --callDepth_; }
  public int getCallDepth() { return callDepth_; }

  private boolean hasMutualValueTransfer(SlotId slotA, SlotId slotB) {
    return hasValueTransfer(slotA, slotB) && hasValueTransfer(slotB, slotA);
  }

  public boolean hasValueTransfer(SlotId a, SlotId b) {
    return globalState_.valueTransferGraph.hasValueTransfer(a, b);
  }

  public EventSequence getTimeline() { return globalState_.timeline; }

  /**
   * Assign all remaining unassigned slots to their own equivalence classes.
   */
  public void createIdentityEquivClasses() {
    int numSlots = globalState_.descTbl.getMaxSlotId().asInt() + 1;
    for (int i = 0; i < numSlots; ++i) {
      SlotId slotId = new SlotId(i);
      if (globalState_.equivClassBySlotId.get(slotId) == null) {
        EquivalenceClassId classId = globalState_.equivClassIdGenerator.getNextId();
        globalState_.equivClassMembers.put(classId, Lists.newArrayList(slotId));
        globalState_.equivClassBySlotId.put(slotId, classId);
      }
    }
  }

  public Map<String, View> getLocalViews() { return localViews_; }

  /**
   * Add a warning that will be displayed to the user. Ignores null messages.
   */
  public void addWarning(String msg) {
    if (msg == null) return;
    Integer count = globalState_.warnings.get(msg);
    if (count == null) count = 0;
    globalState_.warnings.put(msg, count + 1);
  }

  /**
   * Efficiently computes and stores the transitive closure of the value transfer graph
   * of slots. After calling computeValueTransfers(), value transfers between slots can
   * be queried via hasValueTransfer(). Value transfers can be uni-directional due to
   * outer joins, inline views with a limit, or unions.
   */
  private class ValueTransferGraph {
    // Represents all bi-directional value transfers. Each disjoint set is a complete
    // subgraph of value transfers. Maps from a slot id to its set of slots with mutual
    // value transfers (in the original slot domain). Since the value transfer graph is
    // a DAG these disjoint sets represent all the strongly connected components.
    private final DisjointSet<SlotId> completeSubGraphs_ = new DisjointSet<SlotId>();

    // Maps each slot id in the original slot domain to a slot id in the new slot domain
    // created by coalescing complete subgraphs into a single slot, and retaining only
    // slots that have value transfers.
    // Used for representing a condensed value-transfer graph with dense slot ids.
    private int[] coalescedSlots_;

    // Used for generating slot ids in the new slot domain.
    private int nextCoalescedSlotId_ = 0;

    // Condensed DAG of value transfers in the new slot domain.
    private boolean[][] valueTransfer_;

    /**
     * Computes all direct and transitive value transfers based on the registered
     * conjuncts of the form <slotref> = <slotref>. The high-level steps are:
     * 1. Identify complete subgraps based on bi-directional value transfers, and
     *    coalesce the slots of each complete subgraph into a single slot.
     * 2. Map the remaining uni-directional value transfers into the new slot domain.
     * 3. Identify the connected components of the uni-directional value transfers.
     *    This step partitions the value transfers into disjoint sets.
     * 4. Compute the transitive closure of each partition from (3) in the new slot
     *    domain separately. Hopefully, the partitions are small enough to afford
     *    the O(N^3) complexity of the brute-force transitive closure computation.
     * The condensed graph is not transformed back into the original slot domain because
     * of the potential performance penalty. Instead, hasValueTransfer() consults
     * coalescedSlots_, valueTransfer_, and completeSubGraphs_ which can together
     * determine any value transfer in the original slot domain in constant time.
     */
    public void computeValueTransfers() {
      long start = System.currentTimeMillis();

      // Step1: Compute complete subgraphs and get uni-directional value transfers.
      List<Pair<SlotId, SlotId>> origValueTransfers = Lists.newArrayList();
      partitionValueTransfers(completeSubGraphs_, origValueTransfers);

      // Coalesce complete subgraphs into a single slot and assign new slot ids.
      int origNumSlots = globalState_.descTbl.getMaxSlotId().asInt() + 1;
      coalescedSlots_ = new int[origNumSlots];
      Arrays.fill(coalescedSlots_, -1);
      for (Set<SlotId> equivClass: completeSubGraphs_.getSets()) {
        int representative = nextCoalescedSlotId_;
        for (SlotId slotId: equivClass) {
          coalescedSlots_[slotId.asInt()] = representative;
        }
        ++nextCoalescedSlotId_;
      }

      // Step 2: Map uni-directional value transfers onto the new slot domain, and
      // store the connected components in graphPartitions.
      List<Pair<Integer, Integer>> coalescedValueTransfers = Lists.newArrayList();
      // A graph partition is a set of slot ids that are connected by uni-directional
      // value transfers. The graph corresponding to a graph partition is a DAG.
      DisjointSet<Integer> graphPartitions = new DisjointSet<Integer>();
      mapSlots(origValueTransfers, coalescedValueTransfers, graphPartitions);
      mapSlots(globalState_.registeredValueTransfers, coalescedValueTransfers,
          graphPartitions);

      // Step 3: Group the coalesced value transfers by the graph partition they
      // belong to. Maps from the graph partition to its list of value transfers.
      // TODO: Implement a specialized DisjointSet data structure to avoid this step.
      Map<Set<Integer>, List<Pair<Integer, Integer>>> partitionedValueTransfers =
          Maps.newHashMap();
      for (Pair<Integer, Integer> vt: coalescedValueTransfers) {
        Set<Integer> partition = graphPartitions.get(vt.first.intValue());
        List<Pair<Integer, Integer>> l = partitionedValueTransfers.get(partition);
        if (l == null) {
          l = Lists.newArrayList();
          partitionedValueTransfers.put(partition, l);
        }
        l.add(vt);
      }

      // Initialize the value transfer graph.
      int numCoalescedSlots = nextCoalescedSlotId_ + 1;
      valueTransfer_ = new boolean[numCoalescedSlots][numCoalescedSlots];
      for (int i = 0; i < numCoalescedSlots; ++i) {
        valueTransfer_[i][i] = true;
      }

      // Step 4: Compute the transitive closure for each graph partition.
      for (Map.Entry<Set<Integer>, List<Pair<Integer, Integer>>> graphPartition:
        partitionedValueTransfers.entrySet()) {
        // Set value transfers of this partition.
        for (Pair<Integer, Integer> vt: graphPartition.getValue()) {
          valueTransfer_[vt.first][vt.second] = true;
        }
        Set<Integer> partitionSlotIds = graphPartition.getKey();
        // No transitive value transfers.
        if (partitionSlotIds.size() <= 2) continue;

        // Indirection vector into valueTransfer_. Contains one entry for each distinct
        // slot id referenced in a value transfer of this partition.
        int[] p = new int[partitionSlotIds.size()];
        int numPartitionSlots = 0;
        for (Integer slotId: partitionSlotIds) {
          p[numPartitionSlots++] = slotId;
        }
        // Compute the transitive closure of this graph partition.
        // TODO: Since we are operating on a DAG the performance can be improved if
        // necessary (e.g., topological sort + backwards propagation of the transitive
        // closure).
        boolean changed = false;
        do {
          changed = false;
          for (int i = 0; i < numPartitionSlots; ++i) {
            for (int j = 0; j < numPartitionSlots; ++j) {
              for (int k = 0; k < numPartitionSlots; ++k) {
                if (valueTransfer_[p[i]][p[j]] && valueTransfer_[p[j]][p[k]]
                    && !valueTransfer_[p[i]][p[k]]) {
                  valueTransfer_[p[i]][p[k]] = true;
                  changed = true;
                }
              }
            }
          }
        } while (changed);
      }

      long end = System.currentTimeMillis();
      LOG.trace("Time taken in computeValueTransfers(): " + (end - start) + "ms");
    }

    /**
     * Bulk updates the value transfer graph based on the given list of new mutual value
     * transfers. The first element of each pair must be an existing slot id already in
     * the value transfer graph and the second element must be a new slot id not yet in
     * the value transfer graph. In particular, this requirement means that no new
     * complete subgraphs may be introduced by the new slots.
     */
    public void bulkUpdate(List<Pair<SlotId, SlotId>> mutualValueTransfers) {
      // Requires an existing value transfer graph.
      Preconditions.checkState(valueTransfer_ != null);
      int oldNumSlots = coalescedSlots_.length;
      int maxNumSlots = globalState_.descTbl.getMaxSlotId().asInt() + 1;
      // Expand the coalesced slots to the new maximum number of slots,
      // and initalize the new entries with -1.
      coalescedSlots_ = Arrays.copyOf(coalescedSlots_, maxNumSlots);
      Arrays.fill(coalescedSlots_, oldNumSlots, maxNumSlots, -1);
      for (Pair<SlotId, SlotId> valTrans: mutualValueTransfers) {
        SlotId existingSid = valTrans.first;
        SlotId newSid = valTrans.second;
        // New slot id must not already be registered in the value transfer graph.
        Preconditions.checkState(completeSubGraphs_.get(newSid) == null);
        Preconditions.checkState(coalescedSlots_[newSid.asInt()] == -1);
        completeSubGraphs_.union(existingSid, newSid);
        coalescedSlots_[newSid.asInt()] = coalescedSlots_[existingSid.asInt()];
      }
    }

    /**
     * Returns true if slotA always has the same value as slotB or the tuple
     * containing slotB is NULL.
     */
    public boolean hasValueTransfer(SlotId slotA, SlotId slotB) {
      if (slotA.equals(slotB)) return true;
      int mappedSrcId = coalescedSlots_[slotA.asInt()];
      int mappedDestId = coalescedSlots_[slotB.asInt()];
      if (mappedSrcId == -1 || mappedDestId == -1) return false;
      if (valueTransfer_[mappedSrcId][mappedDestId]) return true;
      Set<SlotId> eqSlots = completeSubGraphs_.get(slotA);
      if (eqSlots == null) return false;
      return eqSlots.contains(slotB);
    }

    /**
     * Maps the slots of the given origValueTransfers to the coalescedSlots_. For most
     * queries the uni-directional value transfers only reference a fraction of the
     * original slots, so we assign new slot ids as necessary to make them dense. Returns
     * the new list of value transfers in coalescedValueTransfers. Also adds each new
     * value transfer into the given graphPartitions (via union() on the slots).
     */
    private void mapSlots(List<Pair<SlotId, SlotId>> origValueTransfers,
        List<Pair<Integer, Integer>> coalescedValueTransfers,
        DisjointSet<Integer> graphPartitions) {
      for (Pair<SlotId, SlotId> vt: origValueTransfers) {
        int src = coalescedSlots_[vt.first.asInt()];
        if (src == -1) {
          src = nextCoalescedSlotId_;
          coalescedSlots_[vt.first.asInt()] = nextCoalescedSlotId_;
          ++nextCoalescedSlotId_;
        }
        int dest = coalescedSlots_[vt.second.asInt()];
        if (dest == -1) {
          dest = nextCoalescedSlotId_;
          coalescedSlots_[vt.second.asInt()] = nextCoalescedSlotId_;
          ++nextCoalescedSlotId_;
        }
        coalescedValueTransfers.add(
            new Pair<Integer, Integer>(Integer.valueOf(src), Integer.valueOf(dest)));
        graphPartitions.union(Integer.valueOf(src), Integer.valueOf(dest));
      }
    }

    /**
     * Transforms the registered equality predicates of the form <slotref> = <slotref>
     * into disjoint sets of slots with mutual value transfers (completeSubGraphs),
     * and a list of remaining uni-directional value transfers (valueTransfers).
     * Both completeSubGraphs and valueTransfers use the original slot ids.
     *
     * For debugging: If completeSubGraphs is null, adds all value transfers including
     * bi-directional ones into valueTransfers.
     */
    private void partitionValueTransfers(DisjointSet<SlotId> completeSubGraphs,
        List<Pair<SlotId, SlotId>> valueTransfers) {
      // transform equality predicates into a transfer graph
      for (ExprId id: globalState_.conjuncts.keySet()) {
        Expr e = globalState_.conjuncts.get(id);
        Pair<SlotId, SlotId> slotIds = BinaryPredicate.getEqSlots(e);
        if (slotIds == null) continue;

        boolean isAntiJoin = false;
        TableRef sjTblRef = globalState_.sjClauseByConjunct.get(id);
        Preconditions.checkState(sjTblRef == null || sjTblRef.getJoinOp().isSemiJoin());
        isAntiJoin = sjTblRef != null && sjTblRef.getJoinOp().isAntiJoin();

        TableRef ojTblRef = globalState_.ojClauseByConjunct.get(id);
        Preconditions.checkState(ojTblRef == null || ojTblRef.getJoinOp().isOuterJoin());
        if (ojTblRef == null && !isAntiJoin) {
          // this eq predicate doesn't involve any outer or anti join, ie, it is true for
          // each result row;
          // value transfer is not legal if the receiving slot is in an enclosed
          // scope of the source slot and the receiving slot's block has a limit
          Analyzer firstBlock = globalState_.blockBySlot.get(slotIds.first);
          Analyzer secondBlock = globalState_.blockBySlot.get(slotIds.second);
          LOG.trace("value transfer: from " + slotIds.first.toString());
          Pair<SlotId, SlotId> firstToSecond = null;
          Pair<SlotId, SlotId> secondToFirst = null;
          if (!(secondBlock.hasLimitOffsetClause_ &&
              secondBlock.ancestors_.contains(firstBlock))) {
            firstToSecond = new Pair<SlotId, SlotId>(slotIds.first, slotIds.second);
          }
          if (!(firstBlock.hasLimitOffsetClause_ &&
              firstBlock.ancestors_.contains(secondBlock))) {
            secondToFirst = new Pair<SlotId, SlotId>(slotIds.second, slotIds.first);
          }
          // Add bi-directional value transfers to the completeSubGraphs, or
          // uni-directional value transfers to valueTransfers.
          if (firstToSecond != null && secondToFirst != null
              && completeSubGraphs != null) {
            completeSubGraphs.union(slotIds.first, slotIds.second);
          } else {
            if (firstToSecond != null) valueTransfers.add(firstToSecond);
            if (secondToFirst != null) valueTransfers.add(secondToFirst);
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
          valueTransfers.add(new Pair<SlotId, SlotId>(outerSlot, innerSlot));
        } else if (tblRef.getJoinOp() == JoinOperator.RIGHT_OUTER_JOIN
            || tblRef.getJoinOp() == JoinOperator.RIGHT_ANTI_JOIN) {
          valueTransfers.add(new Pair<SlotId, SlotId>(innerSlot, outerSlot));
        }
      }
    }

    /**
     * Debug utility to validate the correctness of the value transfer graph using a
     * brute-force transitive closure algorithm.
     * Returns true if this value transfer graph is identical to one computed with
     * the brute-force method, false otherwise.
     * Writes string representations of the expected and actual value transfer
     * matrices into expected and actual, respectively.
     */
    public boolean validate(StringBuilder expected, StringBuilder actual) {
      Preconditions.checkState(expected.length() == 0 && actual.length() == 0);
      int numSlots = globalState_.descTbl.getMaxSlotId().asInt() + 1;
      boolean[][] expectedValueTransfer = new boolean[numSlots][numSlots];
      for (int i = 0; i < numSlots; ++i) {
        expectedValueTransfer[i][i] = true;
      }

      // Initialize expectedValueTransfer with the value transfers from conjuncts_.
      List<Pair<SlotId, SlotId>> valueTransfers = Lists.newArrayList();
      partitionValueTransfers(null, valueTransfers);
      for (Pair<SlotId, SlotId> vt: valueTransfers) {
        expectedValueTransfer[vt.first.asInt()][vt.second.asInt()] = true;
      }
      // Set registered value tranfers in expectedValueTransfer.
      for (Pair<SlotId, SlotId> vt: globalState_.registeredValueTransfers) {
        expectedValueTransfer[vt.first.asInt()][vt.second.asInt()] = true;
      }

      // Compute the expected transitive closure.
      boolean changed = false;
      do {
        changed = false;
        for (int i = 0; i < numSlots; ++i) {
          for (int j = 0; j < numSlots; ++j) {
            for (int k = 0; k < numSlots; ++k) {
              if (expectedValueTransfer[i][j] && expectedValueTransfer[j][k]
                  && !expectedValueTransfer[i][k]) {
                expectedValueTransfer[i][k] = true;
                changed = true;
              }
            }
          }
        }
      } while (changed);

      // Populate actual value transfer graph.
      boolean[][] actualValueTransfer = new boolean[numSlots][numSlots];
      for (int i = 0; i < numSlots; ++i) {
        for (int j = 0; j < numSlots; ++j) {
          actualValueTransfer[i][j] = hasValueTransfer(new SlotId(i), new SlotId(j));
        }
      }

      // Print matrices and string-compare them.
      PrintUtils.printMatrix(expectedValueTransfer, 3, expected);
      PrintUtils.printMatrix(actualValueTransfer, 3, actual);
      String expectedStr = expected.toString();
      String actualStr = actual.toString();
      return expectedStr.equals(actualStr);
    }
  }

  /**
   * Registers a new PrivilegeRequest in the analyzer. If authErrorMsg_ is set,
   * the privilege request will be added to the list of "masked" privilege requests,
   * using authErrorMsg_ as the auth failure error message. Otherwise it will get
   * added as a normal privilege request that will use the standard error message
   * on authorization failure.
   * If enablePrivChecks_ is false, the registration request will be ignored. This
   * is used when analyzing catalog views since users should be able to query a view
   * even if they do not have privileges on the underlying tables.
   */
  public void registerPrivReq(PrivilegeRequest privReq) {
    if (!enablePrivChecks_) return;

    if (Strings.isNullOrEmpty(authErrorMsg_)) {
      globalState_.privilegeReqs.add(privReq);
    } else {
      globalState_.maskedPrivilegeReqs.add(Pair.create(privReq, authErrorMsg_));
    }
  }
}
