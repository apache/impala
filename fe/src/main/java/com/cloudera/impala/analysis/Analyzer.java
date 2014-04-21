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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.ColumnType;
import com.cloudera.impala.catalog.DatabaseNotFoundException;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.ImpaladCatalog;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.TableLoadingException;
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
import com.cloudera.impala.thrift.TQueryContext;
import com.cloudera.impala.util.DisjointSet;
import com.cloudera.impala.util.TSessionStateUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
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
 */
public class Analyzer {
  // Common analysis error messages
  public final static String DB_DOES_NOT_EXIST_ERROR_MSG = "Database does not exist: ";
  public final static String DB_ALREADY_EXISTS_ERROR_MSG = "Database already exists: ";
  public final static String TBL_DOES_NOT_EXIST_ERROR_MSG = "Table does not exist: ";
  public final static String TBL_ALREADY_EXISTS_ERROR_MSG = "Table already exists: ";
  public final static String FN_DOES_NOT_EXIST_ERROR_MSG = "Function does not exist: ";
  public final static String FN_ALREADY_EXISTS_ERROR_MSG = "Function already exists: ";

  private final static Logger LOG = LoggerFactory.getLogger(Analyzer.class);

  private final User user_;

  // true if the corresponding select block has a limit clause
  private boolean hasLimit_ = false;

  // Current depth of nested analyze() calls. Used for enforcing a
  // maximum expr-tree depth. Needs to be manually maintained by the user
  // of this Analyzer with incrementCallDepth() and decrementCallDepth().
  private int callDepth_ = 0;

  // state shared between all objects of an Analyzer tree
  private static class GlobalState {
    public final ImpaladCatalog catalog;
    public final TQueryContext queryCtxt;
    public final DescriptorTable descTbl = new DescriptorTable();
    public final IdGenerator<ExprId> conjunctIdGenerator = ExprId.createGenerator();

    // True if we are analyzing an explain request. Should be set before starting
    // analysis.
    public boolean isExplain;

    // whether to use Hive's auto-generated column labels
    public boolean useHiveColLabels = false;

    // all registered conjuncts (map from id to Predicate)
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

    // map from outer-joined tuple id, ie, one that is nullable,
    // to the last Join clause (represented by its rhs table ref) that outer-joined it
    public final Map<TupleId, TableRef> outerJoinedTupleIds = Maps.newHashMap();

    // map from right-hand side table ref of an outer join to the list of
    // conjuncts in its On clause
    public final Map<TableRef, List<ExprId>> conjunctsByOjClause = Maps.newHashMap();

    // map from registered conjunct to its containing outer join On clause (represented
    // by its right-hand side table ref); this is limited to conjuncts that can only be
    // correctly evaluated by the originating outer join
    public final Map<ExprId, TableRef> ojClauseByConjunct = Maps.newHashMap();

    // map from slot id to the analyzer/block in which it was registered
    public final Map<SlotId, Analyzer> blockBySlot = Maps.newHashMap();

    // accesses to catalog objects
    public List<TAccessEvent> accessEvents = Lists.newArrayList();

    // map from equivalence class id to the list of its member slots
    private final Map<EquivalenceClassId, ArrayList<SlotId>> equivClassMembers =
        Maps.newHashMap();

    // map from slot id to its equivalence class id;
    // only visible at the root Analyzer
    private final Map<SlotId, EquivalenceClassId> equivClassBySlotId = Maps.newHashMap();

    // represents the direct and transitive value transfers between slots
    private ValueTransferGraph valueTransferGraph;

    private final List<Pair<SlotId, SlotId>> registeredValueTransfers =
        Lists.newArrayList();

    public GlobalState(ImpaladCatalog catalog, TQueryContext queryCtxt) {
      this.catalog = catalog;
      this.queryCtxt = queryCtxt;
    }
  };

  private final GlobalState globalState_;

  // An analyzer stores analysis state for a single select block. A select block can be
  // a top level select statement, or an inline view select block.
  // ancestors contains the Analyzers of the enclosing select blocks of 'this'
  // (ancestors[0] contains the immediate parent, etc.).
  private final ArrayList<Analyzer> ancestors_;

  // map from lowercase table alias to a view definition of a WITH clause.
  private final Map<String, ViewRef> withClauseViews_ = Maps.newHashMap();

  // map from lowercase table alias to descriptor.
  private final Map<String, TupleDescriptor> aliasMap_ = Maps.newHashMap();

  // map from lowercase qualified column name ("alias.col") to descriptor
  private final Map<String, SlotDescriptor> slotRefMap_ = Maps.newHashMap();

  // Tracks the all tables/views found during analysis that were missing metadata.
  Set<TableName> missingTbls_ = new HashSet<TableName>();
  public Set<TableName> getMissingTbls() { return missingTbls_; }

  public Analyzer(ImpaladCatalog catalog, TQueryContext queryCxt) {
    this.ancestors_ = Lists.newArrayList();
    this.globalState_ = new GlobalState(catalog, queryCxt);
    user_ = new User(TSessionStateUtil.getEffectiveUser(queryCxt.session));
  }

  /**
   * Analyzer constructor for nested select block. GlobalState is inherited from the
   * parentAnalyzer.
   * Performs the analysis as the given user that is required to be non null.
   */
  public Analyzer(Analyzer parentAnalyzer, User user) {
    this.ancestors_ = Lists.newArrayList(parentAnalyzer);
    this.ancestors_.addAll(parentAnalyzer.ancestors_);
    this.globalState_ = parentAnalyzer.globalState_;
    this.missingTbls_ = parentAnalyzer.missingTbls_;

    Preconditions.checkNotNull(user);
    this.user_ = user;
  }

  /**
   * Substitute analyzer's internal expressions (conjuncts) with the given substitution
   * map
   */
  public void substitute(Expr.SubstitutionMap sMap) {
    for (ExprId id: globalState_.conjuncts.keySet()) {
      globalState_.conjuncts.put(
          id, (Predicate) globalState_.conjuncts.get(id).substitute(sMap));
    }
  }

  /**
   * Searches the hierarchy of analyzers bottom-up for a registered view
   * whose alias matches the table name of the given BaseTableRef. Returns the
   * ViewRef from the innermost scope (analyzer).
   * If no such registered view was found, also searches for views from the catalog
   * if seachCatalog is true.
   * Returns null if no matching views were found.
   *
   * This method may trigger a metadata load for finding views registered
   * in the metastore, if seachCatalog is true.
   */
  public ViewRef findViewDefinition(BaseTableRef ref, boolean searchCatalog)
      throws AuthorizationException, AnalysisException {
    // Do not consider views from the WITH clause if the table name is fully qualified,
    // or if WITH-clause view matching was explicitly disabled.
    if (!ref.getName().isFullyQualified() && ref.isReplaceableByWithView()) {
      Analyzer analyzer = this;
      do {
        String baseTableName = ref.getName().getTbl().toLowerCase();
        ViewRef view = analyzer.withClauseViews_.get(baseTableName);
        if (view != null) return view;
        analyzer = (analyzer.ancestors_.isEmpty() ? null : analyzer.ancestors_.get(0));
      } while (analyzer != null);
    }
    if (!searchCatalog) return null;

    // Consult the catalog for a matching view.
    Table tbl = null;
    try {
      // Skip audit tracking for this request since it is only to determine if the
      // catalog contains the specified table/view.
      tbl = getTable(ref.getName(), ref.getPrivilegeRequirement(), false);
    } catch (AnalysisException e) {
      // Swallow this analysis exception to allow detection and proper error
      // reporting of recursive references in WITH-clause views.
      // Non-existent tables/databases will be detected when analyzing tbl.
      return null;
    }
    Preconditions.checkNotNull(tbl);
    if (tbl instanceof View) {
      View viewTbl = (View) tbl;
      Preconditions.checkNotNull(viewTbl.getViewDef());
      return viewTbl.getViewDef();
    }
    return null;
  }

  /**
   * Adds view to this analyzer's withClauseViews. Throws an exception if a view
   * definition with the same alias has already been registered.
   */
  public void registerWithClauseView(ViewRef ref) throws AnalysisException {
    if (withClauseViews_.put(ref.getAlias().toLowerCase(), ref) != null) {
      throw new AnalysisException(
          String.format("Duplicate table alias: '%s'", ref.getAlias()));
    }
  }

  /**
   * Checks that 'name' references an existing base table and that alias
   * isn't already registered. Creates and returns an empty TupleDescriptor
   * and registers it against alias. If alias is empty, register
   * "name.tbl" and "name.db.tbl" as aliases.
   * Requires that all views have been substituted.
   */
  public TupleDescriptor registerBaseTableRef(BaseTableRef ref)
      throws AnalysisException, AuthorizationException {
    String lookupAlias = ref.getAlias().toLowerCase();
    if (aliasMap_.containsKey(lookupAlias)) {
      throw new AnalysisException("Duplicate table alias: '" + lookupAlias + "'");
    }

    Table tbl = getTable(ref.getName(), ref.getPrivilegeRequirement(), true);
    // Views should have been substituted already.
    Preconditions.checkState(!(tbl instanceof View),
        String.format("View %s has not been properly substituted.", tbl.getFullName()));
    TupleDescriptor result = globalState_.descTbl.createTupleDescriptor();
    result.setTable(tbl);
    result.setAlias(lookupAlias);
    aliasMap_.put(lookupAlias, result);
    return result;
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
   * Register an inline view. The enclosing select block of the inline view should have
   * been analyzed.
   * Checks that the alias isn't already registered. Checks the inline view doesn't have
   * duplicate column names.
   * Creates and returns an empty, non-materialized TupleDescriptor for the inline view
   * and registers it against alias.
   * An InlineView object is created and is used as the underlying table of the tuple
   * descriptor.
   */
  public TupleDescriptor registerInlineViewRef(InlineViewRef ref)
      throws AnalysisException {
    String lookupAlias = ref.getAlias().toLowerCase();
    if (aliasMap_.containsKey(lookupAlias)) {
      throw new AnalysisException("Duplicate table alias: '" + lookupAlias + "'");
    }
    // Delegate creation of the tuple descriptor to the concrete inline view ref.
    TupleDescriptor tupleDesc = ref.createTupleDescriptor(globalState_.descTbl);
    tupleDesc.setAlias(lookupAlias);
    aliasMap_.put(lookupAlias, tupleDesc);
    return tupleDesc;
  }

  /**
   * Return descriptor of registered table/alias.
   */
  public TupleDescriptor getDescriptor(TableName name) {
    return aliasMap_.get(name.toString().toLowerCase());
  }

  public TupleDescriptor getTupleDesc(TupleId id) {
    return globalState_.descTbl.getTupleDesc(id);
  }

  /**
   * Given a "table alias"."column alias", return the SlotDescriptor
   */
  public SlotDescriptor getSlotDescriptor(String qualifiedColumnName) {
    return slotRefMap_.get(qualifiedColumnName);
  }

  /**
   * Checks that 'col' references an existing column for a registered table alias;
   * if alias is empty, tries to resolve the column name in the context of any of the
   * registered tables. Creates and returns an empty SlotDescriptor if the
   * column hasn't previously been registered, otherwise returns the existing
   * descriptor.
   */
  public SlotDescriptor registerColumnRef(TableName tblName, String colName)
      throws AnalysisException {
    String alias;
    if (tblName == null) {
      alias = resolveColumnRef(colName, null);
      if (alias == null) {
        throw new AnalysisException("couldn't resolve column reference: '" +
            colName + "'");
      }
    } else {
      alias = tblName.toString().toLowerCase();
    }

    TupleDescriptor tupleDesc = aliasMap_.get(alias);
    // Try to resolve column references ("table.col") that do not refer to an explicit
    // alias, and that do not use a fully-qualified table name.
    String tmpAlias = alias;
    if (tupleDesc == null && tblName != null) {
      tmpAlias = resolveColumnRef(colName, tblName.getTbl());
      tupleDesc = aliasMap_.get(tmpAlias);
    }
    if (tupleDesc == null) {
      throw new AnalysisException("unknown table alias: '" + alias + "'");
    }
    alias = tmpAlias;

    Column col = tupleDesc.getTable().getColumn(colName);
    if (col == null) {
      throw new AnalysisException("unknown column '" + colName +
          "' (table alias '" + alias + "')");
    }

    String key = alias + "." + col.getName();
    SlotDescriptor result = slotRefMap_.get(key);
    if (result != null) return result;
    result = addSlotDescriptor(tupleDesc);
    result.setColumn(col);
    result.setLabel(col.getName());
    slotRefMap_.put(alias + "." + col.getName(), result);
    return result;
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
   * Resolves column name in context of any of the registered table aliases.
   * Returns null if not found or multiple bindings to different tables exist,
   * otherwise returns the table alias.
   * If a specific table name was given (tableName != null) then only
   * columns from registered tables with a matching name are considered.
   */
  private String resolveColumnRef(String colName, String tableName)
      throws AnalysisException {
    String result = null;
    for (Map.Entry<String, TupleDescriptor> entry: aliasMap_.entrySet()) {
      Table table = entry.getValue().getTable();
      Column col = table.getColumn(colName);
      if (col != null &&
          (tableName == null || tableName.equalsIgnoreCase(table.getName()))) {
        if (result != null) {
          throw new AnalysisException(
              "Unqualified column reference '" + colName + "' is ambiguous");
        }
        result = entry.getKey();
      }
    }
    return result;
  }

  /**
   * Register all conjuncts in a list of predicates as Where clause conjuncts.
   */
  public void registerConjuncts(List<Expr> l) {
    for (Expr e: l) {
      registerConjuncts(e, null, true);
    }
  }

  /**
   * Register all conjuncts that make up the predicate and assign each conjunct an id.
   * If ref != null, ref is expected to be the right-hand side of an outer join,
   * and the conjuncts of p can only be evaluated by the node implementing that join
   * (p is the On clause).
   */
  public void registerConjuncts(Expr e, TableRef rhsRef, boolean fromWhereClause) {
    List<ExprId> ojConjuncts = null;
    if (rhsRef != null) {
      Preconditions.checkState(rhsRef.getJoinOp().isOuterJoin());
      ojConjuncts = globalState_.conjunctsByOjClause.get(rhsRef);
      if (ojConjuncts == null) {
        ojConjuncts = Lists.newArrayList();
        globalState_.conjunctsByOjClause.put(rhsRef, ojConjuncts);
      }
    }
    for (Expr conjunct: e.getConjuncts()) {
      registerConjunct(conjunct);
      if (rhsRef != null) {
        globalState_.ojClauseByConjunct.put(conjunct.getId(), rhsRef);
        ojConjuncts.add(conjunct.getId());
      }
      if (fromWhereClause) conjunct.setIsWhereClauseConjunct();
    }
  }

  /**
   * Register individual conjunct with all tuple and slot ids it references
   * and with the global conjunct list. Also assigns a new id.
   */
  public void registerConjunct(Expr e) {
    // always generate a new expr id; this might be a cloned conjunct that already
    // has the id of its origin set
    e.setId(globalState_.conjunctIdGenerator.getNextId());
    globalState_.conjuncts.put(e.getId(), e);

    ArrayList<TupleId> tupleIds = Lists.newArrayList();
    ArrayList<SlotId> slotIds = Lists.newArrayList();
    e.getIds(tupleIds, slotIds);

    // register single tid conjuncts
    if (tupleIds.size() == 1) globalState_.singleTidConjuncts.add(e.getId());

    LOG.trace("register tuple/slotConjunct: " + Integer.toString(e.getId().asInt())
        + " " + e.toSql() + " " + e.debugString());

    if (!(e instanceof BinaryPredicate)) return;
    BinaryPredicate binaryPred = (BinaryPredicate) e;

    // check whether this is an equi-join predicate, ie, something of the
    // form <expr1> = <expr2> where at least one of the exprs is bound by
    // exactly one tuple id
   if (binaryPred.getOp() != BinaryPredicate.Operator.EQ) return;
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
   */
  public void createAuxEquivPredicate(Expr lhs, Expr rhs) {
    // create an eq predicate between lhs and rhs
    BinaryPredicate p = new BinaryPredicate(BinaryPredicate.Operator.EQ, lhs, rhs);
    p.setIsAuxExpr();
    LOG.trace("register equiv predicate: " + p.toSql() + " " + p.debugString());
    try {
      // create casts if needed
      p.analyze(this);
    } catch (ImpalaException e) {
      // not an executable predicate; ignore
      return;
    }
    registerConjunct(p);
  }

  /**
   * Creates an analyzed equality predicate between the given slots.
   */
  public Expr createEqPredicate(SlotId lhsSlotId, SlotId rhsSlotId) {
    BinaryPredicate pred = new BinaryPredicate(BinaryPredicate.Operator.EQ,
        new SlotRef(globalState_.descTbl.getSlotDesc(lhsSlotId)),
        new SlotRef(globalState_.descTbl.getSlotDesc(rhsSlotId)));
    // analyze() creates casts, if needed
    try {
      pred.analyze(this);
    } catch (Exception e) {
      throw new IllegalStateException(
          "constructed predicate failed analysis: " + pred.toSql(), e);
    }
    return pred;
  }

  /**
   * Return all unassigned registered conjuncts that are fully bound by given
   * list of tuple ids. If 'inclOjConjuncts' is false, conjuncts tied to an Outer Join
   * clause are excluded.
   */
  public List<Expr> getUnassignedConjuncts(
      List<TupleId> tupleIds, boolean inclOjConjuncts) {
    LOG.trace("getUnassignedConjuncts for " + Id.printIds(tupleIds));
    List<Expr> result = Lists.newArrayList();
    for (Expr e: globalState_.conjuncts.values()) {
      if (e.isBoundByTupleIds(tupleIds)
          && !e.isAuxExpr()
          && !globalState_.assignedConjuncts.contains(e.getId())
          && (inclOjConjuncts
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
    if (tids.size() > 1 || isOjConjunct(e)
        || isOuterJoined(tids.get(0)) && e.isWhereClauseConjunct()) {
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
    List<ExprId> candidates = globalState_.conjunctsByOjClause.get(ref);
    if (candidates == null) return result;
    for (ExprId conjunctId: candidates) {
      if (!globalState_.assignedConjuncts.contains(conjunctId)) {
        Expr e = globalState_.conjuncts.get(conjunctId);
        Preconditions.checkState(e != null);
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
   * Return list of equi-join conjuncts that reference tid. If rhsRef != null, it is
   * assumed to be for an outer join, and only equi-join conjuncts from that outer join's
   * On clause are returned.
   */
  public List<Expr> getEqJoinConjuncts(TupleId id, TableRef rhsRef) {
    List<ExprId> conjunctIds = globalState_.eqJoinConjuncts.get(id);
    if (conjunctIds == null) return null;
    List<Expr> result = Lists.newArrayList();
    List<ExprId> ojClauseConjuncts = null;
    if (rhsRef != null) {
      Preconditions.checkState(rhsRef.getJoinOp().isOuterJoin());
      ojClauseConjuncts = globalState_.conjunctsByOjClause.get(rhsRef);
    }
    for (ExprId conjunctId: conjunctIds) {
      Expr e = globalState_.conjuncts.get(conjunctId);
      Preconditions.checkState(e != null);
      if (ojClauseConjuncts != null) {
        if (ojClauseConjuncts.contains(conjunctId)) result.add(e);
      } else {
        result.add(e);
      }
    }
    return result;
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
        // bail if this is from an OJ On clause; the join node will pick
        // it up later via getUnassignedOjConjuncts()
        return !globalState_.ojClauseByConjunct.containsKey(e.getId());
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
      } else {
        // non-OJ On-clause predicate: not okay if tid is nullable and the
        // predicate tests for null
        if (globalState_.outerJoinedTupleIds.containsKey(tid)
            && isTrueWithNullSlots(e)) {
          return false;
        }
      }
      return true;
    }

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
   * Returns a list of predicates that are fully bound by destTid. Predicates are derived
   * by replacing the slots of a source predicate with slots of the destTid, if for each
   * source slot there is an equivalent slot in destTid.
   * In particular, the returned list contains predicates that must be evaluated
   * at a join node (bound to outer-joined tuple) but can also be safely evaluated by a
   * plan node materializing destTid. Such predicates are not marked as assigned.
   * All other inferred predicates are marked as assigned.
   * This function returns bound predicates regardless of whether the source predicates
   * have been assigned. It is up to the caller to decide if a bound predicate
   * should actually be used.
   * Destination slots in destTid can be ignored by passing them in ignoreSlots.
   * TODO: exclude UDFs from predicate propagation? their overloaded variants could
   * have very different semantics
   */
  public ArrayList<Expr> getBoundPredicates(TupleId destTid, Set<SlotId> ignoreSlots) {
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
      }

      // Generate predicates for all src-to-dest slot mappings.
      for (List<SlotId> destSids: allDestSids) {
        Preconditions.checkState(destSids.size() == srcSids.size());
        Expr p;
        if (srcSids.containsAll(destSids)) {
          p = srcConjunct;
        } else {
          Expr.SubstitutionMap smap = new Expr.SubstitutionMap();
          for (int i = 0; i < srcSids.size(); ++i) {
            smap.addMapping(
                new SlotRef(globalState_.descTbl.getSlotDesc(srcSids.get(i))),
                new SlotRef(globalState_.descTbl.getSlotDesc(destSids.get(i))));
          }
          p = srcConjunct.clone(smap);
          try {
            // create casts if needed
            p.reanalyze(this);
          } catch (ImpalaException exc) {
            // not an executable predicate; ignore
            continue;
          }
          LOG.trace("new pred: " + p.toSql() + " " + p.debugString());
        }

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

        boolean evalByJoin = evalByJoin(srcConjunct) &&
            (globalState_.ojClauseByConjunct.get(srcConjunct.getId())
                != globalState_.outerJoinedTupleIds.get(srcTid));

        // mark all bound predicates including duplicate ones
        if (reverseValueTransfer && !evalByJoin) markConjunctAssigned(srcConjunct);

        // check if we already created this predicate
        if (!result.contains(p)) result.add(p);
      }
    }
    return result;
  }

  public ArrayList<Expr> getBoundPredicates(TupleId destTid) {
    return getBoundPredicates(destTid, new HashSet<SlotId>());
  }

  /**
   * For each equivalence class, adds/removes predicates from conjuncts such that
   * it contains a minimum set of <slot> = <slot> predicates that "cover" the equivalent
   * slots belonging to tid. The returned predicates are a minimum spanning tree of the
   * complete graph formed by connecting all of tid's equivalent slots of that class.
   * Preserves original conjuncts when possible. Should be called by PlanNodes that
   * materialize a new tuple and evaluate conjuncts (scan and aggregation nodes).
   * Does not enforce equivalence between slots in ignoreSlots. Equivalences (if any)
   * among slots in ignoreSlots can be assumed to have already been enforced.
   * TODO: Consider optimizing for the cheapest minimum set of predicates.
   */
  public void enforceSlotEquivalences(TupleId tid, List<Expr> conjuncts,
      Set<SlotId> ignoreSlots) {
    // Slot equivalences derived from the given conjuncts per equivalence class.
    // The map's value maps from slot id to a set of equivalent slots.
    DisjointSet<SlotId> conjunctsEquivSlots = new DisjointSet<SlotId>();
    Iterator<Expr> conjunctIter = conjuncts.iterator();
    while (conjunctIter.hasNext()) {
      Expr conjunct = conjunctIter.next();
      Pair<SlotId, SlotId> eqSlots = BinaryPredicate.getEqSlots(conjunct);
      if (eqSlots == null) continue;
      EquivalenceClassId lhsEqClassId = getEquivClassId(eqSlots.first);
      EquivalenceClassId rhsEqClassId = getEquivClassId(eqSlots.second);
      // slots may not be in the same eq class due to outer joins
      if (!lhsEqClassId.equals(rhsEqClassId)) continue;
      if (!conjunctsEquivSlots.union(eqSlots.first, eqSlots.second)) {
        // conjunct is redundant
        conjunctIter.remove();
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

    // Equivalences among slots belonging to tid by equivalence class. These equivalences
    // are derived from the whole query and must hold for the final query result.
    Map<EquivalenceClassId, List<SlotId>> targetEquivSlots = Maps.newHashMap();
    TupleDescriptor tupleDesc = getTupleDesc(tid);
    for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
      EquivalenceClassId eqClassId = getEquivClassId(slotDesc.getId());
      // Ignore equivalence classes that are empty or only have a single member.
      if (globalState_.equivClassMembers.get(eqClassId).size() <= 1) continue;
      List<SlotId> slotIds = targetEquivSlots.get(eqClassId);
      if (slotIds == null) {
        slotIds = Lists.newArrayList();
        targetEquivSlots.put(eqClassId, slotIds);
      }
      slotIds.add(slotDesc.getId());
    }

    // For each equivalence class, add missing predicates to conjuncts to form the
    // minimum spanning tree.
    for (Map.Entry<EquivalenceClassId, List<SlotId>> targetEqClass:
      targetEquivSlots.entrySet()) {
      List<SlotId> equivClassSlots = targetEqClass.getValue();
      if (equivClassSlots.size() < 2) continue;
      // Treat ignored slots as already connected.
      conjunctsEquivSlots.bulkUnion(ignoreSlots);
      conjunctsEquivSlots.checkConsistency();

      // Loop over all pairs of equivalent slots and merge their disjoint slots sets,
      // creating missing equality predicates as necessary.
      boolean done = false;
      for (int i = 1; i < equivClassSlots.size(); ++i) {
        SlotId rhs = equivClassSlots.get(i);
        for (int j = 0; j < i; ++j) {
          SlotId lhs = equivClassSlots.get(j);
          if (!conjunctsEquivSlots.union(lhs, rhs)) continue;
          Expr newEqPred = createEqPredicate(lhs, rhs);
          conjuncts.add(newEqPred);
          // Check for early termination.
          if (conjunctsEquivSlots.get(lhs).size() == equivClassSlots.size()) {
            done = true;
            break;
          }
        }
        if (done) break;
      }
    }
  }

  public void enforceSlotEquivalences(TupleId tid, List<Expr> conjuncts) {
    enforceSlotEquivalences(tid, conjuncts, new HashSet<SlotId>());
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

    // Map for substituting SlotRefs with NullLiterals.
    Expr.SubstitutionMap nullSmap = new Expr.SubstitutionMap();
    for (SlotRef slotRef: slotRefs) {
      nullSmap.addMapping(slotRef.clone(null), new NullLiteral());
    }
    Expr nullTuplePred = p.clone(nullSmap);
    try {
      nullTuplePred.reanalyze(this);
    } catch (Exception e) {
      Preconditions.checkState(false, "Failed to analyze generated predicate: "
          + nullTuplePred.toSql() + "." + e.getMessage());
    }
    try {
      return FeSupport.EvalPredicate(nullTuplePred, getQueryContext());
    } catch (InternalException e) {
      Preconditions.checkState(false, "Failed to evaluate generated predicate: "
          + nullTuplePred.toSql() + "." + e.getMessage());
    }
    return true;
  }

  private TupleId getTupleId(SlotId slotId) {
    return globalState_.descTbl.getSlotDesc(slotId).getParent().getId();
  }

  public void registerValueTransfer(SlotId id1, SlotId id2) {
    globalState_.registeredValueTransfers.add(new Pair(id1, id2));
  }

  public boolean isOuterJoined(TupleId tid) {
    return globalState_.outerJoinedTupleIds.containsKey(tid);
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
    IdGenerator<EquivalenceClassId> equivClassIdGenerator =
        EquivalenceClassId.createGenerator();
    for (int i = 0; i < numSlots; ++i) {
      EquivalenceClassId id = equivClassIdGenerator.getNextId();
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

    // populate equivClassBySlotId
    for (EquivalenceClassId id: globalState_.equivClassMembers.keySet()) {
      for (SlotId slotId: globalState_.equivClassMembers.get(id)) {
        globalState_.equivClassBySlotId.put(slotId, id);
      }
    }
  }

  /**
   * Return in equivSlotIds the ids of slots that are in the same equivalence class
   * as slotId and are part of a tuple in tupleIds.
   */
  public void getEquivSlots(SlotId slotId, List<TupleId> tupleIds,
      List<SlotId> equivSlotIds) {
    equivSlotIds.clear();
    LOG.trace("getequivslots: slotid=" + Integer.toString(slotId.asInt()));
    EquivalenceClassId classId = globalState_.equivClassBySlotId.get(slotId);
    for (SlotId memberId: globalState_.equivClassMembers.get(classId)) {
      if (tupleIds.contains(
          globalState_.descTbl.getSlotDesc(memberId).getParent().getId())) {
        equivSlotIds.add(memberId);
      }
    }
  }

  public EquivalenceClassId getEquivClassId(SlotId slotId) {
    return globalState_.equivClassBySlotId.get(slotId);
  }

  /**
   * Returns true if l1 and l2 only contain SlotRefs, and for all SlotRefs in l1 there
   * is an equivalent SlotRef in l2, and vice versa.
   * Returns false otherwise or if l1 or l2 is empty.
   */
  public boolean isEquivSlots(List<Expr> l1, List<Expr> l2) {
    if (l1.isEmpty() || l2.isEmpty()) return false;
    for (Expr e1: l1) {
      boolean matched = false;
      for (Expr e2: l2) {
        if (isEquivSlots(e1, e2)) {
          matched = true;
          break;
        }
      }
      if (!matched) return false;
    }
    for (Expr e2: l2) {
      boolean matched = false;
      for (Expr e1: l1) {
        if (isEquivSlots(e2, e1)) {
          matched = true;
          break;
        }
      }
      if (!matched) return false;
    }
    return true;
  }

  /**
   * Returns true if e1 and e2 are equivalent SlotRefs.
   */
  public boolean isEquivSlots(Expr e1, Expr e2) {
    SlotRef aSlot = e1.unwrapSlotRef(true);
    SlotRef bSlot = e2.unwrapSlotRef(true);
    if (aSlot == null || bSlot == null) return false;
    Preconditions.checkNotNull(globalState_.equivClassBySlotId.get(aSlot.getSlotId()));
    Preconditions.checkNotNull(globalState_.equivClassBySlotId.get(bSlot.getSlotId()));
    // Check whether aSlot and bSlot are in the same equivalence class.
    return globalState_.equivClassBySlotId.get(aSlot.getSlotId()).equals(
        globalState_.equivClassBySlotId.get(bSlot.getSlotId()));
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
      e.getIds(null, slotIds);
    }
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
  public ColumnType getCompatibleType(ColumnType lastCompatibleType,
      Expr lastCompatibleExpr, Expr expr)
      throws AnalysisException {
    ColumnType newCompatibleType;
    if (lastCompatibleType == null) {
      newCompatibleType = expr.getType();
    } else {
      newCompatibleType =
        ColumnType.getAssignmentCompatibleType(lastCompatibleType, expr.getType());
    }
    if (!newCompatibleType.isValid()) {
      throw new AnalysisException("Incompatible return types '" + lastCompatibleType +
          "' and '" + expr.getType() + "' of exprs '" +
          lastCompatibleExpr.toSql() + "' and '" + expr.toSql() + "'.");
    }
    return newCompatibleType;
  }

  /**
   * Determines compatible type for given exprs, and casts them to compatible type.
   * Calls analyze() on each of the exprs.
   * Throw an AnalysisException if the types are incompatible,
   * returns compatible type otherwise.
   */
  public ColumnType castAllToCompatibleType(List<Expr> exprs)
      throws AnalysisException, AuthorizationException {
    // Determine compatible type of exprs.
    Expr lastCompatibleExpr = exprs.get(0);
    ColumnType compatibleType = null;
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

  public Map<String, ViewRef> getWithClauseViews() { return withClauseViews_; }
  public String getDefaultDb() { return globalState_.queryCtxt.session.database; }
  public User getUser() { return user_; }
  public TQueryContext getQueryContext() { return globalState_.queryCtxt; }

  /**
   * Returns a list of the successful catalog object access events. Does not include
   * accesses that failed due to AuthorizationExceptions. In general, if analysis
   * fails for any reason this list may be incomplete.
   */
  public List<TAccessEvent> getAccessEvents() { return globalState_.accessEvents; }
  public void addAccessEvent(TAccessEvent event) { globalState_.accessEvents.add(event); }

  /**
   * Returns the Catalog Table object for the TableName at the given Privilege level.
   * If the table has not yet been loaded in the local catalog cache, it will get
   * added to the set of table names in "missingTbls_" and an AnalysisException will be
   * thrown.
   *
   * If the user does not have sufficient privileges to access the table an
   * AuthorizationException is thrown. If the table or the db does not exist in the
   * Catalog, an AnalysisError is thrown.
   * If addAccessEvent is true, this call will add a new entry to accessEvents if the
   * catalog access was successful. If false, no accessEvent will be added.
   */
  public Table getTable(TableName tableName, Privilege privilege, boolean addAccessEvent)
      throws AnalysisException, AuthorizationException {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(privilege);
    Table table = null;
    tableName = new TableName(getTargetDbName(tableName), tableName.getTbl());

    // This may trigger a metadata load, in which case we want to return the errors as
    // AnalysisExceptions.
    try {
      table = getCatalog().getTable(tableName.getDb(),
          tableName.getTbl(), getUser(), privilege);
      if (table == null) {
        throw new AnalysisException(TBL_DOES_NOT_EXIST_ERROR_MSG + tableName.toString());
      }
      if (!table.isLoaded()) {
        missingTbls_.add(new TableName(table.getDb().getName(), table.getName()));
        throw new AnalysisException(
            "Table/view is missing metadata: " + table.getFullName());
      }

      if (addAccessEvent) {
        // Add an audit event for this access
        TCatalogObjectType objectType = TCatalogObjectType.TABLE;
        if (table instanceof View) objectType = TCatalogObjectType.VIEW;
        globalState_.accessEvents.add(new TAccessEvent(
            tableName.toString(), objectType, privilege.toString()));
      }
    } catch (DatabaseNotFoundException e) {
      throw new AnalysisException(DB_DOES_NOT_EXIST_ERROR_MSG + tableName.getDb());
    } catch (TableLoadingException e) {
      String errorMsg =
          String.format("Failed to load metadata for table: %s", tableName.toString());
      // We don't want to log all AnalysisExceptions as ERROR, only failures due to
      // TableLoadingExceptions.
      LOG.error(errorMsg + "\n" + e.getMessage());
      throw new AnalysisException(errorMsg, e);
    }
    Preconditions.checkNotNull(table);
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
      throws AuthorizationException, AnalysisException {
    return getTable(tableName, privilege, true);
  }

  /**
   * Returns the Catalog Db object for the given database name at the given
   * Privilege level.
   *
   * If the user does not have sufficient privileges to access the database an
   * AuthorizationException is thrown.
   * If the database does not exist in the catalog an AnalysisError is thrown.
   */
  public Db getDb(String dbName, Privilege privilege)
      throws AnalysisException, AuthorizationException {
    Db db = getCatalog().getDb(dbName, getUser(), privilege);
    if (db == null) throw new AnalysisException(DB_DOES_NOT_EXIST_ERROR_MSG + dbName);
    globalState_.accessEvents.add(new TAccessEvent(
        dbName, TCatalogObjectType.DATABASE, privilege.toString()));
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
      throws AuthorizationException, AnalysisException {
    try {
      return getCatalog().dbContainsTable(dbName, tableName, getUser(), privilege);
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

  public void setIsExplain(boolean isExplain) { globalState_.isExplain = isExplain; }
  public boolean isExplain() { return globalState_.isExplain; }
  public void setUseHiveColLabels(boolean useHiveColLabels) {
    globalState_.useHiveColLabels = useHiveColLabels;
  }
  public boolean useHiveColLabels() { return globalState_.useHiveColLabels; }

  public boolean hasLimit() { return hasLimit_; }
  public void setHasLimit(boolean hasLimit) { this.hasLimit_ = hasLimit; }

  public List<Expr> getConjuncts() {
    return new ArrayList<Expr>(globalState_.conjuncts.values());
  }

  public int incrementCallDepth() { return ++callDepth_; }
  public int decrementCallDepth() { return --callDepth_; }
  public int getCallDepth() { return callDepth_; }

  public boolean hasValueTransfer(SlotId a, SlotId b) {
    return globalState_.valueTransferGraph.hasValueTransfer(a, b);
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
        TableRef tblRef = globalState_.ojClauseByConjunct.get(id);
        Preconditions.checkState(tblRef == null || tblRef.getJoinOp().isOuterJoin());
        if (tblRef == null) {
          // this eq predicate doesn't involve any outer join, ie, it is true for
          // each result row;
          // value transfer is not legal if the receiving slot is in an enclosed
          // scope of the source slot and the receiving slot's block has a limit
          Analyzer firstBlock = globalState_.blockBySlot.get(slotIds.first);
          Analyzer secondBlock = globalState_.blockBySlot.get(slotIds.second);
          LOG.trace("value transfer: from " + slotIds.first.toString());
          Pair<SlotId, SlotId> firstToSecond = null;
          Pair<SlotId, SlotId> secondToFirst = null;
          if (!(secondBlock.hasLimit_ && secondBlock.ancestors_.contains(firstBlock))) {
            firstToSecond = new Pair<SlotId, SlotId>(slotIds.first, slotIds.second);
          }
          if (!(firstBlock.hasLimit_ && firstBlock.ancestors_.contains(secondBlock))) {
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

        if (tblRef.getJoinOp() == JoinOperator.FULL_OUTER_JOIN) {
          // full outer joins don't guarantee any value transfer
          continue;
        }

        // this is some form of outer join
        SlotId outerSlot, innerSlot;
        if (tblRef.getId() == getTupleId(slotIds.first)) {
          innerSlot = slotIds.first;
          outerSlot = slotIds.second;
        } else if (tblRef.getId() == getTupleId(slotIds.second)) {
          innerSlot = slotIds.second;
          outerSlot = slotIds.first;
        } else {
          // this eq predicate is part of an OJ clause but doesn't reference
          // the joined table -> ignore this, we can't reason about when it'll
          // actually be true
          continue;
        }

        // value transfer is not legal if the receiving slot is in an enclosed
        // scope of the source slot and the receiving slot's block has a limit
        Analyzer innerBlock = globalState_.blockBySlot.get(innerSlot);
        Analyzer outerBlock = globalState_.blockBySlot.get(outerSlot);
        if (tblRef.getJoinOp() == JoinOperator.LEFT_OUTER_JOIN) {
          if (!(outerBlock.hasLimit_ && outerBlock.ancestors_.contains(innerBlock))) {
            valueTransfers.add(new Pair<SlotId, SlotId>(outerSlot, innerSlot));
          }
        } else if (tblRef.getJoinOp() == JoinOperator.RIGHT_OUTER_JOIN) {
          if (!(innerBlock.hasLimit_ && innerBlock.ancestors_.contains(outerBlock))) {
            valueTransfers.add(new Pair<SlotId, SlotId>(innerSlot, outerSlot));
          }
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
}
