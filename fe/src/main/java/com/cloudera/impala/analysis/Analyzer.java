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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.User;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.DatabaseNotFoundException;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.ImpaladCatalog;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.TableLoadingException;
import com.cloudera.impala.catalog.TableNotFoundException;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.Id;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.planner.PlanNode;
import com.cloudera.impala.thrift.TAccessEvent;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TQueryGlobals;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
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

  private final User user;

  // true if the corresponding select block has a limit clause
  private boolean hasLimit = false;

  // Current depth of nested analyze() calls. Used for enforcing a
  // maximum expr-tree depth. Needs to be manually maintained by the user
  // of this Analyzer with incrementCallDepth() and decrementCallDepth().
  private int callDepth = 0;

  // state shared between all objects of an Analyzer tree
  private static class GlobalState {
    public final ImpaladCatalog catalog;
    public final DescriptorTable descTbl = new DescriptorTable();
    public final String defaultDb;
    public final IdGenerator<ExprId> conjunctIdGenerator = new IdGenerator<ExprId>();
    public final TQueryGlobals queryGlobals = new TQueryGlobals();

    // True if we are analyzing an explain request. Should be set before starting
    // analysis.
    public boolean isExplain;

    // whether to use Hive's auto-generated column labels
    public boolean useHiveColLabels = false;

    // all registered conjuncts (map from id to Predicate)
    public final Map<ExprId, Expr> conjuncts = Maps.newHashMap();

    // map from tuple id to list of conjuncts referencing tuple
    public final Map<TupleId, List<ExprId>> tupleConjuncts = Maps.newHashMap();

    // map from slot id to list of conjuncts referencing slot
    public final Map<SlotId, List<ExprId>> slotConjuncts = Maps.newHashMap();

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

    // valueTransfer[slotA][slotB] is true if slotB always has the same value as slotA
    // or the tuple containing slotB is NULL
    private boolean[][] valueTransfer;

    private final List<Pair<SlotId, SlotId>> registeredValueTransfers =
        Lists.newArrayList();

    public GlobalState(ImpaladCatalog catalog, String defaultDb, User user) {
      this.catalog = catalog;
      this.defaultDb = defaultDb;

      // Create query global parameters to be set in each TPlanExecRequest.
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
      queryGlobals.setNow_string(formatter.format(Calendar.getInstance().getTime()));
      queryGlobals.setUser(user.getName());
    }
  };

  private final GlobalState globalState;

  // An analyzer stores analysis state for a single select block. A select block can be
  // a top level select statement, or an inline view select block.
  // ancestors contains the Analyzers of the enclosing select blocks of 'this'
  // (ancestors[0] contains the immediate parent, etc.).
  private final ArrayList<Analyzer> ancestors;

  // map from lowercase table alias to a view definition of a WITH clause.
  private final Map<String, ViewRef> withClauseViews = Maps.newHashMap();

  // map from lowercase table alias to descriptor.
  private final Map<String, TupleDescriptor> aliasMap = Maps.newHashMap();

  // map from lowercase qualified column name ("alias.col") to descriptor
  private final Map<String, SlotDescriptor> slotRefMap = Maps.newHashMap();

  public Analyzer(ImpaladCatalog catalog, String defaultDb, User user) {
    this.ancestors = Lists.newArrayList();
    this.globalState = new GlobalState(catalog, defaultDb, user);
    this.user = user;
  }

  /**
   * Analyzer constructor for nested select block. GlobalState is inherited from the
   * parentAnalyzer.
   * Performs the analysis as the given user that is required to be non null.
   */
  public Analyzer(Analyzer parentAnalyzer, User user) {
    this.ancestors = Lists.newArrayList(parentAnalyzer);
    this.ancestors.addAll(parentAnalyzer.ancestors);
    this.globalState = parentAnalyzer.globalState;

    Preconditions.checkNotNull(user);
    this.user = user;
  }

  /**
   * Substitute analyzer's internal expressions (conjuncts) with the given substitution
   * map
   */
  public void substitute(Expr.SubstitutionMap sMap) {
    for (ExprId id: globalState.conjuncts.keySet()) {
      globalState.conjuncts.put(
          id, (Predicate) globalState.conjuncts.get(id).substitute(sMap));
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
        ViewRef view = analyzer.withClauseViews.get(baseTableName);
        if (view != null) return view;
        analyzer = (analyzer.ancestors.isEmpty() ? null : analyzer.ancestors.get(0));
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
    if (withClauseViews.put(ref.getAlias().toLowerCase(), ref) != null) {
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
    if (aliasMap.containsKey(lookupAlias)) {
      throw new AnalysisException("Duplicate table alias: '" + lookupAlias + "'");
    }

    Table tbl = getTable(ref.getName(), ref.getPrivilegeRequirement(), true);
    // Views should have been substituted already.
    Preconditions.checkState(!(tbl instanceof View),
        String.format("View %s has not been properly substituted.", tbl.getFullName()));
    TupleDescriptor result = globalState.descTbl.createTupleDescriptor();
    result.setTable(tbl);
    result.setAlias(lookupAlias);
    aliasMap.put(lookupAlias, result);
    return result;
  }

  /**
   * Register tids as being outer-joined by Join clause represented by rhsRef.
   */
  public void registerOuterJoinedTids(List<TupleId> tids, TableRef rhsRef) {
    for (TupleId tid: tids) {
      globalState.outerJoinedTupleIds.put(tid, rhsRef);
    }
    LOG.trace("registerOuterJoinedTids: " + globalState.outerJoinedTupleIds.toString());
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
    if (aliasMap.containsKey(lookupAlias)) {
      throw new AnalysisException("Duplicate table alias: '" + lookupAlias + "'");
    }
    // Delegate creation of the tuple descriptor to the concrete inline view ref.
    TupleDescriptor tupleDesc = ref.createTupleDescriptor(globalState.descTbl);
    tupleDesc.setAlias(lookupAlias);
    aliasMap.put(lookupAlias, tupleDesc);
    return tupleDesc;
  }

  /**
   * Return descriptor of registered table/alias.
   */
  public TupleDescriptor getDescriptor(TableName name) {
    return aliasMap.get(name.toString().toLowerCase());
  }

  public TupleDescriptor getTupleDesc(TupleId id) {
    return globalState.descTbl.getTupleDesc(id);
  }

  /**
   * Given a "table alias"."column alias", return the SlotDescriptor
   */
  public SlotDescriptor getSlotDescriptor(String qualifiedColumnName) {
    return slotRefMap.get(qualifiedColumnName);
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

    TupleDescriptor tupleDesc = aliasMap.get(alias);
    // Try to resolve column references ("table.col") that do not refer to an explicit
    // alias, and that do not use a fully-qualified table name.
    String tmpAlias = alias;
    if (tupleDesc == null && tblName != null) {
      tmpAlias = resolveColumnRef(colName, tblName.getTbl());
      tupleDesc = aliasMap.get(tmpAlias);
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
    SlotDescriptor result = slotRefMap.get(key);
    if (result != null) return result;
    result = addSlotDescriptor(tupleDesc);
    result.setColumn(col);
    result.setLabel(col.getName());
    slotRefMap.put(alias + "." + col.getName(), result);
    return result;
  }

  /**
   * Creates a new slot descriptor and related state in globalState.
   */
  public SlotDescriptor addSlotDescriptor(TupleDescriptor tupleDesc) {
    SlotDescriptor result = globalState.descTbl.addSlotDescriptor(tupleDesc);
    globalState.blockBySlot.put(result.getId(), this);
    globalState.slotConjuncts.put(result.getId(), new ArrayList<ExprId>());
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
    for (Map.Entry<String, TupleDescriptor> entry: aliasMap.entrySet()) {
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
      ojConjuncts = globalState.conjunctsByOjClause.get(rhsRef);
      if (ojConjuncts == null) {
        ojConjuncts = Lists.newArrayList();
        globalState.conjunctsByOjClause.put(rhsRef, ojConjuncts);
      }
    }
    for (Expr conjunct: e.getConjuncts()) {
      registerConjunct(conjunct);
      if (rhsRef != null) {
        globalState.ojClauseByConjunct.put(conjunct.getId(), rhsRef);
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
    e.setId(new ExprId(globalState.conjunctIdGenerator));
    globalState.conjuncts.put(e.getId(), e);

    ArrayList<TupleId> tupleIds = Lists.newArrayList();
    ArrayList<SlotId> slotIds = Lists.newArrayList();
    e.getIds(tupleIds, slotIds);

    // update tuplePredicates
    for (TupleId id : tupleIds) {
      if (!globalState.tupleConjuncts.containsKey(id)) {
        List<ExprId> conjunctIds = Lists.newArrayList();
        conjunctIds.add(e.getId());
        globalState.tupleConjuncts.put(id, conjunctIds);
      } else {
        globalState.tupleConjuncts.get(id).add(e.getId());
      }
    }

    // update slotPredicates
    for (SlotId id : slotIds) {
      Preconditions.checkState(globalState.slotConjuncts.containsKey(id));
      globalState.slotConjuncts.get(id).add(e.getId());
    }

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
        if (!globalState.eqJoinConjuncts.containsKey(tupleIds.get(0))) {
          List<ExprId> conjunctIds = Lists.newArrayList();
          conjunctIds.add(e.getId());
          globalState.eqJoinConjuncts.put(tupleIds.get(0), conjunctIds);
        } else {
          globalState.eqJoinConjuncts.get(tupleIds.get(0)).add(e.getId());
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
   * Return all unassigned registered conjuncts that are fully bound by given
   * list of tuple ids. If 'inclOjConjuncts' is false, conjuncts tied to an Outer Join
   * clause are excluded.
   */
  public List<Expr> getUnassignedConjuncts(
      List<TupleId> tupleIds, boolean inclOjConjuncts) {
    LOG.trace("getUnassignedConjuncts for " + Id.printIds(tupleIds));
    List<Expr> result = Lists.newArrayList();
    for (Expr e: globalState.conjuncts.values()) {
      if (e.isBoundByTupleIds(tupleIds)
          && !e.isAuxExpr()
          && !globalState.assignedConjuncts.contains(e.getId())
          && (inclOjConjuncts
              || !globalState.ojClauseByConjunct.containsKey(e.getId()))) {
        result.add(e);
        LOG.trace("getUnassignedConjunct: " + e.toSql());
      }
    }
    return result;
  }

  public boolean isOjConjunct(Expr e) {
    return globalState.ojClauseByConjunct.containsKey(e.getId());
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
   * Return all unassigned conjuncts of the outer join referenced by right-hand side
   * table ref.
   */
  public List<Expr> getUnassignedOjConjuncts(TableRef ref) {
    Preconditions.checkState(ref.getJoinOp().isOuterJoin());
    List<Expr> result = Lists.newArrayList();
    List<ExprId> candidates = globalState.conjunctsByOjClause.get(ref);
    if (candidates == null) return result;
    for (ExprId conjunctId: candidates) {
      if (!globalState.assignedConjuncts.contains(conjunctId)) {
        Expr e = globalState.conjuncts.get(conjunctId);
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
    return globalState.outerJoinedTupleIds.get(id);
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
    return globalState.descTbl;
  }

  public ImpaladCatalog getCatalog() throws AnalysisException {
    if (!globalState.catalog.isReady()) {
      throw new AnalysisException("This Impala daemon is not ready to accept user " +
          "requests. Status: Waiting for catalog update from the StateStore.");
    }
    return globalState.catalog;
  }

  public Set<String> getAliases() {
    return aliasMap.keySet();
  }

  /**
   * Return list of equi-join conjuncts that reference tid. If rhsRef != null, it is
   * assumed to be for an outer join, and only equi-join conjuncts from that outer join's
   * On clause are returned.
   */
  public List<Expr> getEqJoinConjuncts(TupleId id, TableRef rhsRef) {
    List<ExprId> conjunctIds = globalState.eqJoinConjuncts.get(id);
    if (conjunctIds == null) return null;
    List<Expr> result = Lists.newArrayList();
    List<ExprId> ojClauseConjuncts = null;
    if (rhsRef != null) {
      Preconditions.checkState(rhsRef.getJoinOp().isOuterJoin());
      ojClauseConjuncts = globalState.conjunctsByOjClause.get(rhsRef);
    }
    for (ExprId conjunctId: conjunctIds) {
      Expr e = globalState.conjuncts.get(conjunctId);
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
        return !globalState.ojClauseByConjunct.containsKey(e.getId());
      }

      TupleId tid = tids.get(0);
      if (globalState.ojClauseByConjunct.containsKey(e.getId())) {
        // OJ On-clause predicate: okay if it's from
        // the same On clause that makes tid nullable
        // (otherwise e needn't be true when that tuple is set)
        if (!globalState.outerJoinedTupleIds.containsKey(tid)) return false;
        if (globalState.ojClauseByConjunct.get(e.getId())
            != globalState.outerJoinedTupleIds.get(tid)) {
          return false;
        }
      } else {
        // non-OJ On-clause predicate: not okay if tid is nullable and the
        // predicate tests for null
        if (globalState.outerJoinedTupleIds.containsKey(tid)
            // TODO: also check for IFNULL()
            && e.contains(IsNullPredicate.class)) {
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
   * Returns pairs <Predicate, bool>: the predicate is fully bound by slotId and can
   * be evaluated by 'node'; the bool indicates whether the application of the
   * predicate to slotId implies an
   * assignment of that predicate (ie, it doesn't need to be applied to the slot
   * from which it originated).
   * Predicates are derived from binding predicates of slots in the same equivalence
   * class as slotId.
   * TODO: find a more descriptive name for this function
   * getImpliedPredicates()? getInferredPredicates()?
   * TODO: exclude UDFs from predicate propagation? their overloaded variants could
   * have very different semantics
   */
  public ArrayList<Pair<Expr, Boolean>> getBoundPredicates(SlotId slotId, PlanNode node) {
    LOG.trace("getBoundPredicates(" + slotId.toString() + ")");
    TupleId tid = getTupleId(slotId);
    ArrayList<Pair<Expr, Boolean>> result = Lists.newArrayList();
    int maxSlotId = globalState.descTbl.getMaxSlotId().asInt();
    for (int i = 0; i <= maxSlotId; ++i) {
      SlotId equivSlotId = new SlotId(i);

      // skip this slot if we're not allowed to transfer values to slotId
      if (!globalState.valueTransfer[equivSlotId.asInt()][slotId.asInt()]) continue;

      for (ExprId id: globalState.slotConjuncts.get(equivSlotId)) {
        Expr e = globalState.conjuncts.get(id);
        LOG.trace("getBoundPredicates: considering " + e.toSql() + " " + e.debugString());
        if (!e.isBound(equivSlotId)) continue;

        // if this predicate is directly against slotId, check whether 'node' can
        // actually evaluate it (for other slot ids, this is expressed by value
        // transfer)
        // TODO: do we need this or is the following check sufficient?
        //if (equivSlotId.equals(slotId) && !canEvalPredicate(node, e)) continue;

        // we cannot evaluate anything containing '<> is null' from the Where clause if
        // we're being outer-joined: this would alter the outcome of the outer join
        if (globalState.outerJoinedTupleIds.containsKey(tid)
            && e.isWhereClauseConjunct()
            // TODO: also check for IFNULL()
            && e.contains(IsNullPredicate.class)) {
          continue;
        }

        // if e comes out of an OJ's On clause, we need to make sure it's the same
        // as the one that makes slotId's tuple nullable
        // (otherwise e needn't be true when slotId's tuple is set)
        if (globalState.ojClauseByConjunct.containsKey(e.getId())) {
          if (!globalState.outerJoinedTupleIds.containsKey(tid)) continue;
          if (globalState.ojClauseByConjunct.get(e.getId())
              != globalState.outerJoinedTupleIds.get(tid)) {
            continue;
          }
        }

        Expr p;
        if (slotId.equals(equivSlotId)) {
          Preconditions.checkState(e.isBound(slotId));
          p = e;
        } else {
          Expr.SubstitutionMap smap = new Expr.SubstitutionMap();
          smap.addMapping(
              new SlotRef(globalState.descTbl.getSlotDesc(equivSlotId)),
              new SlotRef(globalState.descTbl.getSlotDesc(slotId)));
          p = e.clone(smap);
          // we need to re-analyze in order to create casts, if necessary
          p.unsetIsAnalyzed();
          LOG.trace("new pred: " + p.toSql() + " " + p.debugString());

          try {
            // create casts if needed
            p.analyze(this);
          } catch (ImpalaException exc) {
            // not an executable predicate; ignore
            continue;
          }
        }

        // check if we already created this predicate
        boolean isDuplicate = false;
        for (Pair<Expr, Boolean> pair: result) {
          if (pair.first.equals(p)) {
            isDuplicate = true;
            break;
          }
        }
        if (isDuplicate) continue;

        // predicate assignment doesn't hold if:
        // - the application against slotId doesn't transfer the value back to its
        //   originating slot
        // - the original predicate is on an OJ'd table but doesn't originate from
        //   that table's OJ clause's ON clause (if it comes from anywhere but that
        //   ON clause, it needs to be evaluated directly by the join node that
        //   materializes the OJ'd table)
        boolean reverseValueTransfer =
            globalState.valueTransfer[slotId.asInt()][equivSlotId.asInt()];
        TupleId origTupleId = getTupleId(equivSlotId);
        boolean evalByJoin =
            globalState.outerJoinedTupleIds.containsKey(origTupleId)
              && (!globalState.ojClauseByConjunct.containsKey(e.getId())
                  || globalState.ojClauseByConjunct.get(e.getId())
                    != globalState.outerJoinedTupleIds.get(origTupleId));
        LOG.trace("getBoundPredicates: adding " + p.toSql() + " " + p.debugString());
        result.add(new Pair<Expr, Boolean>(p, reverseValueTransfer && ! evalByJoin));
      }
    }
    return result;
  }

  private TupleId getTupleId(SlotId slotId) {
    return globalState.descTbl.getSlotDesc(slotId).getParent().getId();
  }

  public void registerValueTransfer(SlotId id1, SlotId id2) {
    globalState.registeredValueTransfers.add(new Pair(id1, id2));
  }

  public boolean isOuterJoined(TupleId tid) {
    return globalState.outerJoinedTupleIds.containsKey(tid);
  }

  /**
   * Populate globalState.valueTransfer based on the registered equi-join predicates
   * of the form <slotref> = <slotref>.
   */
  private void computeValueTransferGraph() {
    int numSlots = globalState.descTbl.getMaxSlotId().asInt() + 1;
    globalState.valueTransfer = new boolean[numSlots][numSlots];
    for (int i = 0; i < numSlots; ++i) {
      Arrays.fill(globalState.valueTransfer[i], false);
      globalState.valueTransfer[i][i] = true;
    }
    for (Pair<SlotId, SlotId> t: globalState.registeredValueTransfers) {
      Preconditions.checkState(t.first.asInt() < numSlots);
      Preconditions.checkState(t.second.asInt() < numSlots);
      globalState.valueTransfer[t.first.asInt()][t.second.asInt()] = true;
    }
    Set<ExprId> analyzedIds = Sets.newHashSet();

    // transform equality predicates into a transfer graph
    for (ExprId id: globalState.conjuncts.keySet()) {
      LOG.trace("check id " + Integer.toString(id.asInt()));
      if (!analyzedIds.add(id)) continue;

      Expr e = globalState.conjuncts.get(id);
      Preconditions.checkState(e != null);
      if (!(e instanceof BinaryPredicate)) continue;
      BinaryPredicate p = (BinaryPredicate) e;
      if (p.getOp() != BinaryPredicate.Operator.EQ) continue;
      LOG.trace("check " + p.toSql() + " " + p.debugString());
      Pair<SlotId, SlotId> slotIds = p.getEqSlots();
      if (slotIds == null) continue;
      TableRef tblRef = globalState.ojClauseByConjunct.get(id);
      Preconditions.checkState(tblRef == null || tblRef.getJoinOp().isOuterJoin());
      if (tblRef == null) {
        // this eq predicate doesn't involve any outer join, ie, it is true for
        // each result row;
        // value transfer is not legal if the receiving slot is in an enclosed
        // scope of the source slot and the receiving slot's block has a limit
        Analyzer firstBlock = globalState.blockBySlot.get(slotIds.first);
        Analyzer secondBlock = globalState.blockBySlot.get(slotIds.second);
        LOG.trace("value transfer: from " + slotIds.first.toString());
        if (!(secondBlock.hasLimit && secondBlock.ancestors.contains(firstBlock))) {
          globalState.valueTransfer[slotIds.first.asInt()][slotIds.second.asInt()] =
              true;
        }
        if (!(firstBlock.hasLimit && firstBlock.ancestors.contains(secondBlock))) {
          globalState.valueTransfer[slotIds.second.asInt()][slotIds.first.asInt()] =
              true;
        }
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

      if (tblRef.getJoinOp() == JoinOperator.FULL_OUTER_JOIN) {
        // full outer joins don't guarantee any value transfer
        continue;
      }

      // value transfer is not legal if the receiving slot is in an enclosed
      // scope of the source slot and the receiving slot's block has a limit
      Analyzer innerBlock = globalState.blockBySlot.get(innerSlot);
      Analyzer outerBlock = globalState.blockBySlot.get(outerSlot);
      if (tblRef.getJoinOp() == JoinOperator.LEFT_OUTER_JOIN) {
        if (!(outerBlock.hasLimit && outerBlock.ancestors.contains(innerBlock))) {
          globalState.valueTransfer[outerSlot.asInt()][innerSlot.asInt()] = true;
        }
      } else if (tblRef.getJoinOp() == JoinOperator.RIGHT_OUTER_JOIN) {
        if (!(innerBlock.hasLimit && innerBlock.ancestors.contains(outerBlock))) {
          globalState.valueTransfer[innerSlot.asInt()][outerSlot.asInt()] = true;
        }
      }
    }

    // compute the transitive closure
    boolean changed = false;
    do {
      changed = false;
      for (int i = 0; i < numSlots; ++i) {
        for (int j = 0; j < numSlots; ++j) {
          for (int k = 0; k < numSlots; ++k) {
            if (globalState.valueTransfer[i][j] && globalState.valueTransfer[j][k]
                && !globalState.valueTransfer[i][k]) {
              globalState.valueTransfer[i][k] = true;
              changed = true;
            }
          }
        }
      }
    } while (changed);

    // TODO: remove
    for (int i = 0; i < numSlots; ++i) {
      List<String> strings = Lists.newArrayList();
      for (int j = 0; j < numSlots; ++j) {
        if (i != j && globalState.valueTransfer[i][j]) strings.add(Integer.toString(j));
      }
      if (!strings.isEmpty()) {
        LOG.trace("transfer from " + Integer.toString(i) + " to: "
            + Joiner.on(" ").join(strings));
      }
    }
  }

  public void computeEquivClasses() {
    computeValueTransferGraph();

    // we start out by assigning each slot to its own equiv class
    int numSlots = globalState.descTbl.getMaxSlotId().asInt() + 1;
    IdGenerator<EquivalenceClassId> equivClassIdGenerator =
        new IdGenerator<EquivalenceClassId>();
    for (int i = 0; i < numSlots; ++i) {
      EquivalenceClassId id = new EquivalenceClassId(equivClassIdGenerator);
      globalState.equivClassMembers.put(id, Lists.newArrayList(new SlotId(i)));
    }

    // merge two classes if there is a value transfer between all members of the
    // combined class; do this until there's nothing left to merge
    boolean merged;
    do {
      merged = false;
      for (Map.Entry<EquivalenceClassId, ArrayList<SlotId>> e1:
          globalState.equivClassMembers.entrySet()) {
        for (Map.Entry<EquivalenceClassId, ArrayList<SlotId>> e2:
            globalState.equivClassMembers.entrySet()) {
          if (e1.getKey() == e2.getKey()) continue;
          List<SlotId> class1Members = e1.getValue();
          if (class1Members.isEmpty()) continue;
          List<SlotId> class2Members = e2.getValue();
          if (class2Members.isEmpty()) continue;

          // check whether we can transfer values between all members
          boolean canMerge = true;
          for (SlotId class1Slot: class1Members) {
            for (SlotId class2Slot: class2Members) {
              if (!globalState.valueTransfer[class1Slot.asInt()][class2Slot.asInt()]
                  && !globalState.valueTransfer[class2Slot.asInt()][class1Slot.asInt()]) {
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
    for (EquivalenceClassId id: globalState.equivClassMembers.keySet()) {
      for (SlotId slotId: globalState.equivClassMembers.get(id)) {
        globalState.equivClassBySlotId.put(slotId, id);
      }
    }

    // TODO: remove
    for (EquivalenceClassId id: globalState.equivClassMembers.keySet()) {
      List<SlotId> members = globalState.equivClassMembers.get(id);
      if (members.isEmpty()) continue;
      List<String> strings = Lists.newArrayList();
      for (SlotId slotId: members) {
        strings.add(slotId.toString());
      }
      LOG.trace("equiv class: id=" + id.toString() + " members=("
          + Joiner.on(" ").join(strings) + ")");
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
    EquivalenceClassId classId = globalState.equivClassBySlotId.get(slotId);
    for (SlotId memberId: globalState.equivClassMembers.get(classId)) {
      if (tupleIds.contains(
          globalState.descTbl.getSlotDesc(memberId).getParent().getId())) {
        equivSlotIds.add(memberId);
      }
    }
  }

  public EquivalenceClassId getEquivClassId(SlotId slotId) {
    return globalState.equivClassBySlotId.get(slotId);
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
    Preconditions.checkNotNull(globalState.equivClassBySlotId.get(aSlot.getSlotId()));
    Preconditions.checkNotNull(globalState.equivClassBySlotId.get(bSlot.getSlotId()));
    // Check whether aSlot and bSlot are in the same equivalence class.
    return globalState.equivClassBySlotId.get(aSlot.getSlotId()).equals(
        globalState.equivClassBySlotId.get(bSlot.getSlotId()));
  }

  /**
   * Mark predicates as assigned.
   */
  public void markConjunctsAssigned(List<Expr> conjuncts) {
    if (conjuncts == null) return;
    for (Expr p: conjuncts) {
      globalState.assignedConjuncts.add(p.getId());
      LOG.trace("markAssigned " + p.toSql() + " " + p.debugString());
    }
  }

  /**
   * Mark predicate as assigned.
   */
  public void markConjunctAssigned(Expr conjunct) {
    LOG.trace("markAssigned " + conjunct.toSql() + " " + conjunct.debugString());
    globalState.assignedConjuncts.add(conjunct.getId());
  }

  public boolean isConjunctAssigned(Expr conjunct) {
    return globalState.assignedConjuncts.contains(conjunct.getId());
  }

  public Set<ExprId> getAssignedConjuncts() {
    return Sets.newHashSet(globalState.assignedConjuncts);
  }

  public void setAssignedConjuncts(Set<ExprId> assigned) {
    globalState.assignedConjuncts = Sets.newHashSet(assigned);
  }

  /**
   * Return true if there's at least one unassigned non-auxiliary conjunct.
   */
  public boolean hasUnassignedConjuncts() {
    for (ExprId id: globalState.conjuncts.keySet()) {
      if (globalState.assignedConjuncts.contains(id)) continue;
      Expr e = globalState.conjuncts.get(id);
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
    globalState.descTbl.markSlotsMaterialized(slotIds);
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
  public PrimitiveType getCompatibleType(PrimitiveType lastCompatibleType,
      Expr lastCompatibleExpr, Expr expr)
      throws AnalysisException {
    PrimitiveType newCompatibleType;
    if (lastCompatibleType == null) {
      newCompatibleType = expr.getType();
    } else {
      newCompatibleType =
          PrimitiveType.getAssignmentCompatibleType(lastCompatibleType, expr.getType());
    }
    if (newCompatibleType == PrimitiveType.INVALID_TYPE) {
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
  public PrimitiveType castAllToCompatibleType(List<Expr> exprs)
      throws AnalysisException, AuthorizationException {
    // Determine compatible type of exprs.
    Expr lastCompatibleExpr = exprs.get(0);
    PrimitiveType compatibleType = null;
    for (int i = 0; i < exprs.size(); ++i) {
      exprs.get(i).analyze(this);
      compatibleType = getCompatibleType(compatibleType, lastCompatibleExpr,
          exprs.get(i));
    }
    // Add implicit casts if necessary.
    for (int i = 0; i < exprs.size(); ++i) {
      if (exprs.get(i).getType() != compatibleType) {
        Expr castExpr = exprs.get(i).castTo(compatibleType);
        exprs.set(i, castExpr);
      }
    }
    return compatibleType;
  }

  public Map<String, ViewRef> getWithClauseViews() {
    return withClauseViews;
  }

  public String getDefaultDb() {
    return globalState.defaultDb;
  }

  public User getUser() {
    return user;
  }

  public TQueryGlobals getQueryGlobals() {
    return globalState.queryGlobals;
  }

  /**
   * Returns a list of the successful catalog object access events. Does not include
   * accesses that failed due to AuthorizationExceptions. In general, if analysis
   * fails for any reason this list may be incomplete.
   */
  public List<TAccessEvent> getAccessEvents() { return globalState.accessEvents; }
  public void addAccessEvent(TAccessEvent event) { globalState.accessEvents.add(event); }

  /**
   * Returns the Catalog Table object for the TableName at the given Privilege level.
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
      if (addAccessEvent) {
        // Add an audit event for this access
        TCatalogObjectType objectType = TCatalogObjectType.TABLE;
        if (table instanceof View) objectType = TCatalogObjectType.VIEW;
        globalState.accessEvents.add(new TAccessEvent(
            tableName.toString(), objectType, privilege.toString()));
      }
    } catch (DatabaseNotFoundException e) {
      throw new AnalysisException(DB_DOES_NOT_EXIST_ERROR_MSG + tableName.getDb());
    } catch (TableNotFoundException e) {
      throw new AnalysisException(TBL_DOES_NOT_EXIST_ERROR_MSG + tableName.toString());
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
    globalState.accessEvents.add(new TAccessEvent(
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

  public void setIsExplain(boolean isExplain) { globalState.isExplain = isExplain; }
  public boolean isExplain() { return globalState.isExplain; }
  public void setUseHiveColLabels(boolean useHiveColLabels) {
    globalState.useHiveColLabels = useHiveColLabels;
  }
  public boolean useHiveColLabels() { return globalState.useHiveColLabels; }

  public boolean hasLimit() { return hasLimit; }
  public void setHasLimit(boolean hasLimit) { this.hasLimit = hasLimit; }

  public List<Expr> getConjuncts() {
    return new ArrayList(globalState.conjuncts.values());
  }

  public int incrementCallDepth() { return ++callDepth; }
  public int decrementCallDepth() { return --callDepth; }
  public int getCallDepth() { return callDepth; }
}
