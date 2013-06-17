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
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.DatabaseNotFoundException;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.InlineView;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.TableLoadingException;
import com.cloudera.impala.catalog.TableNotFoundException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.thrift.TQueryGlobals;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Repository of analysis state for single select block.
 *
 * All conjuncts are assigned a unique id when initially registered, and all registered
 * conjuncts are referenced by their id (ie, there are no containers other than the one
 * holding the referenced conjuncts), to make substitute() simple.
 */
public class Analyzer {
  // Common analysis error messages
  public final static String DB_DOES_NOT_EXIST_ERROR_MSG = "Database does not exist: ";
  public final static String DB_ALREADY_EXISTS_ERROR_MSG = "Database already exists: ";
  public final static String TBL_DOES_NOT_EXIST_ERROR_MSG = "Table does not exist: ";
  public final static String TBL_ALREADY_EXISTS_ERROR_MSG = "Table already exists: ";

  private final static Logger LOG = LoggerFactory.getLogger(Analyzer.class);

  private final DescriptorTable descTbl;
  private final Catalog catalog;
  private final String defaultDb;
  private final User user;
  private final IdGenerator<ExprId> conjunctIdGenerator;
  private final TQueryGlobals queryGlobals;

  // An analyzer is a repository for a single select block. A select block can be a top
  // level select statement, or a inline view select block. An inline
  // view has its own Analyzer. parentAnalyzer is the analyzer of the enclosing
  // (or parent) select block analyzer. For top level select statement, parentAnalyzer is
  // null.
  private final Analyzer parentAnalyzer;

  // map from lowercase table alias to a virtual-view definition of a WITH clause.
  private final Map<String, VirtualViewRef> withClauseViews = Maps.newHashMap();

  // map from lowercase table alias to descriptor.
  private final Map<String, TupleDescriptor> aliasMap = Maps.newHashMap();

  // map from lowercase qualified column name ("alias.col") to descriptor
  private final Map<String, SlotDescriptor> slotRefMap = Maps.newHashMap();

  // all registered conjuncts (map from id to Predicate)
  private final Map<ExprId, Expr> conjuncts = Maps.newHashMap();

  // map from tuple id to list of conjuncts referencing tuple
  private final Map<TupleId, List<ExprId> > tuplePredicates = Maps.newHashMap();

  // map from slot id to list of conjuncts referencing slot
  private final Map<SlotId, List<ExprId> > slotPredicates = Maps.newHashMap();

  // eqJoinPredicates[tid] contains all conjuncts of the form
  // "<lhs> = <rhs>" in which either lhs or rhs is fully bound by tid
  // and the other side is not bound by tid (ie, predicates that express equi-join
  // conditions between two tablerefs).
  // A predicate such as "t1.a = t2.b" has two entries, one for 't1' and
  // another one for 't2'.
  private final Map<TupleId, List<ExprId> > eqJoinConjuncts = Maps.newHashMap();

  // set of conjuncts that have been assigned to some PlanNode
  private final Set<ExprId> assignedConjuncts =
      Collections.newSetFromMap(new IdentityHashMap<ExprId, Boolean>());

  // map from outer-joined tuple id, ie, one that is nullable in this select block,
  // to the last Join clause (represented by its rhs table ref) that outer-joined it
  private final Map<TupleId, TableRef> outerJoinedTupleIds = Maps.newHashMap();

  // map from right-hand side table ref of an outer join to the list of
  // conjuncts in its On clause
  private final Map<TableRef, List<ExprId>> conjunctsByOjClause = Maps.newHashMap();

  // map from registered conjunct to its containing outer join On clause (represented
  // by its right-hand side table ref); only conjuncts that can only be correctly
  // evaluated by the originating outer join are registered here
  private final Map<ExprId, TableRef> ojClauseByConjunct = Maps.newHashMap();

  // all conjuncts of the Where clause
  private final Set<ExprId> whereClauseConjuncts = Sets.newHashSet();

  /**
   * Analyzer constructor for AnalyzerTest.
   * @param catalog
   */
  public Analyzer(Catalog catalog) {
    this(catalog, Catalog.DEFAULT_DB, new User(System.getProperty("user.name")),
        createQueryGlobals());
  }

  public Analyzer(Catalog catalog, String defaultDb, User user,
        TQueryGlobals queryGlobals) {
    this.parentAnalyzer = null;
    this.catalog = catalog;
    this.descTbl = new DescriptorTable();
    this.defaultDb = defaultDb;
    this.user = user;
    this.conjunctIdGenerator = new IdGenerator<ExprId>();
    this.queryGlobals = queryGlobals;
  }

  /**
   * Analyzer constructor for nested select block. Catalog and DescriptorTable is
   * inherited from the parentAnalyzer.
   * @param parentAnalyzer the analyzer of the enclosing select block
   */
  public Analyzer(Analyzer parentAnalyzer) {
    this.parentAnalyzer = parentAnalyzer;
    this.catalog = parentAnalyzer.catalog;
    this.descTbl = parentAnalyzer.descTbl;
    this.defaultDb = parentAnalyzer.defaultDb;
    this.user = parentAnalyzer.user;
    // make sure we don't create duplicate ids across entire stmt
    this.conjunctIdGenerator = parentAnalyzer.conjunctIdGenerator;
    this.queryGlobals = parentAnalyzer.queryGlobals;
  }

  /**
   * Substitute analyzer's internal expressions (conjuncts) with the given substitution
   * map
   */
  public void substitute(Expr.SubstitutionMap sMap) {
    for (ExprId id: conjuncts.keySet()) {
      conjuncts.put(id, (Predicate) conjuncts.get(id).substitute(sMap));
    }
  }

  /**
   * Replaces BaseTableRefs in tblRefs whose alias matches a view registered in
   * this analyzer or its parent analyzers with a clone of the matching inline view.
   * The cloned inline view inherits the context-dependent attributes such as the
   * on-clause, join hints, etc. from the original BaseTableRef.
   *
   * Matches views from the inside out, i.e., we first look
   * in this analyzer then in the parentAnalyzer then and its parent, etc.
   *
   * This method is used for substituting views from WITH clauses.
   */
  public void substituteBaseTablesWithMatchingViews(List<TableRef> tblRefs) {
    for (int i = 0; i < tblRefs.size(); ++i) {
      if (!(tblRefs.get(i) instanceof BaseTableRef)) continue;
      BaseTableRef tblRef = (BaseTableRef) tblRefs.get(i);
      VirtualViewRef viewDefinition = findViewDefinition(tblRef);
      if (viewDefinition == null) continue;

      // Instantiate the virtual view to replace the original BaseTableRef.
      VirtualViewRef viewInstantiation = viewDefinition.instantiate(tblRef);
      tblRefs.set(i, viewInstantiation);
    }
  }

  /**
   * Searches the hierarchy of analyzers bottom-up for a registered (non-inline) view
   * whose alias matches the table name of the given BaseTableRef. Returns the
   * InlineViewRef from the innermost scope (analyzer).
   * Returns null if no matching views were found.
   */
  public VirtualViewRef findViewDefinition(BaseTableRef ref) {
    // Do not consider views from the WITH clause if the table name is fully qualified,
    // or if view replacement was explicitly disabled.
    if (!ref.getName().isFullyQualified() && ref.isReplacableByView()) {
      Analyzer analyzer = this;
      do {
        String baseTableName = ref.getName().getTbl().toLowerCase();
        VirtualViewRef view = analyzer.withClauseViews.get(baseTableName);
        if (view != null) return view;
        analyzer = analyzer.parentAnalyzer;
      } while (analyzer != null);
    }
    return null;
    // TODO: Search for matching views from the global scope, i.e., the catalog,
    // when Impala supports virtual views (IMPALA-372).
  }

  /**
   * Adds view to this analyzer's withClauseViews. Throws an exception if a view
   * definition with the same alias has already been registered.
   */
  public void registerWithClauseView(VirtualViewRef ref) throws AnalysisException {
    if (withClauseViews.put(ref.getAlias().toLowerCase(), ref) != null) {
      throw new AnalysisException(
          String.format("Duplicate table alias: '%s'", ref.getAlias()));
    }
  }

  /**
   * Checks that 'name' references an existing table and that alias
   * isn't already registered. Creates and returns an empty TupleDescriptor
   * and registers it against alias. If alias is empty, register
   * "name.tbl" and "name.db.tbl" as aliases.
   * @param ref the BaseTableRef to be registered
   * @return newly created TupleDescriptor
   */
  public TupleDescriptor registerBaseTableRef(BaseTableRef ref)
      throws AnalysisException, AuthorizationException {
    String lookupAlias = ref.getAlias().toLowerCase();
    if (aliasMap.containsKey(lookupAlias)) {
      throw new AnalysisException("Duplicate table alias: '" + lookupAlias + "'");
    }

    Table tbl = getTable(ref.getName(), ref.getPrivilegeRequirement());
    TupleDescriptor result = descTbl.createTupleDescriptor();
    result.setTable(tbl);
    aliasMap.put(lookupAlias, result);
    return result;
  }

  /**
   * Register tids as being outer-joined by Join clause represented by rhsRef.
   */
  public void registerOuterJoinedTids(List<TupleId> tids, TableRef rhsRef) {
    for (TupleId tid: tids) {
      outerJoinedTupleIds.put(tid, rhsRef);
    }
    //LOG.info("outerJoinedTids: " + outerJoinedTupleIds.toString());
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
   *
   * @param ref the InlineView to be registered
   */
  public TupleDescriptor registerInlineViewRef(InlineViewRef ref)
      throws AnalysisException {
    String lookupAlias = ref.getAlias().toLowerCase();
    if (aliasMap.containsKey(lookupAlias)) {
      throw new AnalysisException("Duplicate table alias: '" + lookupAlias + "'");
    }

    // Create a fake catalog table for the inline view
    QueryStmt viewStmt = ref.getViewStmt();
    InlineView inlineView = new InlineView(ref.getAlias());
    for (int i = 0; i < viewStmt.getColLabels().size(); ++i) {
      // inline view select statement has been analyzed. Col label should be filled.
      Expr selectItemExpr = viewStmt.getResultExprs().get(i);
      String colAlias = viewStmt.getColLabels().get(i);

      // inline view col cannot have duplicate name
      if (inlineView.getColumn(colAlias) != null) {
        throw new AnalysisException("duplicated inline view column alias: '" +
            colAlias + "'" + " in inline view " + "'" + ref.getAlias() + "'");
      }

      // create a column and add it to the inline view
      Column col = new Column(colAlias, selectItemExpr.getType(), i);
      inlineView.addColumn(col);
    }

    // Register the inline view
    TupleDescriptor result = descTbl.createTupleDescriptor();
    result.setIsMaterialized(false);
    result.setTable(inlineView);
    aliasMap.put(lookupAlias, result);
    return result;
  }

  /**
   * Return descriptor of registered table/alias.
   * @param name
   * @return  null if not registered.
   */
  public TupleDescriptor getDescriptor(TableName name) {
    return aliasMap.get(name.toString().toLowerCase());
  }

  public TupleDescriptor getTupleDesc(TupleId id) {
    return descTbl.getTupleDesc(id);
  }

  /**
   * Given a "table alias"."column alias", return the SlotDescriptor
   * @param qualifiedColumnName table qualified column name
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
    if (result != null) {
      return result;
    }
    result = descTbl.addSlotDescriptor(tupleDesc);
    result.setColumn(col);
    slotRefMap.put(alias + "." + col.getName(), result);
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
      ojConjuncts = conjunctsByOjClause.get(rhsRef);
      if (ojConjuncts == null) {
        ojConjuncts = Lists.newArrayList();
        conjunctsByOjClause.put(rhsRef, ojConjuncts);
      }
    }
    for (Expr conjunct: e.getConjuncts()) {
      registerConjunct(conjunct);
      if (rhsRef != null) {
        ojClauseByConjunct.put(conjunct.getId(), rhsRef);
        ojConjuncts.add(conjunct.getId());
        //LOG.info(conjunctsByOjClause.toString());
      }
      if (fromWhereClause) {
        whereClauseConjuncts.add(conjunct.getId());
      }
    }
  }

  /**
   * Register individual conjunct with all tuple and slot ids it references
   * and with the global conjunct list.
   */
  private void registerConjunct(Expr e) {
    // this conjunct would already have an id assigned if it is being re-registered
    // in a subqery analyzer
    if (e.getId() == null) {
      e.setId(new ExprId(conjunctIdGenerator));
    }
    conjuncts.put(e.getId(), e);
    //LOG.info("registered conjunct " + p.getId().toString() + ": " + p.toSql());

    ArrayList<TupleId> tupleIds = Lists.newArrayList();
    ArrayList<SlotId> slotIds = Lists.newArrayList();
    e.getIds(tupleIds, slotIds);

    // update tuplePredicates
    for (TupleId id : tupleIds) {
      if (!tuplePredicates.containsKey(id)) {
        List<ExprId> conjunctIds = Lists.newArrayList();
        conjunctIds.add(e.getId());
        tuplePredicates.put(id, conjunctIds);
      } else {
        tuplePredicates.get(id).add(e.getId());
      }
    }

    // update slotPredicates
    for (SlotId id : slotIds) {
      if (!slotPredicates.containsKey(id)) {
        List<ExprId> conjunctIds = Lists.newArrayList();
        conjunctIds.add(e.getId());
        slotPredicates.put(id, conjunctIds);
      } else {
        slotPredicates.get(id).add(e.getId());
      }
    }

    // check whether this is an equi-join predicate, ie, something of the
    // form <expr1> = <expr2> where at least one of the exprs is bound by
    // exactly one tuple id
    if (!(e instanceof BinaryPredicate)) {
      return;
    }
    BinaryPredicate binaryPred = (BinaryPredicate) e;
    if (binaryPred.getOp() != BinaryPredicate.Operator.EQ) {
      return;
    }
    if (tupleIds.size() != 2) {
      return;
    }

    // examine children and update eqJoinConjuncts
    for (int i = 0; i < 2; ++i) {
      List<TupleId> lhsTupleIds = Lists.newArrayList();
      binaryPred.getChild(i).getIds(lhsTupleIds, null);
      if (lhsTupleIds.size() == 1) {
        if (!eqJoinConjuncts.containsKey(lhsTupleIds.get(0))) {
          List<ExprId> conjunctIds = Lists.newArrayList();
          conjunctIds.add(e.getId());
          eqJoinConjuncts.put(lhsTupleIds.get(0), conjunctIds);
        } else {
          eqJoinConjuncts.get(lhsTupleIds.get(0)).add(e.getId());
        }
        binaryPred.setIsEqJoinConjunct(true);
      }
    }
  }

  /**
   * Return all unassigned registered conjuncts that are fully bound by given
   * list of tuple ids and are not tied to an Outer Join clause.
   * @return possibly empty list of Predicates
   */
  public List<Expr> getUnassignedConjuncts(List<TupleId> tupleIds) {
    List<Expr> result = Lists.newArrayList();
    for (Expr e: conjuncts.values()) {
      if (e.isBound(tupleIds) && !assignedConjuncts.contains(e.getId())
          && !ojClauseByConjunct.containsKey(e.getId())) {
        result.add(e);
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
    List<ExprId> candidates = conjunctsByOjClause.get(ref);
    if (candidates == null) {
      return result;
    }
    for (ExprId conjunctId: candidates) {
      if (!assignedConjuncts.contains(conjunctId)) {
        Expr e = conjuncts.get(conjunctId);
        Preconditions.checkState(e != null);
        result.add(e);
      }
    }
    return result;
  }

  /**
   * Return rhs ref of last Join clause that outer-joined id.
   */
  public TableRef getLastOjClause(TupleId id) {
    return outerJoinedTupleIds.get(id);
  }

  public boolean isWhereClauseConjunct(Expr e) {
    return whereClauseConjuncts.contains(e.getId());
  }

  /**
   * Return slot descriptor corresponding to column referenced in the context of
   * tupleDesc, or null if no such reference exists.
   */
  public SlotDescriptor getColumnSlot(TupleDescriptor tupleDesc, Column col) {
    for (SlotDescriptor slotDesc: tupleDesc.getSlots()) {
      if (slotDesc.getColumn() == col) {
        return slotDesc;
      }
    }
    return null;
  }

  public DescriptorTable getDescTbl() {
    return descTbl;
  }

  public Catalog getCatalog() {
    return catalog;
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
    List<ExprId> conjunctIds = eqJoinConjuncts.get(id);
    if (conjunctIds == null) {
      return null;
    }
    List<Expr> result = Lists.newArrayList();
    List<ExprId> ojClauseConjuncts = null;
    if (rhsRef != null) {
      Preconditions.checkState(rhsRef.getJoinOp().isOuterJoin());
      ojClauseConjuncts = conjunctsByOjClause.get(rhsRef);
    }
    for (ExprId conjunctId: conjunctIds) {
      Expr e = conjuncts.get(conjunctId);
      Preconditions.checkState(e != null);
      if (ojClauseConjuncts != null) {
        if (ojClauseConjuncts.contains(conjunctId)) {
          result.add(e);
        }
      } else {
        result.add(e);
      }
    }
    return result;
  }

  /**
   * Mark predicates as assigned.
   */
  public void markConjunctsAssigned(List<Expr> conjuncts) {
    if (conjuncts == null) {
      return;
    }
    for (Expr p: conjuncts) {
      assignedConjuncts.add(p.getId());
    }
  }

  /**
   * Mark predicate as assigned.
   */
  public void markConjunctAssigned(Predicate conjunct) {
    assignedConjuncts.add(conjunct.getId());
  }

  /**
   * Return true if there's at least one unassigned conjunct.
   */
  public boolean hasUnassignedConjuncts() {
    return !assignedConjuncts.containsAll(conjuncts.keySet());
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
      throws AnalysisException {
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

  public Map<String, VirtualViewRef> getWithClauseViews() {
    return withClauseViews;
  }

  public String getDefaultDb() {
    return defaultDb;
  }

  public User getUser() {
    return user;
  }

  public TQueryGlobals getQueryGlobals() {
    return queryGlobals;
  }

  /*
   * Returns the Catalog Table object for the TableName at the given Privilege level.
   *
   * If the user does not have sufficient privileges to access the table an
   * AuthorizationException is thrown.
   * If the table or the db does not exist in the Catalog, an AnalysisError is thrown.
   */
  public Table getTable(TableName tableName, Privilege privilege)
      throws AuthorizationException, AnalysisException {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(privilege);
    Table table = null;
    tableName = new TableName(getTargetDbName(tableName), tableName.getTbl());

    // This may trigger a metadata load, in which case we want to return the errors as
    // AnalysisExceptions.
    try {
      table =
          catalog.getTable(tableName.getDb(), tableName.getTbl(), getUser(), privilege);
    } catch (DatabaseNotFoundException e) {
      throw new AnalysisException(DB_DOES_NOT_EXIST_ERROR_MSG + tableName.getDb());
    } catch (TableNotFoundException e) {
      throw new AnalysisException(TBL_DOES_NOT_EXIST_ERROR_MSG + tableName.toString());
    } catch (TableLoadingException e) {
      throw new AnalysisException(String.format("Failed to load metadata for table: %s",
          tableName), e);
    }
    Preconditions.checkNotNull(table);
    return table;
  }

  /*
   * Returns the Catalog Db object for the given database name at the given
   * Privilege level.
   *
   * If the user does not have sufficient privileges to access the database an
   * AuthorizationException is thrown.
   * If the database does not exist in the catalog an AnalysisError is thrown.
   */
  public Db getDb(String dbName, Privilege privilege)
      throws AnalysisException, AuthorizationException {
    Db db = catalog.getDb(dbName, getUser(), privilege);
    if (db == null) {
      throw new AnalysisException(DB_DOES_NOT_EXIST_ERROR_MSG + dbName);
    }
    return db;
  }

  /*
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
      return catalog.dbContainsTable(dbName, tableName, getUser(), privilege);
    } catch (DatabaseNotFoundException e) {
      throw new AnalysisException(DB_DOES_NOT_EXIST_ERROR_MSG + dbName);
    }
  }

  /**
   * Create query global parameters to be set in each TPlanExecRequest.
   */
  public static TQueryGlobals createQueryGlobals() {
    SimpleDateFormat formatter =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    TQueryGlobals queryGlobals = new TQueryGlobals();
    Calendar currentDate = Calendar.getInstance();
    String nowStr = formatter.format(currentDate.getTime());
    queryGlobals.setNow_string(nowStr);
    return queryGlobals;
  }

  /*
   * If the table name is fully qualified, the database from the TableName object will
   * be returned. Otherwise the default analyzer database will be returned.
   */
  public String getTargetDbName(TableName tableName) {
    return tableName.isFullyQualified() ? tableName.getDb() : getDefaultDb();
  }
}
