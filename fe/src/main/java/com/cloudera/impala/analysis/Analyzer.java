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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Db.TableLoadingException;
import com.cloudera.impala.catalog.InlineView;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.IdGenerator;
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
  private final static Logger LOG = LoggerFactory.getLogger(Analyzer.class);

  private final DescriptorTable descTbl;
  private final Catalog catalog;
  private final String defaultDb;
  private final IdGenerator<ExprId> conjunctIdGenerator;

  // An analyzer is a repository for a single select block. A select block can be a top
  // level select statement, or a inline view select block. An inline
  // view has its own Analyzer. parentAnalyzer is the analyzer of the enclosing
  // (or parent) select block analyzer. For top level select statement, parentAnalyzer is
  // null.
  private final Analyzer parentAnalyzer;

  // map from lowercase table alias to descriptor.
  private final Map<String, TupleDescriptor> aliasMap = Maps.newHashMap();

  // map from lowercase qualified column name ("alias.col") to descriptor
  private final Map<String, SlotDescriptor> slotRefMap = Maps.newHashMap();

  // all registered conjuncts (map from id to Predicate)
  private final Map<ExprId, Predicate> conjuncts = Maps.newHashMap();

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
   * Analyzer constructor for top level statement.
   * @param catalog
   */
  public Analyzer(Catalog catalog) {
    this(catalog, Catalog.DEFAULT_DB);
  }

  public Analyzer(Catalog catalog, String defaultDb) {
    this.parentAnalyzer = null;
    this.catalog = catalog;
    this.descTbl = new DescriptorTable();
    this.defaultDb = defaultDb;
    this.conjunctIdGenerator = new IdGenerator<ExprId>();
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
    // make sure we don't create duplicate ids across entire stmt
    this.conjunctIdGenerator = parentAnalyzer.conjunctIdGenerator;
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
   * Checks that 'name' references an existing table and that alias
   * isn't already registered. Creates and returns an empty TupleDescriptor
   * and registers it against alias. If alias is empty, register
   * "name.tbl" and "name.db.tbl" as aliases.
   * @param ref the BaseTableRef to be registered
   * @return newly created TupleDescriptor
   * @throws AnalysisException
   */
  public TupleDescriptor registerBaseTableRef(BaseTableRef ref) throws AnalysisException {
    String lookupAlias = ref.getAlias().toLowerCase();
    if (aliasMap.containsKey(lookupAlias)) {
      throw new AnalysisException("Duplicate table alias: '" + lookupAlias + "'");
    }

    // Always register the ref under the unqualified table name (if there's no
    // explicit alias), but the *table* must be fully qualified.
    String dbName =
        ref.getName().getDb() == null ? getDefaultDb() : ref.getName().getDb();
    Db db = catalog.getDb(dbName);
    if (db == null) {
      throw new AnalysisException("unknown db: '" + ref.getName().getDb() + "'");
    }

    Table tbl = null;
    try {
      tbl = db.getTable(ref.getName().getTbl());
    } catch (TableLoadingException e) {
      throw new AnalysisException("Failed to load metadata for table: " +
          ref.getName().getTbl(), e);
    }
    if (tbl == null) {
      throw new AnalysisException("Unknown table: '" + ref.getName().getTbl() + "'");
    }
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
      throw new AnalysisException("duplicate table alias: '" + lookupAlias + "'");
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
   * @param tblName
   * @param colName
   * @throws AnalysisException
   */
  public SlotDescriptor registerColumnRef(TableName tblName, String colName)
      throws AnalysisException {
    String alias;
    if (tblName == null) {
      alias = resolveColumnRef(colName);
      if (alias == null) {
        throw new AnalysisException("couldn't resolve column reference: '" +
            colName + "'");
      }
    } else {
      alias = tblName.toString().toLowerCase();
    }
    TupleDescriptor d = aliasMap.get(alias);
    if (d == null) {
      throw new AnalysisException("unknown table alias: '" + alias + "'");
    }

    Column col = d.getTable().getColumn(colName);
    if (col == null) {
      throw new AnalysisException("unknown column '" + colName +
          "' (table alias '" + alias + "')");
    }

    String key = alias + "." + col.getName();
    SlotDescriptor result = slotRefMap.get(key);
    if (result != null) {
      return result;
    }
    result = descTbl.addSlotDescriptor(d);
    result.setColumn(col);
    slotRefMap.put(alias + "." + col.getName(), result);
    return result;
  }

  /**
   * Resolves column name in context of any of the registered table aliases.
   * Returns null if not found or multiple bindings to different tables exist,
   * otherwise returns the table alias.
   */
  private String resolveColumnRef(String colName) throws AnalysisException {
    String result = null;
    for (Map.Entry<String, TupleDescriptor> entry: aliasMap.entrySet()) {
      Column col = entry.getValue().getTable().getColumn(colName);
      if (col != null) {
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
  public void registerConjuncts(List<Predicate> l) {
    for (Predicate p: l) {
      registerConjuncts(p, null, true);
    }
  }

  /**
   * Register all conjuncts that make up the predicate and assign each conjunct an id.
   * If ref != null, ref is expected to be the right-hand side of an outer join,
   * and the conjuncts of p can only be evaluated by the node implementing that join
   * (p is the On clause).
   */
  public void registerConjuncts(Predicate p, TableRef rhsRef, boolean fromWhereClause) {
    List<ExprId> ojConjuncts = null;
    if (rhsRef != null) {
      Preconditions.checkState(rhsRef.getJoinOp().isOuterJoin());
      ojConjuncts = conjunctsByOjClause.get(rhsRef);
      if (ojConjuncts == null) {
        ojConjuncts = Lists.newArrayList();
        conjunctsByOjClause.put(rhsRef, ojConjuncts);
      }
    }
    for (Predicate conjunct: p.getConjuncts()) {
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
  private void registerConjunct(Predicate p) {
    // this conjunct would already have an id assigned if it is being re-registered
    // in a subqery analyzer
    if (p.getId() == null) {
      p.setId(new ExprId(conjunctIdGenerator));
    }
    conjuncts.put(p.getId(), p);
    //LOG.info("registered conjunct " + p.getId().toString() + ": " + p.toSql());

    ArrayList<TupleId> tupleIds = Lists.newArrayList();
    ArrayList<SlotId> slotIds = Lists.newArrayList();
    p.getIds(tupleIds, slotIds);

    // update tuplePredicates
    for (TupleId id : tupleIds) {
      if (!tuplePredicates.containsKey(id)) {
        List<ExprId> conjunctIds = Lists.newArrayList();
        conjunctIds.add(p.getId());
        tuplePredicates.put(id, conjunctIds);
      } else {
        tuplePredicates.get(id).add(p.getId());
      }
    }

    // update slotPredicates
    for (SlotId id : slotIds) {
      if (!slotPredicates.containsKey(id)) {
        List<ExprId> conjunctIds = Lists.newArrayList();
        conjunctIds.add(p.getId());
        slotPredicates.put(id, conjunctIds);
      } else {
        slotPredicates.get(id).add(p.getId());
      }
    }

    // check whether this is an equi-join predicate, ie, something of the
    // form <expr1> = <expr2> where at least one of the exprs is bound by
    // exactly one tuple id
    if (!(p instanceof BinaryPredicate)) {
      return;
    }
    BinaryPredicate binaryPred = (BinaryPredicate) p;
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
          conjunctIds.add(p.getId());
          eqJoinConjuncts.put(lhsTupleIds.get(0), conjunctIds);
        } else {
          eqJoinConjuncts.get(lhsTupleIds.get(0)).add(p.getId());
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
  public List<Predicate> getUnassignedConjuncts(List<TupleId> tupleIds) {
    //LOG.info("getUnassignedConjuncts()");
    //LOG.info("ojconjuncts:" + ojClauseByConjunct.toString());
    List<Predicate> result = Lists.newArrayList();
    for (Predicate pred: conjuncts.values()) {
      //LOG.info("check pred: " + pred.toSql());
      if (pred.isBound(tupleIds) && !assignedConjuncts.contains(pred.getId())
          && !ojClauseByConjunct.containsKey(pred.getId())) {
        result.add(pred);
      }
    }
    return result;
  }

  /**
   * Return all unassigned conjuncts of the outer join referenced by right-hand side
   * table ref.
   */
  public List<Predicate> getUnassignedOjConjuncts(TableRef ref) {
    //LOG.info("getUnassignedOjConjuncts()");
    Preconditions.checkState(ref.getJoinOp().isOuterJoin());
    List<Predicate> result = Lists.newArrayList();
    //LOG.info(conjunctsByOjClause.toString());
    List<ExprId> candidates = conjunctsByOjClause.get(ref);
    if (candidates == null) {
      return result;
    }
    for (ExprId conjunctId: candidates) {
      if (!assignedConjuncts.contains(conjunctId)) {
        Predicate p = conjuncts.get(conjunctId);
        Preconditions.checkState(p != null);
        result.add(p);
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

  public boolean isWhereClauseConjunct(Predicate p) {
    return whereClauseConjuncts.contains(p.getId());
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
  public List<Predicate> getEqJoinConjuncts(TupleId id, TableRef rhsRef) {
    List<ExprId> conjunctIds = eqJoinConjuncts.get(id);
    if (conjunctIds == null) return null;
    List<Predicate> result = Lists.newArrayList();
    List<ExprId> ojClauseConjuncts = null;
    if (rhsRef != null) {
      Preconditions.checkState(rhsRef.getJoinOp().isOuterJoin());
      ojClauseConjuncts = conjunctsByOjClause.get(rhsRef);
    }
    for (ExprId conjunctId: conjunctIds) {
      Predicate p = conjuncts.get(conjunctId);
      Preconditions.checkState(p != null);
      if (ojClauseConjuncts != null) {
        if (ojClauseConjuncts.contains(conjunctId)) {
          result.add(p);
        }
      } else {
        result.add(p);
      }
    }
    return result;
  }

  /**
   * Mark predicates as assigned.
   */
  public void markConjunctsAssigned(List<Predicate> predicates) {
    if (predicates == null) {
      return;
    }
    for (Predicate p: predicates) {
      assignedConjuncts.add(p.getId());
    }
    //LOG.info("mark assigned: " + assignedConjuncts.toString());
  }

  /**
   * Mark predicate as assigned.
   */
  public void markConjunctAssigned(Predicate conjunct) {
    assignedConjuncts.add(conjunct.getId());
    //LOG.info("mark assigned: " + assignedConjuncts.toString());
  }

  /**
   * Return true if there's at least one unassigned conjunct.
   */
  public boolean hasUnassignedConjuncts() {
    //LOG.info("assigned: " + assignedConjuncts.toString());
    //LOG.info("conjuncts: " + conjuncts.keySet().toString());
    boolean result = !assignedConjuncts.containsAll(conjuncts.keySet());
    if (result) {
      //LOG.info("has unassigned conjuncts");
    }
    return result;
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
   *
   * @param lastCompatibleType
   * @param expr
   * @param lastExprIndex
   * @return
   * @throws AnalysisException
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
   *
   * @param exprs
   * @throws AnalysisException
   */
  public PrimitiveType castAllToCompatibleType(List<Expr> exprs)
      throws AnalysisException {
    // Determine compatible type of exprs.
    Expr lastCompatibleExpr = exprs.get(0);
    PrimitiveType compatibleType = null;
    for (int i = 0; i < exprs.size(); ++i) {
      exprs.get(i).analyze(this);
      compatibleType = getCompatibleType(compatibleType,
          lastCompatibleExpr, exprs.get(i));
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

  public String getDefaultDb() {
    return defaultDb;
  }
}
