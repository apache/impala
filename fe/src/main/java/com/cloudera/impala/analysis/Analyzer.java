// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.InlineView;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Repository of analysis state for single select block..
 *
 */
public class Analyzer {
  private final DescriptorTable descTbl;
  private final Catalog catalog;
  private final String defaultDb;

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

  // map from tuple id to list of predicates referencing tuple
  private final Map<TupleId, List<Predicate> > tuplePredicates = Maps.newHashMap();

  // map from slot id to list of predicates referencing slot
  private final Map<SlotId, List<Predicate> > slotPredicates = Maps.newHashMap();

  // eqJoinPredicates[tid] contains all predicates of the form
  // "<lhs> = <rhs>" in which either lhs or rhs is fully bound by tid
  // and the other side is not bound by tid (ie, predicates that express equi-join
  // conditions between two tablerefs).
  // A predicate such as "t1.a = t2.b" has two entries, one for 't1' and
  // another one for 't2'.
  private final Map<TupleId, List<Predicate> > eqJoinPredicates = Maps.newHashMap();

  // list of all registered conjuncts
  private final List<Predicate> conjuncts = Lists.newArrayList();

  // set of conjuncts that have been assigned to some PlanNode
  private final Set<Predicate> assignedConjuncts =
      Collections.newSetFromMap(new IdentityHashMap<Predicate, Boolean>());

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
  }

  /**
   * Substitute analyzer's internal expressions (eqJoinPredicates and conjuncts) with the
   * given substitution map
   */
  public void substitute(Expr.SubstitutionMap sMap) {
    Expr.substituteList(conjuncts, sMap);

    // conjuncts list is substituted, but the original list element might not be.
    // so, we need to substitute eqJoinPredicates.
    for (List<Predicate> eqJoinPred: eqJoinPredicates.values()) {
      Expr.substituteList(eqJoinPred, sMap);
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
      throw new AnalysisException("duplicate table alias: '" + lookupAlias + "'");
    }

    // Always register the ref under the unqualified table name (if there's no
    // explicit alias), but the *table* must be fully qualified.
    String dbName = 
        ref.getName().getDb() == null ? getDefaultDb() : ref.getName().getDb();
    Db db = catalog.getDb(dbName);
    if (db == null) {
      throw new AnalysisException("unknown db: '" + ref.getName().getDb() + "'");
    }
    Table tbl = db.getTable(ref.getName().getTbl());
    if (tbl == null) {
      throw new AnalysisException("unknown table: '" + ref.getName().getTbl() + "'");
    }
    TupleDescriptor result = descTbl.createTupleDescriptor();
    result.setTable(tbl);
    aliasMap.put(lookupAlias, result);

    return result;
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
    int numMatches = 0;
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
   * Register all conjuncts in a list of predicates.
   */
  public void registerConjuncts(List<Predicate> l) {
    for (Predicate p: l) {
      registerConjuncts(p);
    }
  }

  /**
   * Register all conjuncts that make up the predicate.
   */
  public void registerConjuncts(Predicate p) {
    List<Predicate> conjuncts = p.getConjuncts();
    for (Predicate conjunct: conjuncts) {
      registerConjunct(conjunct);
    }
  }

  /**
   * Register individual conjunct with all tuple and slot ids it references
   * and with the global conjunct list.
   */
  private void registerConjunct(Predicate p) {
    conjuncts.add(p);

    ArrayList<TupleId> tupleIds = Lists.newArrayList();
    ArrayList<SlotId> slotIds = Lists.newArrayList();
    p.getIds(tupleIds, slotIds);

    // update tuplePredicates
    for (TupleId id : tupleIds) {
      if (!tuplePredicates.containsKey(id)) {
        List<Predicate> predList = Lists.newLinkedList();
        predList.add(p);
        tuplePredicates.put(id, predList);
      } else {
        tuplePredicates.get(id).add(p);
      }
    }

    // update slotPredicates
    for (SlotId id : slotIds) {
      if (!slotPredicates.containsKey(id)) {
        List<Predicate> predList = Lists.newLinkedList();
        predList.add(p);
        slotPredicates.put(id, predList);
      } else {
        slotPredicates.get(id).add(p);
      }
    }

    // update eqJoinPredicates
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

    // examine children
    for (int i = 0; i < 2; ++i) {
      List<TupleId> lhsTupleIds = Lists.newArrayList();
      binaryPred.getChild(i).getIds(lhsTupleIds, null);
      if (lhsTupleIds.size() == 1) {
        if (!eqJoinPredicates.containsKey(lhsTupleIds.get(0))) {
          List<Predicate> predList = Lists.newLinkedList();
          predList.add(p);
          eqJoinPredicates.put(lhsTupleIds.get(0), predList);
        } else {
          eqJoinPredicates.get(lhsTupleIds.get(0)).add(p);
        }
        binaryPred.setIsEqJoinConjunct(true);
      }
    }
  }

  /**
   * Return all unassigned registered conjuncts that are fully bound by given
   * list of tuple ids.
   * @return possibly empty list of Predicates
   */
  public List<Predicate> getBoundConjuncts(List<TupleId> tupleIds) {
    List<Predicate> result = Lists.newArrayList();
    for (int i = 0; i < conjuncts.size(); ++i) {
      Predicate pred = conjuncts.get(i);
      if (pred.isBound(tupleIds) && !assignedConjuncts.contains(pred)) {
        result.add(pred);
      }
    }
    return result;
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

  public List<Predicate> getEqJoinPredicates(TupleId id) {
    return eqJoinPredicates.get(id);
  }

  /**
   * Mark those predicates as assigned.
   * @param predicates mark the given list of predicate as assigned
   */
  public void markConjunctsAssigned(List<Predicate> predicates) {
    assignedConjuncts.addAll(predicates);
  }

  /**
   * Return true if there's at least one unassigned conjunct.
   * @return true if there's at least one unassigned conjunct
   */
  public boolean hasUnassignedConjuncts() {
    return !assignedConjuncts.containsAll(conjuncts);
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
