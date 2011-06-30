// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Db;
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

  // map from lowercase table alias to descriptor
  private final Map<String, TupleDescriptor> aliasMap;

  // map from lowercase qualified column name ("alias.col") to descriptor
  private final Map<String, SlotDescriptor> slotRefMap;

  // map from tuple id to list of predicates referencing tuple
  private final Map<TupleId, List<Predicate> > tuplePredicates;

  // map from slot id to list of predicates referencing slot
  private final Map<SlotId, List<Predicate> > slotPredicates;

  // list of all registered conjuncts
  private final List<Predicate> conjuncts;

  public Analyzer(Catalog catalog) {
    this.catalog = catalog;
    this.descTbl = new DescriptorTable();
    this.aliasMap = Maps.newHashMap();
    this.slotRefMap = Maps.newHashMap();
    this.tuplePredicates = Maps.newHashMap();
    this.slotPredicates = Maps.newHashMap();
    this.conjuncts = Lists.newArrayList();
  }

  /**
   * Checks that 'name' references an existing table and that alias
   * isn't already registered. Creates and returns an empty TupleDescriptor
   * and registers it against alias. If alias is empty, register
   * "name.tbl" and "name.db.tbl" as aliases.
   * @param ref
   * @return newly created TupleDescriptor
   * @throws AnalysisException
   */
  public TupleDescriptor registerTableRef(TableRef ref) throws AnalysisException {
    String lookupAlias = ref.getAlias();
    if (aliasMap.containsKey(lookupAlias)) {
      throw new AnalysisException("duplicate table alias: '" + lookupAlias + "'");
    }
    Db db = catalog.getDb(ref.getName().getDb());
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
   * Return descriptor of registered table/alias.
   * @param name
   * @return  null if not registered.
   */
  public TupleDescriptor getDescriptor(TableName name) {
    return aliasMap.get(name.toString().toLowerCase());
  }

  public SlotDescriptor getSlotDescriptor(String colAlias) {
    return slotRefMap.get(colAlias);
  }

  /**
   * Checks that 'col' references an existing column for a registered table alias;
   * if alias is empty, tries to resolve the column name in the context of any of the
   * registered tables. Creates and returns an empty SlotDescriptor if the
   * column hasn't previously been registered, otherwise returns the existing
   * descriptor.
   * @param tblName
   * @param colName
   * @return
   * @throws AnalysisException
   */
  public SlotDescriptor registerColumnRef(TableName tblName, String colName)
      throws AnalysisException {
    String alias;
    if (tblName == null) {
      alias = resolveColumnRef(colName);
      if (alias == null) {
        throw new AnalysisException("couldn't resolve column reference: '" + colName + "'");
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
      throw new AnalysisException("unknown column '" + colName + "' (table alias '" + alias + "')");
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
   * Returns null if not found or multiple bindings exist, otherwise returns
   * the alias.
   * @param colName
   * @return
   * @throws AnalysisException
   */
  private String resolveColumnRef(String colName) throws AnalysisException {
    int numMatches = 0;
    String result = null;
    for (String alias: aliasMap.keySet()) {
      Column col = aliasMap.get(alias).getTable().getColumn(colName);
      if (col != null) {
    	result = alias;
        ++numMatches;
        if (numMatches > 1) {
          throw new AnalysisException(
              "Unqualified column reference '" + colName + "' is ambiguous");
        }
      }
    }
    return result;
  }

  /**
   * Register all conjuncts that make up the predicate.
   * @param p
   */
  public void registerPredicate(Predicate p) {
    List<Predicate> conjuncts = p.getConjuncts();
    for (Predicate conjunct: conjuncts) {
      registerConjunct(conjunct);
    }
  }

  /**
   * Register individual conjunct with all tuple and slot ids it references
   * and with the global conjunct list.
   * @param p
   */
  private void registerConjunct(Predicate p) {
    conjuncts.add(p);

    List<TupleId> tupleIds = Lists.newArrayList();
    List<SlotId> slotIds = Lists.newArrayList();
    p.getIds(tupleIds, slotIds);

    for (TupleId id: tupleIds) {
      if (!tuplePredicates.containsKey(id)) {
        List<Predicate> predList = Lists.newLinkedList();
        predList.add(p);
        tuplePredicates.put(id, predList);
      } else {
        tuplePredicates.get(id).add(p);
      }
    }
    for (SlotId id: slotIds) {
      if (!slotPredicates.containsKey(id)) {
        List<Predicate> predList = Lists.newLinkedList();
        predList.add(p);
        slotPredicates.put(id, predList);
      } else {
        slotPredicates.get(id).add(p);
      }
    }
  }

  /**
   * Return all registered conjuncts that are fully bound by given
   * list of tuple ids.
   * @param tupleIds
   * @return possibly empty list of Predicates
   */
  public List<Predicate> getConjuncts(List<TupleId> tupleIds) {
    List<Predicate> result = Lists.newArrayList();
    for (Predicate pred: conjuncts) {
      if (pred.isBound(tupleIds)) {
        result.add(pred);
      }
    }
    return result;
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
}
