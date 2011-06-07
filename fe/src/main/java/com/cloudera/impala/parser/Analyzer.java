// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.parser;

import java.util.HashMap;
import java.util.Set;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Table;

// Repository of analysis state for single select block.
class Analyzer {
  private final DescriptorTable descTbl;
  private final Catalog catalog;

  // map from lowercase table alias to descriptor
  private final HashMap<String, TupleDescriptor> aliasMap;

  // map from lowercase qualified column name ("alias.col") to descriptor
  private final HashMap<String, SlotDescriptor> slotRefMap;

  public Analyzer(Catalog catalog) {
    this.catalog = catalog;
    this.descTbl = new DescriptorTable();
    this.aliasMap = new HashMap<String, TupleDescriptor>();
    this.slotRefMap = new HashMap<String, SlotDescriptor>();
  }

  public static class Exception extends java.lang.Exception {
    public Exception(String msg) {
      super(msg);
    }
  }

  // Checks that 'name' references an existing table and that alias
  // isn't already registered. Creates and returns an empty TupleDescriptor
  // and registers it against alias. If alias is empty, register
  // "name.tbl" and "name.db.tbl" as aliases.
  public TupleDescriptor registerTableRef(TableRef ref) throws Exception {
    String lookupAlias = ref.getAlias();
    if (aliasMap.containsKey(lookupAlias)) {
      throw new Exception("duplicate table alias: " + lookupAlias);
    }
    Db db = catalog.getDb(ref.getName().getDb());
    if (db == null) {
      throw new Exception("unknown db: " + ref.getName().getDb());
    }
    Table tbl = db.getTable(ref.getName().getTbl());
    if (tbl == null) {
      throw new Exception("unknown table: " + ref.getName().getTbl());
    }
    TupleDescriptor result = descTbl.createTupleDescriptor();
    result.setTable(tbl);
    aliasMap.put(lookupAlias, result);
    return result;
  }

  // Return descriptor of registered table/alias. Returns null if not
  // registered.
  public TupleDescriptor getDescriptor(TableName name) {
    return aliasMap.get(name.toString().toLowerCase());
  }

  // Checks that 'col' references an existing column for a registered table alias;
  // if alias is empty, tries to resolve the column name in the context of any of the
  // registered tables. Creates and returns an empty SlotDescriptor if the
  // column hasn't previously been registered, otherwise returns the existing
  // descriptor.
  public SlotDescriptor registerColumnRef(TableName tblName, String colName)
      throws Exception {
    String alias;
    if (tblName == null) {
      alias = resolveColumnRef(colName);
      if (alias == null) {
        throw new Exception("couldn't resolve column reference: " + colName);
      }
    } else {
      alias = tblName.toString().toLowerCase();
    }
    TupleDescriptor d = aliasMap.get(alias);
    if (d == null) {
      throw new Exception("unknown table alias: " + alias);
    }

    Column col = d.getTable().getColumn(colName);
    if (col == null) {
      throw new Exception("unknown column " + colName + " (table alias " + alias);
    }

    SlotDescriptor result = descTbl.addSlotDescriptor(d);
    result.setColumn(col);
    slotRefMap.put(alias + "." + col.getName(), result);
    return result;
  }

  // Resolves column name in context of any of the registered table aliases.
  // Returns null if not found or multiple bindings exist, otherwise returns
  // the alias.
  private String resolveColumnRef(String colName) throws Exception {
    int numMatches = 0;
    String result = null;
    for (String alias: aliasMap.keySet()) {
      Column col = aliasMap.get(alias).getTable().getColumn(colName);
      if (col != null) {
    	result = alias;
        ++numMatches;
        if (numMatches > 1) {
          throw new Exception(
              "Unqualified column reference " + colName + " is ambiguous");
        }
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
