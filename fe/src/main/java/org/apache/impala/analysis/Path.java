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
import java.util.List;

import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.MapType;
import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.Type;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Represents a resolved or unresolved dot-separated path that is rooted at a registered
 * tuple descriptor, catalog table/view, or an existing resolved path.
 *
 * This class implements the resolution logic for mapping an implicit or explicit
 * raw path to the corresponding physical types/positions in the schema tree.
 *
 * Implicit vs. Explicit Paths
 * The item of an array and the key/value of maps are accessed via their implicit field
 * names. However, if the type of an array item or a map value is a struct, then we allow
 * omitting the explicit reference to the struct type in paths for accessing fields
 * within that struct as a shorthand for user convenience. An explicit reference to the
 * struct type is always legal. Paths that explicitly reference such a struct are
 * "physical" because they typically map exactly to the schema representation in the
 * underlying storage format (e.g. Parquet/Avro). Paths that omit the struct reference
 * are called "implicit". During resolution, explicit paths are always preferred over
 * implicit paths for resolving ambiguities.
 *
 * Example
 * create table d.t (
 *   c array<struct<f:int,item:int,pos:int>>
 * );
 *
 * select ... from d.t.c
 *   d.t.c   <-- resolves to type array<struct<f:int,item:int,pos:int>>
 *   c alias <-- type struct<item:struct<f:int,item:int,pos:int>,pos:bigint>>
 *
 * select c.item.f, c.f from d.t.c
 *   c.item.f   <-- explicit path to "f"
 *   c.f        <-- implicit path to "f", skips "item" reference
 *   (same for the unqualified versions item.f and f)
 *
 * select c.item, c.item.item from d.t.c
 *   c.item      <-- explicit path to "item" struct of type struct<f:int,item:string>
 *   c.item.item <-- explicit path to string "item"; there is no logical path to the
 *                   string "item" due to the "item" name conflict
 *   c.pos       <-- explicit path to "pos" of type bigint
 *   c.item.pos  <-- explicit path to "pos" of type int; there is no logical path to the
 *                   int "pos" due to the "pos" name conflict
 *   (same for unqualified versions item, item.item, pos, item.pos)
 *
 * Please refer to TestImplicitAndExplicitPaths() for analogous examples for maps.
 *
 * Illegal Implicit Paths
 * The intention of implicit paths is to allow users to skip a *single* trivial level of
 * indirection in common cases. In particular, it is illegal to implicitly skip multiple
 * levels in a path, illustrated as follows.
 *
 * Example
 * create table d.t (
 *   c array<array<struct<e:int,f:string>>>
 * );
 *
 * select c.f from d.t.c
 * select 1 from d.t.c, c.f
 *   c.f <-- illegal path because it would have to implicitly skip two 'item' fields
 *
 *
 * Uses of Paths and Terminology
 *
 * Uncorrelated References: Star exprs, SlotRefs and TableRefs that are rooted at a
 * catalog Table or a registered TupleDescriptor in the same query block.
 *
 * Relative References: TableRefs that are rooted at a TupleDescriptor.
 *
 * Correlated References: SlotRefs and TableRefs that are rooted at a TupleDescriptor
 * registered in an ancestor query block are called 'correlated'. All correlated
 * references are relative, but not all relative references are correlated.
 *
 * A Path itself is never said to be un/correlated because it is intentionally unaware
 * of the query block that it is used in.
 */
public class Path {
  // Implicit field names of collections.
  public static final String ARRAY_ITEM_FIELD_NAME = "item";
  public static final String ARRAY_POS_FIELD_NAME = "pos";
  public static final String MAP_KEY_FIELD_NAME = "key";
  public static final String MAP_VALUE_FIELD_NAME = "value";

  public static enum PathType {
    SLOT_REF,
    TABLE_REF,
    STAR,
    ANY, // Reference to any field or table in schema.
  }

  // Implicit or explicit raw path to be resolved relative to rootDesc_ or rootTable_.
  // Every raw-path element is mapped to zero, one or two types/positions in resolution.
  private final List<String> rawPath_;

  // Registered table alias that this path is rooted at, if any.
  // Null if the path is rooted at a catalog table/view.
  private final TupleDescriptor rootDesc_;

  // Catalog table that this resolved path is rooted at, if any.
  // Null if the path is rooted at a registered tuple that does not
  // belong to a catalog table/view.
  private final Table rootTable_;

  // Root path that a relative path was created from.
  private final Path rootPath_;

  // List of matched types and field positions set during resolution. The matched
  // types/positions describe the physical path through the schema tree.
  private final List<Type> matchedTypes_ = Lists.newArrayList();
  private final List<Integer> matchedPositions_ = Lists.newArrayList();

  // Remembers the indices into rawPath_ and matchedTypes_ of the first collection
  // matched during resolution.
  private int firstCollectionPathIdx_ = -1;
  private int firstCollectionTypeIdx_ = -1;

  // Indicates whether this path has been resolved. Set in resolve().
  private boolean isResolved_ = false;

  // Caches the result of getAbsolutePath() to avoid re-computing it.
  private List<Integer> absolutePath_ = null;

  /**
   * Constructs a Path rooted at the given rootDesc.
   */
  public Path(TupleDescriptor rootDesc, List<String> rawPath) {
    Preconditions.checkNotNull(rootDesc);
    Preconditions.checkNotNull(rawPath);
    rootTable_ = rootDesc.getTable();
    rootDesc_ = rootDesc;
    rootPath_ = null;
    rawPath_ = rawPath;
  }

  /**
   * Constructs a Path rooted at the given rootTable.
   */
  public Path(Table rootTable, List<String> rawPath) {
    Preconditions.checkNotNull(rootTable);
    Preconditions.checkNotNull(rawPath);
    rootTable_ = rootTable;
    rootDesc_ = null;
    rootPath_ = null;
    rawPath_ = rawPath;
  }

  /**
   * Constructs a new unresolved path relative to an existing resolved path.
   */
  public Path(Path rootPath, List<String> relRawPath) {
    Preconditions.checkNotNull(rootPath);
    Preconditions.checkState(rootPath.isResolved());
    Preconditions.checkNotNull(relRawPath);
    rootTable_ = rootPath.rootTable_;
    rootDesc_ = rootPath.rootDesc_;
    rootPath_ = rootPath;
    rawPath_ = Lists.newArrayListWithCapacity(
        rootPath.getRawPath().size() + relRawPath.size());
    rawPath_.addAll(rootPath.getRawPath());
    rawPath_.addAll(relRawPath);
    matchedTypes_.addAll(rootPath.matchedTypes_);
    matchedPositions_.addAll(rootPath.matchedPositions_);
    firstCollectionPathIdx_ = rootPath.firstCollectionPathIdx_;
    firstCollectionTypeIdx_ = rootPath.firstCollectionTypeIdx_;
  }

  /**
   * Resolves this path in the context of the root tuple descriptor / root table
   * or continues resolving this relative path from an existing root path.
   * Returns true if the path could be fully resolved, false otherwise.
   * A failed resolution leaves this Path in a partially resolved state.
   */
  public boolean resolve() {
    if (isResolved_) return true;
    Preconditions.checkState(rootDesc_ != null || rootTable_ != null);
    Type currentType = null;
    int rawPathIdx = 0;
    if (rootPath_ != null) {
      // Continue resolving this path relative to the rootPath_.
      currentType = rootPath_.destType();
      rawPathIdx = rootPath_.getRawPath().size();
    } else if (rootDesc_ != null) {
      currentType = rootDesc_.getType();
    } else {
      // Directly start from the item type because only implicit paths are allowed.
      currentType = rootTable_.getType().getItemType();
    }

    // Map all remaining raw-path elements to field types and positions.
    while (rawPathIdx < rawPath_.size()) {
      if (!currentType.isComplexType()) return false;
      StructType structType = getTypeAsStruct(currentType);
      // Resolve explicit path.
      StructField field = structType.getField(rawPath_.get(rawPathIdx));
      if (field == null) {
        // Resolve implicit path.
        if (structType instanceof CollectionStructType) {
          field = ((CollectionStructType) structType).getOptionalField();
          // Collections must be matched explicitly.
          if (field.getType().isCollectionType()) return false;
        } else {
          // Failed to resolve implicit or explicit path.
          return false;
        }
        // Update the physical types/positions.
        matchedTypes_.add(field.getType());
        matchedPositions_.add(field.getPosition());
        currentType = field.getType();
        // Do not consume a raw-path element.
        continue;
      }
      matchedTypes_.add(field.getType());
      matchedPositions_.add(field.getPosition());
      if (field.getType().isCollectionType() && firstCollectionPathIdx_ == -1) {
        Preconditions.checkState(firstCollectionTypeIdx_ == -1);
        firstCollectionPathIdx_ = rawPathIdx;
        firstCollectionTypeIdx_ = matchedTypes_.size() - 1;
      }
      currentType = field.getType();
      ++rawPathIdx;
    }
    Preconditions.checkState(matchedTypes_.size() == matchedPositions_.size());
    Preconditions.checkState(matchedTypes_.size() >= rawPath_.size());
    isResolved_ = true;
    return true;
  }

  /**
   * If the given type is a collection, returns a collection struct type representing
   * named fields of its explicit path. Returns the given type itself if it is already
   * a struct. Requires that the given type is a complex type.
   */
  public static StructType getTypeAsStruct(Type t) {
    Preconditions.checkState(t.isComplexType());
    if (t.isStructType()) return (StructType) t;
    if (t.isArrayType()) {
      return CollectionStructType.createArrayStructType((ArrayType) t);
    } else {
      Preconditions.checkState(t.isMapType());
      return CollectionStructType.createMapStructType((MapType) t);
    }
  }

  /**
   * Returns a list of table names that might be referenced by the given path.
   * The path must be non-empty.
   *
   * Examples: path -> result
   * a -> [<sessionDb>.a]
   * a.b -> [<sessionDb>.a, a.b]
   * a.b.c -> [<sessionDb>.a, a.b]
   * a.b.c... -> [<sessionDb>.a, a.b]
   */
  public static List<TableName> getCandidateTables(List<String> path, String sessionDb) {
    Preconditions.checkArgument(path != null && !path.isEmpty());
    List<TableName> result = Lists.newArrayList();
    int end = Math.min(2, path.size());
    for (int tblNameIdx = 0; tblNameIdx < end; ++tblNameIdx) {
      String dbName = (tblNameIdx == 0) ? sessionDb : path.get(0);
      String tblName = path.get(tblNameIdx);
      result.add(new TableName(dbName, tblName));
    }
    return result;
  }

  public Table getRootTable() { return rootTable_; }
  public TupleDescriptor getRootDesc() { return rootDesc_; }
  public boolean isRootedAtTable() { return rootTable_ != null; }
  public boolean isRootedAtTuple() { return rootDesc_ != null; }
  public List<String> getRawPath() { return rawPath_; }
  public boolean isResolved() { return isResolved_; }

  public List<Type> getMatchedTypes() {
    Preconditions.checkState(isResolved_);
    return matchedTypes_;
  }

  public boolean hasNonDestCollection() {
    Preconditions.checkState(isResolved_);
    return firstCollectionPathIdx_ != -1 &&
        firstCollectionPathIdx_ != rawPath_.size() - 1;
  }

  public String getFirstCollectionName() {
    Preconditions.checkState(isResolved_);
    if (firstCollectionPathIdx_ == -1) return null;
    return rawPath_.get(firstCollectionPathIdx_);
  }

  public Type getFirstCollectionType() {
    Preconditions.checkState(isResolved_);
    if (firstCollectionTypeIdx_ == -1) return null;
    return matchedTypes_.get(firstCollectionTypeIdx_);
  }

  public int getFirstCollectionIndex() {
    Preconditions.checkState(isResolved_);
    return firstCollectionTypeIdx_;
  }

  public Type destType() {
    Preconditions.checkState(isResolved_);
    if (!matchedTypes_.isEmpty()) return matchedTypes_.get(matchedTypes_.size() - 1);
    if (rootDesc_ != null) return rootDesc_.getType();
    if (rootTable_ != null) return rootTable_.getType();
    return null;
  }

  public Table destTable() {
    Preconditions.checkState(isResolved_);
    if (rootTable_ != null && rootDesc_ == null && matchedTypes_.isEmpty()) {
      return rootTable_;
    }
    return null;
  }

  /**
   * Returns the destination Column of this path, or null if the destination of this
   * path is not a Column. This path must be rooted at a table or a tuple descriptor
   * corresponding to a table for the destination to be a Column.
   */
  public Column destColumn() {
    Preconditions.checkState(isResolved_);
    if (rootTable_ == null || rawPath_.size() != 1) return null;
    return rootTable_.getColumn(rawPath_.get(rawPath_.size() - 1));
  }

  /**
   * Returns the destination tuple descriptor of this path, or null
   * if the destination of this path is not a registered alias.
   */
  public TupleDescriptor destTupleDesc() {
    Preconditions.checkState(isResolved_);
    if (rootDesc_ != null && matchedTypes_.isEmpty()) return rootDesc_;
    return null;
  }

  public List<String> getFullyQualifiedRawPath() {
    Preconditions.checkState(rootTable_ != null || rootDesc_ != null);
    List<String> result = Lists.newArrayListWithCapacity(rawPath_.size() + 2);
    if (rootDesc_ != null) {
      result.addAll(Lists.newArrayList(rootDesc_.getAlias().split("\\.")));
    } else {
      result.add(rootTable_.getDb().getName());
      result.add(rootTable_.getName());
    }
    result.addAll(rawPath_);
    return result;
  }

  /**
   * Returns the absolute explicit path starting from the fully-qualified table name.
   * The goal is produce a canonical non-ambiguous path that can be used as an
   * identifier for table and slot references.
   *
   * Example:
   * create table mydb.test (a array<struct<f1:int,f2:string>>);
   * use mydb;
   * select f1 from test t, t.a;
   *
   * This function should return the following for the path of the 'f1' SlotRef:
   * mydb.test.a.item.f1
   */
  public List<String> getCanonicalPath() {
    List<String> result = Lists.newArrayList();
    getCanonicalPath(result);
    return result;
  }

  /**
   * Recursive helper for getCanonicalPath().
   */
  private void getCanonicalPath(List<String> result) {
    Type currentType = null;
    if (isRootedAtTuple()) {
      rootDesc_.getPath().getCanonicalPath(result);
      currentType = rootDesc_.getType();
    } else {
      Preconditions.checkNotNull(isRootedAtTable());
      result.add(rootTable_.getTableName().getDb());
      result.add(rootTable_.getTableName().getTbl());
      currentType = rootTable_.getType().getItemType();
    }
    // Compute the explicit path from the matched positions. Note that rawPath_ is
    // not sufficient because it could contain implicit matches.
    for (int i = 0; i < matchedPositions_.size(); ++i) {
      StructType structType = getTypeAsStruct(currentType);
      int matchPos = matchedPositions_.get(i);
      Preconditions.checkState(matchPos < structType.getFields().size());
      StructField match = structType.getFields().get(matchPos);
      result.add(match.getName());
      currentType = match.getType();
    }
  }

  /**
   * Returns the absolute physical path in positions starting from the schema root to the
   * destination of this path.
   */
  public List<Integer> getAbsolutePath() {
    if (absolutePath_ != null) return absolutePath_;
    Preconditions.checkState(isResolved_);
    absolutePath_ = Lists.newArrayList();
    if (rootDesc_ != null) absolutePath_.addAll(rootDesc_.getPath().getAbsolutePath());
    absolutePath_.addAll(matchedPositions_);
    return absolutePath_;
  }

  @Override
  public String toString() {
    Preconditions.checkState(rootTable_ != null || rootDesc_ != null);
    String pathRoot = null;
    if (rootDesc_ != null) {
      pathRoot = rootDesc_.getAlias();
    } else {
      pathRoot = rootTable_.getFullName();
    }
    if (rawPath_.isEmpty()) return pathRoot;
    return pathRoot + "." + Joiner.on(".").join(rawPath_);
  }

  /**
   * Returns a raw path from a known root alias and field name.
   */
  public static ArrayList<String> createRawPath(String rootAlias, String fieldName) {
    ArrayList<String> result = Lists.newArrayList(rootAlias.split("\\."));
    result.add(fieldName);
    return result;
  }

  public static Path createRelPath(Path rootPath, String... fieldNames) {
    Preconditions.checkState(rootPath.isResolved());
    Path result = new Path(rootPath, Lists.newArrayList(fieldNames));
    return result;
  }
}
