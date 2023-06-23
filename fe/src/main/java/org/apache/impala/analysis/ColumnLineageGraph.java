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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.ImmutableSortedMap;
import org.apache.impala.analysis.ColumnLineageGraph.Vertex.Metadata;
import org.apache.impala.catalog.FeDataSource;
import org.apache.impala.catalog.FeDataSourceTable;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.VirtualTable;
import org.apache.impala.common.Id;
import org.apache.impala.common.IdGenerator;
import org.apache.impala.thrift.TEdgeType;
import org.apache.impala.thrift.TLineageGraph;
import org.apache.impala.thrift.TMultiEdge;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.thrift.TVertex;
import org.apache.impala.thrift.TVertexMetadata;
import org.apache.impala.util.TUniqueIdUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * Represents the column lineage graph of a query. This is a directional graph that is
 * used to track dependencies among the table/column entities that participate in
 * a query. There are two types of dependencies that are represented as edges in the
 * column lineage graph:
 * a) Projection dependency: This is a dependency between a set of source
 * columns (base table columns) and a single target (result expr or table column).
 * This dependency indicates that values of the target depend on the values of the source
 * columns.
 * b) Predicate dependency: This is a dependency between a set of target
 * columns (or exprs) and a set of source columns (base table columns). It indicates that
 * the source columns restrict the values of their targets (e.g. by participating in
 * WHERE clause predicates).
 *
 * The following dependencies are generated for a query:
 * - Exactly one projection dependency for every result expr / target column.
 * - Exactly one predicate dependency that targets all result exprs / target cols and
 *   depends on all columns participating in a conjunct in the query.
 * - Special case of analytic fns: One predicate dependency per result expr / target col
 *   whose value is directly or indirectly affected by an analytic function with a
 *   partition by and/or order by clause.
 */
public class ColumnLineageGraph {
  public static final String ICEBERG = "iceberg";
  public static final String HIVE = "hive";
  public static final String KUDU = "kudu";
  public static final String HBASE = "hbase";
  public static final String VIEW = "view";
  public static final String VIRTUAL = "virtual";
  public static final String EXTERNAL_DATASOURCE = "external-datasource";
  /**
   * Represents a vertex in the column lineage graph. A Vertex may correspond to a base
   * table column, a column in the destination table (for the case of INSERT or CTAS
   * queries) or a result expr (labeled column of a query result set).
   */
  public static final class Vertex implements Comparable<Vertex> {
    public static class Metadata {
      private final String tableName_; // the table name
      private final String tableType_; // type of the table
      private final long tableCreateTime_; // the table/view create time

      public Metadata(String tableName, String tableType, long tableCreateTime) {
        tableName_ = tableName;
        tableType_ = tableType;
        tableCreateTime_ = tableCreateTime;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Metadata metadata = (Metadata) o;
        return tableCreateTime_ == metadata.tableCreateTime_ &&
            Objects.equals(tableName_, metadata.tableName_) &&
            Objects.equals(tableType_, metadata.tableType_);
      }

      @Override
      public int hashCode() {
        return Objects.hash(tableName_, tableType_, tableCreateTime_);
      }

      /**
       * For testing only. We ignore the table create time.
       */
      private boolean equalsForTests(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Metadata metadata = (Metadata) o;
        return Objects.equals(tableName_, metadata.tableName_) &&
            Objects.equals(tableType_, metadata.tableType_);
      }
    }

    // Unique identifier of this vertex.
    private final VertexId id_;

    private final String type_ = "COLUMN";

    // A fully-qualified column name or the label of a result expr
    private final String label_;

    // The table metadata.
    private final Metadata metadata_;

    public Vertex(VertexId id, String label, Metadata metadata) {
      Preconditions.checkNotNull(id);
      Preconditions.checkNotNull(label);
      id_ = id;
      label_ = label;
      metadata_ = metadata;
    }
    public VertexId getVertexId() { return id_; }
    public String getLabel() { return label_; }
    public String getType() { return type_; }
    public Metadata getMetadata() { return metadata_; }

    @Override
    public String toString() { return "(" + id_ + ":" + type_ + ":" + label_ + ")"; }

    /**
     * Encodes this Vertex object into a JSON object represented by a Map.
     */
    public Map<String, Object> toJson() {
      // Use a LinkedHashMap to generate a strict ordering of elements.
      Map<String,Object> obj = new LinkedHashMap<>();
      obj.put("id", id_.asInt());
      obj.put("vertexType", type_);
      obj.put("vertexId", label_);
      if (metadata_ != null) {
        JSONObject jsonMetadata = new JSONObject();
        jsonMetadata.put("tableName", metadata_.tableName_);
        jsonMetadata.put("tableType", metadata_.tableType_);
        jsonMetadata.put("tableCreateTime", metadata_.tableCreateTime_);
        obj.put("metadata", jsonMetadata);
      }
      return obj;
    }

    /**
     * Constructs a Vertex object from a JSON object. The new object is returned.
     */
    public static Vertex fromJsonObj(JSONObject obj) {
      int id = ((Long) obj.get("id")).intValue();
      String label = (String) obj.get("vertexId");
      JSONObject jsonMetadata = (JSONObject) obj.get("metadata");
      if (jsonMetadata == null) {
        return new Vertex(new VertexId(id), label, null);
      }
      String tableName = (String) jsonMetadata.get("tableName");
      String tableType = (String) jsonMetadata.get("tableType");
      long tableCreateTime = (Long) jsonMetadata.get("tableCreateTime");
      return new Vertex(new VertexId(id), label, new Metadata(tableName, tableType,
          tableCreateTime));
    }

    /**
     * Encodes this Vertex object into a thrift object
     */
    public TVertex toThrift() {
      TVertex vertex = new TVertex(id_.asInt(), label_);
      if (metadata_ != null) {
        TVertexMetadata metadata = new TVertexMetadata(metadata_.tableName_,
            metadata_.tableType_, metadata_.tableCreateTime_);
        vertex.setMetadata(metadata);
      }
      return vertex;
    }

    /**
     * Constructs a Vertex object from a thrift object.
     */
    public static Vertex fromThrift(TVertex vertex) {
      int id = ((Long) vertex.id).intValue();
      TVertexMetadata thriftMetadata = vertex.getMetadata();
      Metadata metadata = null;
      if (thriftMetadata != null) {
        metadata = new Metadata(thriftMetadata.getTable_name(),
            thriftMetadata.getTable_type(),
            thriftMetadata.getTable_create_time());
      }
      return new Vertex(new VertexId(id), vertex.label, metadata);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Vertex vertex = (Vertex) o;
      return Objects.equals(id_, vertex.id_) &&
          Objects.equals(type_, vertex.type_) &&
          Objects.equals(label_, vertex.label_) &&
          Objects.equals(metadata_, vertex.metadata_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id_, type_, label_, metadata_);
    }

    /**
     * For testing only.
     */
    private boolean equalsForTests(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Vertex vertex = (Vertex) o;
      return Objects.equals(id_, vertex.id_) &&
          ((metadata_ == vertex.metadata_) ||
              metadata_ != null && metadata_.equalsForTests(vertex.metadata_));
    }

    @Override
    public int compareTo(Vertex cmp) { return this.id_.compareTo(cmp.id_); }
  }

  /**
   * Represents the unique identifier of a Vertex.
   */
  private static final class VertexId extends Id<VertexId> {
    protected VertexId(int id) {
      super(id);
    }
    public static IdGenerator<VertexId> createGenerator() {
      return new IdGenerator<VertexId>() {
        @Override
        public VertexId getNextId() { return new VertexId(nextId_++); }
        @Override
        public VertexId getMaxId() { return new VertexId(nextId_ - 1); }
      };
    }
  }

  /**
   * Represents a set of uni-directional edges in the column lineage graph, one edge from
   * every source Vertex in 'sources_' to every target Vertex in 'targets_'. An edge
   * indicates a dependency between a source and a target Vertex. There are two types of
   * edges, PROJECTION and PREDICATE, that are described in the ColumnLineageGraph class.
   */
  private static final class MultiEdge {
    public static enum EdgeType {
      PROJECTION, PREDICATE
    }
    private final Set<Vertex> sources_;
    private final Set<Vertex> targets_;
    private final EdgeType edgeType_;

    public MultiEdge(Set<Vertex> sources, Set<Vertex> targets, EdgeType type) {
      sources_ = sources;
      targets_ = targets;
      edgeType_ = type;
    }

    /**
     * Return an ordered set of source vertices.
     */
    private ImmutableSortedSet<Vertex> getOrderedSources() {
      return ImmutableSortedSet.copyOf(sources_);
    }

    /**
     * Return an ordered set of target vertices.
     */
    private ImmutableSortedSet<Vertex> getOrderedTargets() {
      return ImmutableSortedSet.copyOf(targets_);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      Joiner joiner = Joiner.on(",");
      builder.append("Sources: [");
      builder.append(joiner.join(getOrderedSources()) + "]\n");
      builder.append("Targets: [");
      builder.append(joiner.join(getOrderedTargets()) + "]\n");
      builder.append("Type: " + edgeType_);
      return builder.toString();
    }

    /**
     * Encodes this MultiEdge object to a JSON object represented by a Map.
     * Returns a LinkedHashMap to guarantee a consistent ordering of elements.
     */
    public Map<String,Object> toJson() {
      Map<String,Object> obj = new LinkedHashMap<>();
      // Add sources
      JSONArray sourceIds = new JSONArray();
      for (Vertex vertex: getOrderedSources()) {
        sourceIds.add(vertex.getVertexId());
      }
      obj.put("sources", sourceIds);
      // Add targets
      JSONArray targetIds = new JSONArray();
      for (Vertex vertex: getOrderedTargets()) {
        targetIds.add(vertex.getVertexId());
      }
      obj.put("targets", targetIds);
      obj.put("edgeType", edgeType_.toString());
      return obj;
    }

    /**
     * Encodes this MultiEdge object to a thrift object
     */
    public TMultiEdge toThrift() {
      List<TVertex> sources = new ArrayList<>();
      for (Vertex vertex: getOrderedSources()) {
        sources.add(vertex.toThrift());
      }
      List<TVertex> targets = new ArrayList<>();
      for (Vertex vertex: getOrderedTargets()) {
        targets.add(vertex.toThrift());
      }
      if (edgeType_ == EdgeType.PROJECTION) {
        return new TMultiEdge(sources, targets, TEdgeType.PROJECTION);
      }
      return new TMultiEdge(sources, targets, TEdgeType.PREDICATE);
    }

    /**
     * Constructs a MultiEdge object from a thrift object.
     */
    public static MultiEdge fromThrift(TMultiEdge obj) {
      Set<Vertex> sources = new HashSet<>();
      for (TVertex vertex: obj.sources) {
        sources.add(Vertex.fromThrift(vertex));
      }
      Set<Vertex> targets = new HashSet<>();
      for (TVertex vertex: obj.targets) {
        targets.add(Vertex.fromThrift(vertex));
      }
      if (obj.edgetype == TEdgeType.PROJECTION) {
        return new MultiEdge(sources, targets, EdgeType.PROJECTION);
      }
      return new MultiEdge(sources, targets, EdgeType.PREDICATE);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MultiEdge multiEdge = (MultiEdge) o;
      return Objects.equals(sources_, multiEdge.sources_) &&
          Objects.equals(targets_, multiEdge.targets_) &&
          edgeType_ == multiEdge.edgeType_;
    }

    @Override
    public int hashCode() {
      return Objects.hash(sources_, targets_, edgeType_);
    }

    /**
     * For testing only.
     */
    private boolean equalsForTests(Object obj) {
      if (obj == null) return false;
      if (obj.getClass() != this.getClass()) return false;
      MultiEdge edge = (MultiEdge) obj;
      return setEqualsForTests(edge.sources_, this.sources_) &&
          setEqualsForTests(edge.targets_, this.targets_) &&
          edge.edgeType_ == this.edgeType_;
    }
  }

  public static class ColumnLabel implements Comparable<ColumnLabel> {
    private final String columnLabel_;
    private final TableName tableName_;
    private final String tableType_;

    public ColumnLabel(String columnName, TableName tableName, String tableType) {
      columnLabel_ = columnName;
      tableName_ = tableName;
      tableType_ = tableType;
    }

    public ColumnLabel(String columnName) {
      this(columnName, null, null);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ColumnLabel that = (ColumnLabel) o;
      return Objects.equals(columnLabel_, that.columnLabel_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(columnLabel_);
    }

    @Override
    public int compareTo(ColumnLabel o) {
      return columnLabel_.compareTo(o.columnLabel_);
    }
  }

  private final static Logger LOG = LoggerFactory.getLogger(ColumnLineageGraph.class);
  // Query statement
  private String queryStr_;

  private TUniqueId queryId_;

  // Name of the user that issued this query
  private String user_;

  private final List<Expr> resultDependencyPredicates_ = new ArrayList<>();

  private final List<MultiEdge> edges_ = new ArrayList<>();

  // Timestamp in seconds since epoch (GMT) this query was submitted for execution.
  private long timestamp_;

  // Map of Vertex labels to Vertex objects.
  private final Map<String, Vertex> vertices_ = new HashMap<>();

  // Map of Vertex ids to Vertex objects. Used primarily during the construction of the
  // ColumnLineageGraph from a serialized JSON object.
  private final Map<VertexId, Vertex> idToVertexMap_ = new HashMap<>();

  // For an INSERT or a CTAS, these are the columns of the
  // destination table plus any partitioning columns (when dynamic partitioning is used).
  // For a SELECT stmt, they are the labels of the result exprs.
  private final List<ColumnLabel> targetColumnLabels_ = new ArrayList<>();

  // Repository for tuple and slot descriptors for this query. Use it to construct the
  // column lineage graph.
  private DescriptorTable descTbl_;

  private final IdGenerator<VertexId> vertexIdGenerator = VertexId.createGenerator();

  public ColumnLineageGraph() { }

  /**
   * Private c'tor, used only for testing.
   */
  private ColumnLineageGraph(String stmt, TUniqueId queryId, String user, long timestamp)
  {
    queryStr_ = stmt;
    queryId_ = queryId;
    user_ = user;
    timestamp_ = timestamp;
  }

  private void setVertices(Set<Vertex> vertices) {
    for (Vertex vertex: vertices) {
      vertices_.put(vertex.getLabel(), vertex);
      idToVertexMap_.put(vertex.getVertexId(), vertex);
    }
  }

  /**
   * Creates a new MultiEdge in the column lineage graph from the sets of 'sources' and
   * 'targets' labels (representing column names or result expr labels). The new
   * MultiEdge object is returned.
   */
  private MultiEdge createMultiEdge(Set<ColumnLabel> targets,
      Map<String, SlotDescriptor> sources, MultiEdge.EdgeType type, Analyzer analyzer) {
    // createVertex() generates new IDs; we sort the input sets to make the output
    // deterministic and independent of the ordering of the input sets.
    Set<Vertex> targetVertices = new HashSet<>();
    for (ColumnLabel target: ImmutableSortedSet.copyOf(targets)) {
      Metadata metadata = null;
      if (target.tableName_ != null) {
        FeTable feTable = analyzer.getStmtTableCache().tables.get(target.tableName_);
        if (feTable != null && feTable.getMetaStoreTable() != null) {
          metadata = new Metadata(target.tableName_.toString(), getTableType(feTable),
              feTable.getMetaStoreTable().getCreateTime());
        } else {
          // -1 is just a placeholder that will be updated after the table/view has been
          // created. See client-request-state.cc (LogLineageRecord) for more information.
          metadata = new Metadata(target.tableName_.toString(), target.tableType_, -1);
        }
      }
      targetVertices.add(createVertex(target.columnLabel_, metadata));
    }
    Set<Vertex> sourceVertices = new HashSet<>();
    for (Map.Entry<String, SlotDescriptor> source:
        ImmutableSortedMap.copyOf(sources).entrySet()) {
      FeTable feTable = source.getValue().getParent().getTable();
      Preconditions.checkState(feTable != null);
      Metadata metadata = feTable != null && feTable.getMetaStoreTable() != null ?
          new Metadata(feTable.getTableName().toString(),
              getTableType(feTable),
              feTable.getMetaStoreTable().getCreateTime()) : null;
      sourceVertices.add(createVertex(source.getKey(), metadata));
    }
    MultiEdge edge = new MultiEdge(sourceVertices, targetVertices, type);
    edges_.add(edge);
    return edge;
  }

  public static String getTableType(FeTable tbl) {
    if (tbl instanceof FeIcebergTable) return ICEBERG;
    if (tbl instanceof FeFsTable) return HIVE;
    if (tbl instanceof FeKuduTable) return KUDU;
    if (tbl instanceof FeHBaseTable) return HBASE;
    if (tbl instanceof FeView) return VIEW;
    if (tbl instanceof VirtualTable) return VIRTUAL;
    if (tbl instanceof FeDataSourceTable) return EXTERNAL_DATASOURCE;
    LOG.error(String.format("Table '%s' has unknown type: '%s'", tbl.getFullName(),
        tbl.getClass().getName()));
    return "unknown";
  }

  /**
   * Creates a new vertex in the column lineage graph. The new Vertex object is
   * returned. If a Vertex with the same label already exists, reuse it.
   */
  private Vertex createVertex(String label, Metadata metadata) {
    Vertex newVertex = vertices_.get(label);
    if (newVertex != null) return newVertex;
    newVertex = new Vertex(vertexIdGenerator.getNextId(), label, metadata);
    vertices_.put(newVertex.getLabel(), newVertex);
    idToVertexMap_.put(newVertex.getVertexId(), newVertex);
    return newVertex;
  }

  /**
   * Computes the column lineage graph of a query from the list of query result exprs.
   * 'rootAnalyzer' is the Analyzer that was used for the analysis of the query.
   */
  public void computeLineageGraph(List<Expr> resultExprs, Analyzer rootAnalyzer) {
    init(rootAnalyzer);
    // Compute the dependencies only if result expressions are available.
    if (resultExprs != null && !resultExprs.isEmpty()) {
      computeProjectionDependencies(resultExprs, rootAnalyzer);
      computeResultPredicateDependencies(rootAnalyzer);
    }
  }

  /**
   * Initialize the ColumnLineageGraph from the root analyzer of a query.
   */
  private void init(Analyzer analyzer) {
    Preconditions.checkNotNull(analyzer);
    Preconditions.checkState(analyzer.isRootAnalyzer());
    TQueryCtx queryCtx = analyzer.getQueryCtx();
    if (queryCtx.client_request.isSetRedacted_stmt()) {
      queryStr_ = queryCtx.client_request.redacted_stmt;
    } else {
      queryStr_ = queryCtx.client_request.stmt;
    }
    Preconditions.checkNotNull(queryStr_);
    timestamp_ = queryCtx.start_unix_millis / 1000;
    descTbl_ = analyzer.getDescTbl();
    user_ = analyzer.getUser().getName();
    queryId_ = queryCtx.query_id;
  }

  private void computeProjectionDependencies(List<Expr> resultExprs, Analyzer analyzer) {
    Preconditions.checkNotNull(resultExprs);
    Preconditions.checkState(!resultExprs.isEmpty());
    Preconditions.checkState(resultExprs.size() == targetColumnLabels_.size());
    for (int i = 0; i < resultExprs.size(); ++i) {
      Expr expr = resultExprs.get(i);
      Map<String, SlotDescriptor> sourceBaseCols = new HashMap<>();
      List<Expr> dependentExprs = new ArrayList<>();
      getSourceBaseCols(expr, sourceBaseCols, dependentExprs, false);
      Set<ColumnLabel> targets = Sets.newHashSet(targetColumnLabels_.get(i));
      createMultiEdge(targets, sourceBaseCols, MultiEdge.EdgeType.PROJECTION, analyzer);
      if (!dependentExprs.isEmpty()) {
        // We have additional exprs that 'expr' has a predicate dependency on.
        // Gather the transitive predicate dependencies of 'expr' based on its direct
        // predicate dependencies. For each direct predicate dependency p, 'expr' is
        // transitively predicate dependent on all exprs that p is projection and
        // predicate dependent on.
        Map<String, SlotDescriptor> predicateBaseCols = new HashMap<>();
        for (Expr dependentExpr: dependentExprs) {
          getSourceBaseCols(dependentExpr, predicateBaseCols, null, true);
        }
        createMultiEdge(targets, predicateBaseCols, MultiEdge.EdgeType.PREDICATE,
            analyzer);
      }
    }
  }

  /**
   * Compute predicate dependencies for the query result, i.e. exprs that affect the
   * possible values of the result exprs / target columns, such as predicates in a WHERE
   * clause.
   */
  private void computeResultPredicateDependencies(Analyzer analyzer) {
    List<Expr> conjuncts = analyzer.getConjuncts();
    for (Expr expr: conjuncts) {
      if (expr.isAuxExpr()) continue;
      resultDependencyPredicates_.add(expr);
    }
    Map<String, SlotDescriptor> predicateBaseCols = new HashMap<>();
    for (Expr expr: resultDependencyPredicates_) {
      getSourceBaseCols(expr, predicateBaseCols, null, true);
    }
    if (predicateBaseCols.isEmpty()) return;
    Set<ColumnLabel> targets = Sets.newHashSet(targetColumnLabels_);
    createMultiEdge(targets, predicateBaseCols, MultiEdge.EdgeType.PREDICATE, analyzer);
  }

  /**
   * Identify the base table columns that 'expr' is connected to by recursively resolving
   * all associated slots through inline views and materialization points to base-table
   * slots. If 'directPredDeps' is not null, it is populated with the exprs that
   * have a predicate dependency with 'expr' (e.g. partitioning and order by exprs for
   * the case of an analytic function). If 'traversePredDeps' is false, not all the
   * children exprs of 'expr' are used to identify the base columns that 'expr' is
   * connected to. Which children are filtered depends on the type of 'expr' (e.g. for
   * AnalyticFunctionExpr, grouping and sorting exprs are filtered out).
   */
  private void getSourceBaseCols(Expr expr, Map<String, SlotDescriptor> sourceBaseCols,
      List<Expr> directPredDeps, boolean traversePredDeps) {
    List<Expr> exprsToTraverse = getProjectionDeps(expr);
    List<Expr> predicateDepExprs = getPredicateDeps(expr);
    if (directPredDeps != null) directPredDeps.addAll(predicateDepExprs);
    if (traversePredDeps) exprsToTraverse.addAll(predicateDepExprs);
    List<SlotId> slotIds = new ArrayList<>();
    for (Expr e: exprsToTraverse) {
      e.getIds(null, slotIds);
    }
    for (SlotId slotId: slotIds) {
      SlotDescriptor slotDesc = descTbl_.getSlotDesc(slotId);
      List<Expr> sourceExprs = slotDesc.getSourceExprs();
      if (sourceExprs.isEmpty() && slotDesc.isScanSlot() &&
          slotDesc.getPath().isRootedAtTuple()) {
        // slot should correspond to a materialized tuple of a table
        Preconditions.checkState(slotDesc.getParent().isMaterialized());
        List<String> path = slotDesc.getPath().getCanonicalPath();
        sourceBaseCols.put(Joiner.on(".").join(path), slotDesc);
      } else {
        for (Expr sourceExpr: sourceExprs) {
          getSourceBaseCols(sourceExpr, sourceBaseCols, directPredDeps,
              traversePredDeps);
        }
      }
    }
  }

  /**
   * Retrieve the exprs that 'e' is directly projection dependent on.
   * TODO Handle conditional exprs (e.g. CASE, IF).
   */
  private List<Expr> getProjectionDeps(Expr e) {
    Preconditions.checkNotNull(e);
    List<Expr> outputExprs = new ArrayList<>();
    if (e instanceof AnalyticExpr) {
      AnalyticExpr analytic = (AnalyticExpr) e;
      outputExprs.addAll(analytic.getChildren().subList(0,
          analytic.getFnCall().getParams().size()));
    } else {
      outputExprs.add(e);
    }
    return outputExprs;
  }

  /**
   * Retrieve the exprs that 'e' is directly predicate dependent on.
   * TODO Handle conditional exprs (e.g. CASE, IF).
   */
  private List<Expr> getPredicateDeps(Expr e) {
    Preconditions.checkNotNull(e);
    List<Expr> outputExprs = new ArrayList<>();
    if (e instanceof AnalyticExpr) {
      AnalyticExpr analyticExpr = (AnalyticExpr) e;
      outputExprs.addAll(analyticExpr.getPartitionExprs());
      for (OrderByElement orderByElem: analyticExpr.getOrderByElements()) {
        outputExprs.add(orderByElem.getExpr());
      }
    }
    return outputExprs;
  }

  public void addDependencyPredicates(Collection<Expr> exprs) {
    resultDependencyPredicates_.addAll(exprs);
  }

  /**
   * Encodes the ColumnLineageGraph object to JSON.
   */
  public String toJson() {
    if (Strings.isNullOrEmpty(queryStr_)) return "";
    Map<String, Object> obj = new LinkedHashMap<>();
    obj.put("queryText", queryStr_);
    obj.put("queryId", TUniqueIdUtil.PrintId(queryId_));
    obj.put("hash", getQueryHash(queryStr_));
    obj.put("user", user_);
    obj.put("timestamp", timestamp_);
    // Add edges
    JSONArray edges = new JSONArray();
    for (MultiEdge edge: edges_) {
      edges.add(edge.toJson());
    }
    obj.put("edges", edges);
    // Add vertices
    TreeSet<Vertex> sortedVertices = Sets.newTreeSet(vertices_.values());
    JSONArray vertices = new JSONArray();
    for (Vertex vertex: sortedVertices) {
      vertices.add(vertex.toJson());
    }
    obj.put("vertices", vertices);
    return JSONValue.toJSONString(obj);
  }

  /**
   * Serializes the ColumnLineageGraph to a thrift object
   */
  public TLineageGraph toThrift() {
    TLineageGraph graph = new TLineageGraph();
    if (Strings.isNullOrEmpty(queryStr_)) return graph;
    graph.setQuery_text(queryStr_);
    graph.setQuery_id(queryId_);
    graph.setHash(getQueryHash(queryStr_));
    graph.setUser(user_);
    graph.setStarted(timestamp_);
    // Add edges
    List<TMultiEdge> edges = new ArrayList<>();
    for (MultiEdge edge: edges_) {
      edges.add(edge.toThrift());
    }
    graph.setEdges(edges);
    // Add vertices
    TreeSet<Vertex> sortedVertices = Sets.newTreeSet(vertices_.values());
    List<TVertex> vertices = new ArrayList<>();
    for (Vertex vertex: sortedVertices) {
      vertices.add(vertex.toThrift());
    }
    graph.setVertices(vertices);
    return graph;
  }

  /**
   * Creates a LineageGraph object from a thrift object
   */
  public static ColumnLineageGraph fromThrift(TLineageGraph obj) {
    ColumnLineageGraph lineage =
        new ColumnLineageGraph(obj.query_text, obj.query_id, obj.user, obj.started);
    Map<TVertex, Vertex> vertexMap = new HashMap<>();
    TreeSet<Vertex> vertices = Sets.newTreeSet();
    for (TVertex vertex: obj.vertices) {
      Vertex v = Vertex.fromThrift(vertex);
      vertices.add(v);
    }
    lineage.setVertices(vertices);
    for (TMultiEdge edge: obj.edges) {
      MultiEdge e = MultiEdge.fromThrift(edge);
      lineage.edges_.add(e);
    }
    return lineage;
  }

  private String getQueryHash(String queryStr) {
    Hasher hasher = Hashing.murmur3_128().newHasher();
    hasher.putUnencodedChars(queryStr);
    return hasher.hash().toString();
  }

  /**
   * Creates a ColumnLineageGraph object from a serialized JSON record. The new
   * ColumnLineageGraph object is returned. Used only during testing.
   */
  public static ColumnLineageGraph createFromJSON(String json) {
    if (json == null || json.isEmpty()) return null;
    JSONParser parser = new JSONParser();
    Object obj = null;
    try {
      obj = parser.parse(json);
    } catch (ParseException e) {
      LOG.error("Error parsing serialized column lineage graph: " + e.getMessage());
      return null;
    }
    if (!(obj instanceof JSONObject)) return null;
    JSONObject jsonObj = (JSONObject) obj;
    String stmt = (String) jsonObj.get("queryText");
    TUniqueId queryId = TUniqueIdUtil.ParseId((String) jsonObj.get("queryId"));
    String user = (String) jsonObj.get("user");
    long timestamp = (Long) jsonObj.get("timestamp");
    ColumnLineageGraph graph = new ColumnLineageGraph(stmt, queryId, user, timestamp);
    JSONArray serializedVertices = (JSONArray) jsonObj.get("vertices");
    Set<Vertex> vertices = new HashSet<>();
    for (int i = 0; i < serializedVertices.size(); ++i) {
      Vertex v = Vertex.fromJsonObj((JSONObject) serializedVertices.get(i));
      vertices.add(v);
    }
    graph.setVertices(vertices);
    JSONArray serializedEdges = (JSONArray) jsonObj.get("edges");
    for (int i = 0; i < serializedEdges.size(); ++i) {
      MultiEdge e =
          graph.createMultiEdgeFromJSONObj((JSONObject) serializedEdges.get(i));
      graph.edges_.add(e);
    }
    return graph;
  }

  private MultiEdge createMultiEdgeFromJSONObj(JSONObject jsonEdge) {
    Preconditions.checkNotNull(jsonEdge);
    JSONArray sources = (JSONArray) jsonEdge.get("sources");
    Set<Vertex> sourceVertices = getVerticesFromJSONArray(sources);
    JSONArray targets = (JSONArray) jsonEdge.get("targets");
    Set<Vertex> targetVertices = getVerticesFromJSONArray(targets);
    MultiEdge.EdgeType type =
        MultiEdge.EdgeType.valueOf((String) jsonEdge.get("edgeType"));
    return new MultiEdge(sourceVertices, targetVertices, type);
  }

  private Set<Vertex> getVerticesFromJSONArray(JSONArray vertexIdArray) {
    Set<Vertex> vertices = new HashSet<>();
    for (int i = 0; i < vertexIdArray.size(); ++i) {
      int sourceId = ((Long) vertexIdArray.get(i)).intValue();
      Vertex sourceVertex = idToVertexMap_.get(new VertexId(sourceId));
      Preconditions.checkNotNull(sourceVertex);
      vertices.add(sourceVertex);
    }
    return vertices;
  }

  /**
   * This is only for testing. It does not check for user and timestamp fields.
   */
  public boolean equalsForTests(Object obj) {
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    ColumnLineageGraph g = (ColumnLineageGraph) obj;
    if (!mapEqualsForTests(this.vertices_, g.vertices_) ||
        !listEqualsForTests(this.edges_, g.edges_)) {
      return false;
    }
    return true;
  }

  private static boolean mapEqualsForTests(Map<String, Vertex> map1,
      Map<String, Vertex> map2) {
    if (map1.size() != map2.size()) return false;
    Iterator<Entry<String, Vertex>> i = map1.entrySet().iterator();
    while (i.hasNext()) {
      Entry<String, Vertex> e = i.next();
      String key = e.getKey();
      Vertex value = e.getValue();
      if (value == null) {
        if (!(map2.get(key) == null && map2.containsKey(key))) return false;
      } else {
        if (!value.equalsForTests(map2.get(key))) return false;
      }
    }
    return true;
  }

  private static boolean setEqualsForTests(Set<Vertex> set1, Set<Vertex> set2) {
    if (set1.size() != set2.size()) return false;
    for (Vertex v1 : set1) {
      boolean found = false;
      Iterator<Vertex> i = set2.iterator();
      while (i.hasNext()) {
        Vertex v2 = i.next();
        if (v1.equalsForTests(v2)) {
          i.remove();
          found = true;
        }
      }
      if (!found) return false;
    }
    return set2.isEmpty();
  }

  private static boolean listEqualsForTests(List<MultiEdge> list1,
      List<MultiEdge> list2) {
    ListIterator<MultiEdge> i1 = list1.listIterator();
    ListIterator<MultiEdge> i2 = list2.listIterator();
    while (i1.hasNext() && i2.hasNext()) {
      MultiEdge e1 = i1.next();
      MultiEdge e2 = i2.next();
      if (!(e1 == null ? e2 == null : e1.equalsForTests(e2))) {
        return false;
      }
    }
    return !(i1.hasNext() || i2.hasNext());
  }

  public String debugString() {
    StringBuilder builder = new StringBuilder();
    for (MultiEdge edge: edges_) {
      builder.append(edge.toString() + "\n");
    }
    builder.append(toJson());
    return builder.toString();
  }

  public void addTargetColumnLabels(Collection<ColumnLabel> columnLabels) {
    Preconditions.checkNotNull(columnLabels);
    targetColumnLabels_.addAll(columnLabels);
  }

  public void addTargetColumnLabels(FeTable dstTable) {
    Preconditions.checkNotNull(dstTable);
    for (String columnName: dstTable.getColumnNames()) {
      targetColumnLabels_.add(new ColumnLabel(columnName, dstTable.getTableName(),
          getTableType(dstTable)));
    }
  }
}
