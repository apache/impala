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
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.Id;
import com.cloudera.impala.common.IdGenerator;
import com.cloudera.impala.thrift.TEdgeType;
import com.cloudera.impala.thrift.TQueryCtx;
import com.cloudera.impala.thrift.TLineageGraph;
import com.cloudera.impala.thrift.TMultiEdge;
import com.cloudera.impala.thrift.TVertex;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * Represents a vertex in the column lineage graph. A Vertex may correspond to a base
 * table column, a column in the destination table (for the case of INSERT or CTAS
 * queries) or a result expr (labeled column of a query result set).
 */
final class Vertex implements Comparable<Vertex> {
  // Unique identifier of this vertex.
  private final VertexId id_;

  private final String type_ = "COLUMN";

  // A fully-qualified column name or the label of a result expr
  private final String label_;

  public Vertex(VertexId id, String label) {
    Preconditions.checkNotNull(id);
    Preconditions.checkNotNull(label);
    id_ = id;
    label_ = label;
  }
  public VertexId getVertexId() { return id_; }
  public String getLabel() { return label_; }
  public String getType() { return type_; }

  @Override
  public String toString() { return "(" + id_ + ":" + type_ + ":" + label_ + ")"; }

  /**
   * Encodes this Vertex object into a JSON object represented by a Map.
   */
  public Map toJson() {
    // Use a LinkedHashMap to generate a strict ordering of elements.
    Map obj = new LinkedHashMap();
    obj.put("id", id_.asInt());
    obj.put("vertexType", type_);
    obj.put("vertexId", label_);
    return obj;
  }

  /**
   * Constructs a Vertex object from a JSON object. The new object is returned.
   */
  public static Vertex fromJsonObj(JSONObject obj) {
    int id = ((Long) obj.get("id")).intValue();
    String label = (String) obj.get("vertexId");
    return new Vertex(new VertexId(id), label);
  }

  /**
   * Encodes this Vertex object into a thrift object
   */
  public TVertex toThrift() {
    return new TVertex(id_.asInt(), label_);
  }

  /**
   * Constructs a Vertex object from a thrift object.
   */
  public static Vertex fromThrift(TVertex vertex) {
    int id = ((Long) vertex.id).intValue();
    return new Vertex(new VertexId(id), vertex.label);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    Vertex vertex = (Vertex) obj;
    return this.id_.equals(vertex.id_) &&
        this.label_.equals(vertex.label_);
  }

  public int compareTo(Vertex cmp) { return this.id_.compareTo(cmp.id_); }

  @Override
  public int hashCode() { return id_.hashCode(); }
}

/**
 * Represents the unique identifier of a Vertex.
 */
class VertexId extends Id<VertexId> {
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
final class MultiEdge {
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

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    Joiner joiner = Joiner.on(",");
    builder.append("Sources: [");
    builder.append(joiner.join(sources_) + "]\n");
    builder.append("Targets: [");
    builder.append(joiner.join(targets_) + "]\n");
    builder.append("Type: " + edgeType_);
    return builder.toString();
  }

  /**
   * Encodes this MultiEdge object to a JSON object represented by a Map.
   */
  public Map toJson() {
    Map obj = new LinkedHashMap();
    // Add sources
    JSONArray sourceIds = new JSONArray();
    for (Vertex vertex: sources_) {
      sourceIds.add(vertex.getVertexId());
    }
    obj.put("sources", sourceIds);
    // Add targets
    JSONArray targetIds = new JSONArray();
    for (Vertex vertex: targets_) {
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
    List<TVertex> sources = Lists.newArrayList();
    for (Vertex vertex: sources_) {
      sources.add(vertex.toThrift());
    }
    List<TVertex> targets = Lists.newArrayList();
    for (Vertex vertex: targets_) {
      targets.add(vertex.toThrift());
    }
    if (edgeType_ == EdgeType.PROJECTION) {
      return new TMultiEdge(sources, targets, TEdgeType.PROJECTION);
    }
    return new TMultiEdge(sources, targets, TEdgeType.PREDICATE);
  }

  /**
   * Constructs a MultiEdge object from a thrift object
   */
  public static MultiEdge fromThrift(TMultiEdge obj){
    Set<Vertex> sources = Sets.newHashSet();
    for (TVertex vertex: obj.sources) {
      sources.add(Vertex.fromThrift(vertex));
    }
    Set<Vertex> targets = Sets.newHashSet();
    for (TVertex vertex: obj.targets) {
      targets.add(Vertex.fromThrift(vertex));
    }
    if (obj.edgetype == TEdgeType.PROJECTION) {
      return new MultiEdge(sources, targets, EdgeType.PROJECTION);
    }
    return new MultiEdge(sources, targets, EdgeType.PREDICATE);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    MultiEdge edge = (MultiEdge) obj;
    return edge.sources_.equals(this.sources_) &&
        edge.targets_.equals(this.targets_) &&
        edge.edgeType_ == this.edgeType_;
  }
}

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
  private final static Logger LOG = LoggerFactory.getLogger(ColumnLineageGraph.class);
  // Query statement
  private String queryStr_;

  // Name of the user that issued this query
  private String user_;

  private final List<Expr> resultDependencyPredicates_ = Lists.newArrayList();

  private final List<MultiEdge> edges_ = Lists.newArrayList();

  // Timestamp in seconds since epoch (GMT) this query was submitted for execution.
  private long timestamp_;

  // Map of Vertex labels to Vertex objects.
  private final Map<String, Vertex> vertices_ = Maps.newHashMap();

  // Map of Vertex ids to Vertex objects. Used primarily during the construction of the
  // ColumnLineageGraph from a serialized JSON object.
  private final Map<VertexId, Vertex> idToVertexMap_ = Maps.newHashMap();

  // For an INSERT or a CTAS, these are the columns of the
  // destination table plus any partitioning columns (when dynamic partitioning is used).
  // For a SELECT stmt, they are the labels of the result exprs.
  private final List<String> targetColumnLabels_ = Lists.newArrayList();

  // Repository for tuple and slot descriptors for this query. Use it to construct the
  // column lineage graph.
  private DescriptorTable descTbl_;

  private final IdGenerator<VertexId> vertexIdGenerator = VertexId.createGenerator();

  public ColumnLineageGraph() { }

  /**
   * Private c'tor, used only for testing.
   */
  private ColumnLineageGraph(String stmt, String user, long timestamp) {
    queryStr_ = stmt;
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
  private MultiEdge createMultiEdge(Set<String> targets, Set<String> sources,
      MultiEdge.EdgeType type) {
    Set<Vertex> targetVertices = Sets.newHashSet();
    for (String target: targets) {
      targetVertices.add(createVertex(target));
    }
    Set<Vertex> sourceVertices = Sets.newHashSet();
    for (String source: sources) {
      sourceVertices.add(createVertex(source));
    }
    MultiEdge edge = new MultiEdge(sourceVertices, targetVertices, type);
    edges_.add(edge);
    return edge;
  }

  /**
   * Creates a new vertex in the column lineage graph. The new Vertex object is
   * returned. If a Vertex with the same label already exists, reuse it.
   */
  private Vertex createVertex(String label) {
    Vertex newVertex = vertices_.get(label);
    if (newVertex != null) return newVertex;
    newVertex = new Vertex(vertexIdGenerator.getNextId(), label);
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
    computeProjectionDependencies(resultExprs);
    computeResultPredicateDependencies(rootAnalyzer);
  }

  /**
   * Initialize the ColumnLineageGraph from the root analyzer of a query.
   */
  private void init(Analyzer analyzer) {
    Preconditions.checkNotNull(analyzer);
    Preconditions.checkState(analyzer.isRootAnalyzer());
    TQueryCtx queryCtx = analyzer.getQueryCtx();
    if (queryCtx.request.isSetRedacted_stmt()) {
      queryStr_ = queryCtx.request.redacted_stmt;
    } else {
      queryStr_ = queryCtx.request.stmt;
    }
    Preconditions.checkNotNull(queryStr_);
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try {
      timestamp_ = df.parse(queryCtx.now_string).getTime() / 1000;
    } catch (java.text.ParseException e) {
      LOG.error("Error parsing timestamp value: " + queryCtx.now_string +
          " " + e.getMessage());
      timestamp_ = new Date().getTime() / 1000;
    }
    descTbl_ = analyzer.getDescTbl();
    user_ = analyzer.getUser().getName();
  }

  private void computeProjectionDependencies(List<Expr> resultExprs) {
    Preconditions.checkNotNull(resultExprs);
    Preconditions.checkState(!resultExprs.isEmpty());
    Preconditions.checkState(resultExprs.size() == targetColumnLabels_.size());
    for (int i = 0; i < resultExprs.size(); ++i) {
      Expr expr = resultExprs.get(i);
      Set<String> sourceBaseCols = Sets.newHashSet();
      List<Expr> dependentExprs = Lists.newArrayList();
      getSourceBaseCols(expr, sourceBaseCols, dependentExprs, false);
      Set<String> targets = Sets.newHashSet(targetColumnLabels_.get(i));
      createMultiEdge(targets, sourceBaseCols, MultiEdge.EdgeType.PROJECTION);
      if (!dependentExprs.isEmpty()) {
        // We have additional exprs that 'expr' has a predicate dependency on.
        // Gather the transitive predicate dependencies of 'expr' based on its direct
        // predicate dependencies. For each direct predicate dependency p, 'expr' is
        // transitively predicate dependent on all exprs that p is projection and
        // predicate dependent on.
        Set<String> predicateBaseCols = Sets.newHashSet();
        for (Expr dependentExpr: dependentExprs) {
          getSourceBaseCols(dependentExpr, predicateBaseCols, null, true);
        }
        createMultiEdge(targets, predicateBaseCols, MultiEdge.EdgeType.PREDICATE);
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
    Set<String> predicateBaseCols = Sets.newHashSet();
    for (Expr expr: resultDependencyPredicates_) {
      getSourceBaseCols(expr, predicateBaseCols, null, true);
    }
    if (predicateBaseCols.isEmpty()) return;
    Set<String> targets = Sets.newHashSet(targetColumnLabels_);
    createMultiEdge(targets, predicateBaseCols, MultiEdge.EdgeType.PREDICATE);
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
  private void getSourceBaseCols(Expr expr, Set<String> sourceBaseCols,
      List<Expr> directPredDeps, boolean traversePredDeps) {
    List<Expr> exprsToTraverse = getProjectionDeps(expr);
    List<Expr> predicateDepExprs = getPredicateDeps(expr);
    if (directPredDeps != null) directPredDeps.addAll(predicateDepExprs);
    if (traversePredDeps) exprsToTraverse.addAll(predicateDepExprs);
    List<SlotId> slotIds = Lists.newArrayList();
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
        sourceBaseCols.add(Joiner.on(".").join(path));
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
    List<Expr> outputExprs = Lists.newArrayList();
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
    List<Expr> outputExprs = Lists.newArrayList();
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
    Map obj = new LinkedHashMap();
    obj.put("queryText", queryStr_);
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
    graph.setHash(getQueryHash(queryStr_));
    graph.setUser(user_);
    graph.setStarted(timestamp_);
    // Add edges
    List<TMultiEdge> edges = Lists.newArrayList();
    for (MultiEdge edge: edges_) {
      edges.add(edge.toThrift());
    }
    graph.setEdges(edges);
    // Add vertices
    TreeSet<Vertex> sortedVertices = Sets.newTreeSet(vertices_.values());
    List<TVertex> vertices = Lists.newArrayList();
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
        new ColumnLineageGraph(obj.query_text, obj.user, obj.started);
    TreeSet<Vertex> vertices = Sets.newTreeSet();
    for (TVertex vertex: obj.vertices) {
      vertices.add(Vertex.fromThrift(vertex));
    }
    lineage.setVertices(vertices);
    for (TMultiEdge edge: obj.edges) {
      MultiEdge e = MultiEdge.fromThrift(edge);
      lineage.edges_.add(e);
    }
    return lineage;
  }

  private String getQueryHash(String queryStr) {
    Hasher hasher = Hashing.md5().newHasher();
    hasher.putString(queryStr);
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
    String hash = (String) jsonObj.get("hash");
    String user = (String) jsonObj.get("user");
    long timestamp = (Long) jsonObj.get("timestamp");
    ColumnLineageGraph graph = new ColumnLineageGraph(stmt, user, timestamp);
    JSONArray serializedVertices = (JSONArray) jsonObj.get("vertices");
    Set<Vertex> vertices = Sets.newHashSet();
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
    Set<Vertex> vertices = Sets.newHashSet();
    for (int i = 0; i < vertexIdArray.size(); ++i) {
      int sourceId = ((Long) vertexIdArray.get(i)).intValue();
      Vertex sourceVertex = idToVertexMap_.get(new VertexId(sourceId));
      Preconditions.checkNotNull(sourceVertex);
      vertices.add(sourceVertex);
    }
    return vertices;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    ColumnLineageGraph g = (ColumnLineageGraph) obj;
    if (!this.vertices_.equals(g.vertices_) ||
        !this.edges_.equals(g.edges_)) {
      return false;
    }
    return true;
  }

  public String debugString() {
    StringBuilder builder = new StringBuilder();
    for (MultiEdge edge: edges_) {
      builder.append(edge.toString() + "\n");
    }
    builder.append(toJson());
    return builder.toString();
  }

  public void addTargetColumnLabels(Collection<String> columnLabels) {
    Preconditions.checkNotNull(columnLabels);
    targetColumnLabels_.addAll(columnLabels);
  }

  public void addTargetColumnLabels(Table dstTable) {
    Preconditions.checkNotNull(dstTable);
    String tblFullName = dstTable.getFullName();
    for (String columnName: dstTable.getColumnNames()) {
      targetColumnLabels_.add(tblFullName + "." + columnName);
    }
  }
}
