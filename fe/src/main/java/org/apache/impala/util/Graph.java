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

package org.apache.impala.util;

import com.google.common.base.Preconditions;
import org.apache.impala.common.Pair;

import java.util.*;

import static java.lang.Math.min;

/** Data structures for graphs represented with an adjacency list. */
public abstract class Graph {
  public abstract int numVertices();

  /** Return an iterator of vertex IDs with an edge from srcVid. */
  public abstract IntIterator dstIter(int srcVid);

  public String print() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numVertices(); ++i) {
      IntIterator dstIter = dstIter(i);
      if (!dstIter.hasNext()) continue;
      sb.append(i).append(" => ");
      while (dstIter.hasNext()) {
        sb.append(dstIter.next()).append(", ");
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  /**
   * Helper function collecting all the indexes set in a bitset into an sorted array.
   * Time complexity: O(n), where n is the number of bits in the bitset.
   */
  static private int[] collectIdxsFromBitSet(BitSet bs) {
    int[] result = new int[bs.cardinality()];
    for (int i = 0, j = bs.nextSetBit(0); j != -1; j = bs.nextSetBit(j + 1)) {
      result[i++] = j;
    }
    return result;
  }

  /** Graph supporting adding edges. Duplicate edges are allowed. */
  public static class WritableGraph extends Graph {
    // Unsorted adjacency list.
    private final IntArrayList[] adjList_;

    public WritableGraph(int numVertices) {
      adjList_ = new IntArrayList[numVertices];
      for (int i = 0; i < numVertices; ++i) adjList_[i] = new IntArrayList();
    }

    @Override
    public int numVertices() { return adjList_.length; }

    @Override
    public IntIterator dstIter(int srcVid) { return adjList_[srcVid].iterator(); }

    public void addEdge(int srcVid, int dstVid) {
      if (dstVid < 0 || dstVid >= numVertices()) throw new IndexOutOfBoundsException();
      adjList_[srcVid].add(dstVid);
    }

    public RandomAccessibleGraph toRandomAccessible() {
      int[][] sortedAdjList = new int[numVertices()][];
      for (int srcVid = 0; srcVid < numVertices(); ++srcVid) {
        int[] dsts = Arrays.copyOfRange(adjList_[srcVid].data(), 0,
            adjList_[srcVid].size());
        Arrays.sort(dsts);
        int uniqueSize = 0;
        for (int i = 0; i < dsts.length; ++i) {
          if (i == 0 || dsts[i] != dsts[i - 1]) dsts[uniqueSize++] = dsts[i];
        }
        sortedAdjList[srcVid] = Arrays.copyOfRange(dsts, 0, uniqueSize);
      }
      return new RandomAccessibleGraph(sortedAdjList);
    }
  }

  /** Graph supporting random access using binary search. */
  public static class RandomAccessibleGraph extends Graph {
    // Sorted adjacency list.
    private final int[][] adjList_;

    /**
     * Construct from an adjacency list.
     * Each list in the adjacency list must have been sorted.
     */
    RandomAccessibleGraph(int[][] adjList) { adjList_ = adjList; }

    @Override
    public int numVertices() { return adjList_.length; }

    @Override
    public IntIterator dstIter(int srcVid) {
      return IntIterator.fromArray(adjList_[srcVid]);
    }

    /**
     * Check whether there is an edge from 'srcVid' to 'dstVid'. Binary search is used
     * instead of a standalone hash table because the typical use case has less than
     * 10,000 vertices.
     * Time complexity: O(log(V))
     */
    boolean hasEdge(int srcVid, int dstVid) {
      int idx = Arrays.binarySearch(adjList_[srcVid], dstVid);
      return idx >= 0 && idx < adjList_[srcVid].length && adjList_[srcVid][idx] == dstVid;
    }

    /**
     * Compute the reflexive transitive closure of the graph by BFS from every vertex.
     * Time complexity: O(V(V+E)).
     */
    public RandomAccessibleGraph reflexiveTransitiveClosure() {
      int[][] tcAdjList = new int[numVertices()][];
      BitSet visited = new BitSet(numVertices());
      IntArrayList queue = new IntArrayList(numVertices());
      for (int srcVid = 0; srcVid < numVertices(); ++srcVid) {
        visited.clear();
        visited.set(srcVid);
        queue.clear();
        queue.add(srcVid);
        for (int queueFront = 0; queueFront < queue.size(); ++queueFront) {
          for (IntIterator dstIt = dstIter(queue.get(queueFront));
               dstIt.hasNext(); dstIt.next()) {
            if (!visited.get(dstIt.peek())) {
              visited.set(dstIt.peek());
              queue.add(dstIt.peek());
            }
          }
        }
        tcAdjList[srcVid] = collectIdxsFromBitSet(visited);
      }
      return new RandomAccessibleGraph(tcAdjList);
    }
  }

  /**
   * A graph condensed by its strongly-connected components (SCC). Vertices are mapped to
   * their SCCs and an inner graph on the SCCs is stored.
   */
  public static class SccCondensedGraph extends Graph {
    // Map an original vid to its SCC ID.
    private final int[] sccIds_;
    // Map an SCC ID to its member vids.
    private final int[][] sccMembers_;
    // The SCC-condensed inner graph.
    private final RandomAccessibleGraph condensed_;

    private SccCondensedGraph(int[] sccIds, int[][] sccMembers,
        RandomAccessibleGraph condensed) {
      sccIds_ = sccIds;
      sccMembers_ = sccMembers;
      condensed_ = condensed;
    }

    @Override
    public int numVertices() { return sccIds_.length; }

    @Override
    public IntIterator dstIter(final int srcVid) {
      return new IntIterator() {
        private final int[] condensedAdjList = condensed_.adjList_[sccIds_[srcVid]];
        private int adjListPos = 0;
        private int memberPos = 0;

        @Override
        public boolean hasNext() {
          // After this loop the iterator either points to a valid dst or reaches the end.
          while (adjListPos < condensedAdjList.length &&
              memberPos == sccMembers_[condensedAdjList[adjListPos]].length) {
            ++adjListPos;
            memberPos = 0;
          }
          return adjListPos < condensedAdjList.length;
        }

        @Override
        public int next() {
          int result = peek();
          ++memberPos;
          return result;
        }

        @Override
        public int peek() {
          if (!hasNext()) throw new IndexOutOfBoundsException();
          return sccMembers_[condensedAdjList[adjListPos]][memberPos];
        }
      };
    }

    /**
     * Check whether there is an edge from 'srcVid' to 'dstVid'.
     * Time complexity: O(log(V))
     */
    public boolean hasEdge(int srcVid, int dstVid) {
      return condensed_.hasEdge(sccIds_[srcVid], sccIds_[dstVid]);
    }

    /**
     * Create a condensed reflexive transitive closure of a graph.
     * Time complexity: O(V(V+E)).
     */
    public static SccCondensedGraph condensedReflexiveTransitiveClosure(WritableGraph g) {
      // Step 0: Compute the strongly connected components. O(V+E)
      Pair<int[], int[][]> scc = tarjanScc(g);
      // Step 1: Compute the condensed inner graph. O(V^2+E)
      RandomAccessibleGraph condensed = condenseGraphOnScc(g, scc.first, scc.second);
      // Step 2: Compute the reflexive transitive closure. O(V(V+E))
      RandomAccessibleGraph condensedTc = condensed.reflexiveTransitiveClosure();
      return new SccCondensedGraph(scc.first, scc.second, condensedTc);
    }

    /**
     * Get the ID of the strongly connected component containing 'vid'.
     * Time complexity: O(1)
     */
    public int sccId(int vid) { return sccIds_[vid]; }

    /**
     * Get an array of vertex IDs in the scc. The caller shouldn't modify the returned
     * array.
     * Time complexity: O(1)
     */
    public int[] sccMembersBySccId(int sccId) { return sccMembers_[sccId]; }

    /**
     * Get an array of vertices IDs in the scc. The caller shouldn't modify the returned
     * array.
     * Time complexity: O(1)
     */
    public int[] sccMembersByVid(int vid) { return sccMembers_[sccIds_[vid]]; }

    /**
     * Compute the strongly connected components using Tarjan's strongly connected
     * component algorithm.
     * https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
     * To avoid unbounded system stack usage, the algorithm is implemented iteratively.
     * Time complexity: O(V+E).
     * Returns A pair of {@link #sccIds_} and {@link #sccMembers_}.
     */
    static private Pair<int[], int[][]> tarjanScc(final WritableGraph g) {
      // A mapping from original vid to its SCC ID.
      int[] sccIds = new int[g.numVertices()];
      // Lists of vertex IDs in each SCC.
      ArrayList<int[]> sccMembers = new ArrayList<>();
      // A mapping from a vertex ID to its DFS preordering index. -1 means not visited.
      int[] dfsIdxs = new int[g.numVertices()];
      Arrays.fill(dfsIdxs, -1);
      int[] lowLinks = new int[g.numVertices()];
      // The stack of visited vertices that haven't been assigned to an SCC.
      IntArrayList unAssignedStack = new IntArrayList(g.numVertices());
      // The context of the iteratively implemented DFS.
      class DfsContext {
        // The vid being searched.
        final int vid;
        // The current position in the vertex's dst list. In each iteration, the dst
        // vid in this position should be considered for DFS.
        IntIterator dstIt;
        // The position of vid in unAssignedStack. When the DFS of the successors
        // finishes, vertices in unAssignedStack above this position belong to the same
        // SCC as vid.
        int unAssignedStackPos;
        DfsContext(int vid) {
          this.vid = vid;
          dstIt = g.dstIter(vid);
          // unAssignedStackPos will be initialized in DFS index assignment step.
        }
      }
      // The DFS stack. java.util.Stack is synchronized, so ArrayDeque is used here.
      ArrayDeque<DfsContext> dfsStack = new ArrayDeque<>();
      BitSet onUnassignedStack = new BitSet(g.numVertices());
      int nextDfsIndex = 0;
      for (int dfsRootVid = 0; dfsRootVid < g.numVertices(); ++dfsRootVid) {
        if (dfsIdxs[dfsRootVid] != -1) continue;
        dfsStack.push(new DfsContext(dfsRootVid));
        while (!dfsStack.isEmpty()) {
          DfsContext ctx = dfsStack.peek();
          if (dfsIdxs[ctx.vid] == -1) {
            dfsIdxs[ctx.vid] = lowLinks[ctx.vid] = nextDfsIndex;
            nextDfsIndex += 1;
            ctx.unAssignedStackPos = unAssignedStack.size();
            unAssignedStack.add(ctx.vid);
            onUnassignedStack.set(ctx.vid);
          }
          if (!ctx.dstIt.hasNext()) {
            // All successors have been searched. Check if this is the root of an SCC.
            if (lowLinks[ctx.vid] == dfsIdxs[ctx.vid]) {
              // Create an SCC from all elements on the unAssignedStack above (inclusive)
              // the vertex being searched.
              int[] members = Arrays.copyOfRange(unAssignedStack.data(),
                  ctx.unAssignedStackPos, unAssignedStack.size());
              unAssignedStack.removeLast(members.length);
              for (int member : members) {
                sccIds[member] = sccMembers.size();
                onUnassignedStack.clear(member);
              }
              sccMembers.add(members);
            }
            dfsStack.pop();
          } else {
            int nextDstVid = ctx.dstIt.peek();
            if (dfsIdxs[nextDstVid] == -1) {
              // Tree edge. DFS this successor. ctx.dstIt is not advanced until the DFS
              // of the successor finishes.
              dfsStack.push(new DfsContext(nextDstVid));
            } else {
              if (onUnassignedStack.get(nextDstVid)) {
                if (dfsIdxs[nextDstVid] >= dfsIdxs[ctx.vid]) {
                  // Tree edge. The DFS of a successor just finished.
                  // Take its lowlink value.
                  lowLinks[ctx.vid] = min(lowLinks[ctx.vid], lowLinks[nextDstVid]);
                } else {
                  // Back or cross edge. Take its DFS index value.
                  lowLinks[ctx.vid] = min(lowLinks[ctx.vid], dfsIdxs[nextDstVid]);
                }
              }
              ctx.dstIt.next();
            }
          }
        }
        Preconditions.checkState(unAssignedStack.size() == 0);
      }
      return Pair.create(sccIds, sccMembers.toArray(new int[0][]));
    }

    /**
     * Condense the original graph 'g' to a new graph in SCC space.
     * Time complexity: O(V^2+E)
     */
    static private RandomAccessibleGraph condenseGraphOnScc(WritableGraph g, int[] sccIds,
        int[][] sccMembers) {
      int[][] condensedAdjList = new int[sccMembers.length][];
      BitSet bs = new BitSet(sccMembers.length);
      for (int srcSccId = 0; srcSccId < sccMembers.length; ++srcSccId) {
        bs.clear();
        for (int srcVid : sccMembers[srcSccId]) {
          for (IntIterator dstIt = g.dstIter(srcVid); dstIt.hasNext(); dstIt.next()) {
            bs.set(sccIds[dstIt.peek()]);
          }
        }
        condensedAdjList[srcSccId] = collectIdxsFromBitSet(bs);
      }
      return new RandomAccessibleGraph(condensedAdjList);
    }

    /** Returns whether this graph is equal to 'reference'. */
    public boolean validate(RandomAccessibleGraph reference) {
      if (reference.numVertices() != numVertices()) return false;
      IntArrayList sortedDsts = new IntArrayList(reference.numVertices());
      for (int i = 0; i < reference.numVertices(); ++i) {
        sortedDsts.clear();
        for (IntIterator dstIt = dstIter(i); dstIt.hasNext(); dstIt.next()) {
          sortedDsts.add(dstIt.peek());
        }
        Arrays.sort(sortedDsts.data(), 0, sortedDsts.size());
        IntIterator refIt = reference.dstIter(i);
        IntIterator condensedIt = sortedDsts.iterator();
        while (refIt.hasNext() || condensedIt.hasNext()) {
          if (!refIt.hasNext() || !condensedIt.hasNext() ||
              refIt.next() != condensedIt.next()) {
            return false;
          }
        }
      }
      return true;
    }
  }
}
