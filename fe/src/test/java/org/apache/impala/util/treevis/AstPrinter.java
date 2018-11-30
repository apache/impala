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

package org.apache.impala.util.treevis;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.ParseNode;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.ScalarType;

/**
 * Utility class to write a raw or decorated Impala AST
 * to the console for debugging, with default formatting
 * options.
 */
public class AstPrinter {

  public static final int NODE_DEPTH = 3;
  public static final int TREE_DEPTH = 20;

  private static ParseNode lastNode;

  /**
   * Print a subtree to a depth of three. Use this form for easy
   * debugging. In Eclipse, open the Expressions view and enter:
   *
   * org.apache.impala.util.treevis.AstPrinter.print(yourVar)
   *
   * A handy trick when debugging unit tests is to set a breakpoint
   * in JUnit's fail() method (or an exception breakpoint on
   * AssertionError), then walk the tree back up to a location that
   * has visibility to the AST, and use the above to print it.
   *
   * Again, in Eclipse, you can enable and disable the expression,
   * no need to add and remove it. Eclipse evaluates the expression
   * each time you touch it. To avoid double-printing the same object,
   * the method skips printing if called twice on the same object.
   *
   * @param node the root node of the subtree to print
   */
  public static void print(ParseNode node) {
    // Don't print the same node twice. Works around the
    // awkward double-eval when enabling and disabling expressions
    // in Eclipse. Use printNode directly if you do want to display
    // the same node multiple times.
    if (node == lastNode) return;
    printTree(node, 3);
    lastNode = node;
  }

  /**
   * Print an entire (sub-)tree to its entire depth.
   * @param node
   */
  public static void printNode(ParseNode node) {
    printTree(node, NODE_DEPTH);
  }

  public static void printTree(ParseNode node) {
    printTree(node, TREE_DEPTH);
  }

  public static void printTree(ParseNode node, int maxDepth) {
    Visualizer vis = new Visualizer(
        new TreePrinter(new PrintWriter(
            new OutputStreamWriter(System.out),
            true)));
    vis.ignore(TupleDescriptor.class);
    vis.ignore(HdfsTable.class);
    vis.ignore(Analyzer.class);
    vis.scalar(ScalarType.class);
    vis.scalar(Function.class);
    vis.scalar(ScalarFunction.class);
    vis.depthLimit(maxDepth);
    vis.visualize(node);
  }
}
