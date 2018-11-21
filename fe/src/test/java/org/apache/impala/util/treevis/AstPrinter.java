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
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.ScalarType;

/**
 * Utility class to write a raw or decorated Impala AST
 * to the console for debugging, with default formatting
 * options.
 */
public class AstPrinter {

  public static void printTree(ParseNode node) {
    Visualizer vis = new Visualizer(
        new TreePrinter(new PrintWriter(
            new OutputStreamWriter(System.out),
            true)));
    vis.ignore(TupleDescriptor.class);
    vis.ignore(HdfsTable.class);
    vis.ignore(Analyzer.class);
    vis.scalar(ScalarType.class);
    vis.visualize(node);
  }
}
