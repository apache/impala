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

import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.impala.util.treevis.Visualizer.TreeVisualizer;

import com.google.common.base.Preconditions;

/**
 * Rudimentary tree printer for use in debugging. Writes the
 * result to stdout in a semi-JSON-like format. Emphasis is
 * on human readability, not accurate serialization.
 */
public class TreePrinter implements TreeVisualizer {

  private enum LevelType { OBJ, ARRAY }

  private final PrintWriter out;
  private final Deque<LevelType> levels = new ArrayDeque<>();

  public TreePrinter(PrintWriter out) {
    this.out = out;
  }

  @Override
  public void startObj(String name, Object obj) {
    indent();
    printName(name);
    out.print(" (");
    out.print(obj.getClass().getSimpleName());
    out.println( "): {");
    levels.push(LevelType.OBJ);
  }

  private void printName(String name) {
    name = name.replaceAll("_$", "");
    out.print(name);
  }

  @Override
  public void startArray(String name) {
    indent();
    printName(name);
    out.println( ": [");
    levels.push(LevelType.ARRAY);
  }

  @Override
  public void field(String name, Object value) {
    indent();
    printName(name);
    out.print(": ");
    if (value == null) {
      out.println("<null>");
    } else if (value instanceof String) {
      out.print("\"");
      out.print(value);
      out.println("\"");
    } else {
      out.println(value.toString());
    }
  }

  @Override
  public void endArray() {
    Preconditions.checkState(! levels.isEmpty());
    Preconditions.checkState(levels.peek() == LevelType.ARRAY);
    levels.pop();
    indent();
    out.println("]");
  }

  @Override
  public void endObj() {
    Preconditions.checkState(! levels.isEmpty());
    Preconditions.checkState(levels.peek() == LevelType.OBJ);
    levels.pop();
    indent();
    out.println("}");
  }

  private void indent() {
    for (int i = 0; i < levels.size(); i++) {
      out.print(". ");
    }
  }

  @Override
  public void special(String name, String value) {
    indent();
    printName(name);
    out.print(": ");
    out.println(value);
  }
}
