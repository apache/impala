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

  private final PrintWriter out_;
  private final Deque<LevelType> levels_ = new ArrayDeque<>();

  public TreePrinter(PrintWriter out) {
    this.out_ = out;
  }

  @Override
  public void startObj(String name, Object obj) {
    objHeader(name, obj);
    out_.println( "{");
    levels_.push(LevelType.OBJ);
  }

  private void objHeader(String name, Object obj) {
    indent();
    printName(name);
    out_.print(" (");
    out_.print(obj.getClass().getSimpleName());
    out_.print(", ");
    out_.print(System.identityHashCode(obj) % 1000);
    out_.print( "): ");
  }

  private void printName(String name) {
    // Remove Impala's trailing _
    // foo_ --> foo
    name = name.replaceAll("_$", "");
    out_.print(name);
  }

  @Override
  public void startArray(String name) {
    indent();
    printName(name);
    out_.println( ": [");
    levels_.push(LevelType.ARRAY);
  }

  @Override
  public void field(String name, Object value) {
    indent();
    printName(name);
    out_.print(": ");
    if (value == null) {
      out_.println("<null>");
    } else if (value instanceof String) {
      out_.print("\"");
      out_.print(value);
      out_.println("\"");
    } else {
      out_.println(value.toString());
    }
  }

  @Override
  public void endArray() {
    Preconditions.checkState(! levels_.isEmpty());
    Preconditions.checkState(levels_.peek() == LevelType.ARRAY);
    levels_.pop();
    indent();
    out_.println("]");
  }

  @Override
  public void endObj() {
    Preconditions.checkState(! levels_.isEmpty());
    Preconditions.checkState(levels_.peek() == LevelType.OBJ);
    levels_.pop();
    indent();
    out_.println("}");
  }

  private void indent() {
    for (int i = 0; i < levels_.size(); i++) {
      out_.print(". ");
    }
  }

  @Override
  public void elide(String name, Object obj, String reason) {
    objHeader(name, obj);
    if (reason.equals("[]")) {
      out_.println(reason);
    } else {
      out_.print("<");
      out_.print(reason);
      out_.println(">");
    }
  }

  @Override
  public void emptyArray(String name) {
    indent();
    printName(name);
    out_.println(": []");
  }
}
