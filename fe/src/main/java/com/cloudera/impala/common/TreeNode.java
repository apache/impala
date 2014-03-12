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

package com.cloudera.impala.common;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Predicate;

public class TreeNode<NodeType extends TreeNode<NodeType>> {
  protected ArrayList<NodeType> children_;

  protected TreeNode() {
    this.children_ = new ArrayList<NodeType>();
  }

  public NodeType getChild(int i) {
    return hasChild(i) ? children_.get(i) : null;
  }

  public boolean hasChild(int i) {
    return children_.size() > i;
  }

  public void addChild(NodeType n) {
    children_.add(n);
  }

  public void setChild(int index, NodeType n) {
    children_.set(index, n);
  }

  public ArrayList<NodeType> getChildren() {
    return children_;
  }

  /**
   * Add all nodes in the tree that satisfy 'predicate' to the list 'matches'
   * This node is checked first, followed by its children in order. If the node
   * itself matches, the children are skipped.
   */
  public <C extends TreeNode<NodeType>, D extends C> void collect(
      Predicate<? super C> predicate, List<D> matches) {
    if (predicate.apply((C) this) && !matches.contains(this)) {
      matches.add((D) this);
      return;
    }

    for (NodeType child: children_) {
      child.collect(predicate, matches);
    }
  }

  /**
   * For each expression in 'nodeList', collect all subexpressions satisfying 'predicate'
   * into 'matches'
   */
  public static <C extends TreeNode<C>, D extends C> void collect(
      List<C> nodeList, Predicate<? super C> predicate, List<D> matches) {
    for (C node : nodeList) {
      node.collect(predicate, matches);
    }
  }

  /**
   * Return true if this node or any of its children satisfy 'predicate'.
   */
  public <C extends TreeNode<NodeType>> boolean contains(
      Predicate<? super C> predicate) {
    if (predicate.apply((C) this)) return true;

    for (NodeType child: children_) {
      if (child.contains(predicate)) return true;
    }

    return false;
  }

  /**
   * For each expression in 'exprList', return true if any subexpression satisfies
   * contains('predicate').
   */
  public static <C extends TreeNode<C>, D extends C> boolean contains(
      List<C> nodeList, Predicate<? super C> predicate) {
    for (C node : nodeList) {
      if (node.contains(predicate)) return true;
    }

    return false;
  }
}
