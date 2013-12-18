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

  // Collect all unique subnodes of type C (but not of subclasses of C), including
  // possibly 'this' if the subnode does not exist in subNodes.
  // If a subnode is collected, none of its own subnodes will
  // be.
  public <C extends NodeType> void collect(Class<C> cl, List<C> subNodes) {
    if (cl.isAssignableFrom(this.getClass()) && this.getClass().isAssignableFrom(cl) &&
        !subNodes.contains((C) this)) {
      subNodes.add((C) this);
      return;
    }
    for (NodeType child: children_) {
      child.collect(cl, subNodes);
    }
  }

  // Collect all unique subnodes of type C (or subclasses of C), including
  // possibly 'this' if the subnode does not exist in subNodes.
  // If a subnode is collected, none of its own subnodes will be.
  public <C extends NodeType> void collectSubclasses(Class<C> cl, List<C> subNodes) {
    if (cl.isAssignableFrom(this.getClass()) && !subNodes.contains((C) this)) {
      subNodes.add((C) this);
      return;
    }
    for (NodeType child: children_) {
      child.collectSubclasses(cl, subNodes);
    }
  }

  public boolean contains(Class cl) {
    if (cl.isAssignableFrom(this.getClass()) && this.getClass().isAssignableFrom(cl)) {
      return true;
    }
    for (NodeType child: children_) {
      if (child.contains(cl)) {
        return true;
      }
    }
    return false;
  }

  public boolean containsSubclass(Class cl) {
    if (cl.isAssignableFrom(this.getClass())) {
      return true;
    }
    for (NodeType child: children_) {
      if (child.containsSubclass(cl)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Return 'this' or first child that is exactly of class 'cl'.
   * Looks for matching children via depth-first, left-to-right traversal.
   */
  public <C extends NodeType> C findFirstOf(Class<C> cl) {
    if (this.getClass().equals(cl)) {
      return (C) this;
    }
    for (NodeType child: children_) {
      NodeType result = child.findFirstOf(cl);
      if (result != null) {
        return (C) result;
      }
    }
    return null;
  }

}
