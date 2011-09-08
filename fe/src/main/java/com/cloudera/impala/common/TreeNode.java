// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.common;

import java.util.ArrayList;
import java.util.List;

public class TreeNode<NodeType extends TreeNode<NodeType>> {
  protected ArrayList<NodeType> children;

  protected TreeNode() {
    this.children = new ArrayList<NodeType>();
  }

  public NodeType getChild(int i) {
    return hasChild(i) ? children.get(i) : null;
  }

  public boolean hasChild(int i) {
    return children.size() > i;
  }

  public void addChild(NodeType n) {
    children.add(n);
  }

  public void setChild(int index, NodeType n) {
    children.set(index, n);
  }

  public ArrayList<NodeType> getChildren() {
    return children;
  }

  // Collect all subnodes of type C (but not of subclasses of C), including
  // possibly 'this'. If a subnode is collected, none of its own subnodes will
  // be.
  public <C extends NodeType> void collect(Class<C> cl, List<C> subNodes) {
    if (cl.isAssignableFrom(this.getClass()) && this.getClass().isAssignableFrom(cl)) {
      subNodes.add((C) this);
      return;
    }
    for (NodeType child: children) {
      child.collect(cl, subNodes);
    }
  }

  // Collect all subnodes of type C (or subclasses of C), including
  // possibly 'this'. If a subnode is collected, none of its own subnodes will
  // be.
  public <C extends NodeType> void collectSubclasses(Class<C> cl, List<C> subNodes) {
    if (cl.isAssignableFrom(this.getClass())) {
      subNodes.add((C) this);
      return;
    }
    for (NodeType child: children) {
      child.collectSubclasses(cl, subNodes);
    }
  }

  public boolean contains(Class cl) {
    if (cl.isAssignableFrom(this.getClass()) && this.getClass().isAssignableFrom(cl)) {
      return true;
    }
    for (NodeType child: children) {
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
    for (NodeType child: children) {
      if (child.containsSubclass(cl)) {
        return true;
      }
    }
    return false;
  }
}
