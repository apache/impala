// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.common;

/**
 * The equivalent of C++'s std::pair<>.
 */
public class Pair<F, S> {
  public F first;
  public S second;

  public Pair(F first, S second) {
    this.first = first;
    this.second = second;
  }

  static public <F, S> Pair<F, S> create(F first, S second) {
    return new Pair<F, S>(first, second);
  }
}
