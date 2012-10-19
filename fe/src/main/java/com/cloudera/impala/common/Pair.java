// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

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

  @Override
  /**
   * A pair is equal if both parts are equal().
   */
  public boolean equals(Object o) {
    if (o instanceof Pair) {
      Pair<F,S> other = (Pair<F,S>) o;
      return this.first.equals(other.first) && this.second.equals(other.second);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashFirst = first != null ? first.hashCode() : 0;
    int hashSecond = second != null ? second.hashCode() : 0;

    return (hashFirst + hashSecond) * hashSecond + hashFirst;
  }

  static public <F, S> Pair<F, S> create(F first, S second) {
    return new Pair<F, S>(first, second);
  }
}
