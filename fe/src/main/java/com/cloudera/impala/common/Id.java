// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.common;

import java.util.ArrayList;
import com.google.common.collect.Lists;

/**
 * Integer ids that cannot accidentally be compared with ints.
 *
 */
public class Id<IdType extends Id<IdType>> {
  protected final int id;

  static private int INVALID_ID = -1;

  public Id() {
    this.id = INVALID_ID;
  }

  public Id(int id) {
    this.id = id;
  }

  public boolean isValid() { return id != INVALID_ID; }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    // only ids of the same subclass are comparable
    if (obj.getClass() != this.getClass()) {
      return false;
    }
    return ((Id)obj).id == id;
  }

  @Override
  public int hashCode() {
    return Integer.valueOf(id).hashCode();
  }

  public int asInt() {
    return id;
  }

  public ArrayList<IdType> asList() {
    ArrayList<IdType> list = new ArrayList<IdType>();
    list.add((IdType) this);
    return list;
  }

  public String toString() {
    return Integer.toString(id);
  }
}
