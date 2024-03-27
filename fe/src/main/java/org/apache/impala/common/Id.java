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

package org.apache.impala.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.base.Joiner;

/**
 * Integer ids that cannot accidentally be compared with ints.
 */
public class Id<IdType extends Id<IdType>> implements Comparable<Id<IdType>> {
  protected final int id_;

  public Id(int id) {
    this.id_ = id;
  }

  public int asInt() { return id_; }

  @Override
  public int hashCode() { return id_; }

  @Override
  public String toString() {
    return Integer.toString(id_);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    // only ids of the same subclass are comparable
    if (obj.getClass() != this.getClass()) return false;
    return ((Id)obj).id_ == id_;
  }

  @Override
  public int compareTo(Id<IdType> cmp) {
    return id_ - cmp.id_;
  }

  public ArrayList<IdType> asList() {
    ArrayList<IdType> list = new ArrayList<IdType>();
    list.add((IdType) this);
    return list;
  }

  public static <C extends Id> String printIds(Collection<C> ids) {
    ArrayList<String> l = new ArrayList<>();
    for (C id: ids) {
      l.add(id.toString());
    }
    return "(" + Joiner.on(" ").join(l) + ")";
  }

  /**
   * Check whether l1 and l2 have intersect.
   */
  public static <C extends Id> boolean intersect(List<C> l1, Set<C> l2) {
    for (C element: l1) {
      if (l2.contains(element)) return true;
    }
    return false;
  }
}
