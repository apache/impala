// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.common;

/**
 * "Indirection layer" that allows returning an object via an output
 * parameter of a function call, similar to a pointer or reference parameter
 * in C/C++.
 * Example:
 *   Reference<T> ref = new Reference<T>();
 *   createT(ref);  // calls ref.setRef()
 *   <do something with ref.getRef()>;
 */
public class Reference<RefType> {
  protected RefType ref;

  public Reference(RefType ref) {
    this.ref = ref;
  }

  public Reference() {
    this.ref = null;
  }

  public RefType getRef() {
    return ref;
  }

  public void setRef(RefType ref) {
    this.ref = ref;
  }
}
