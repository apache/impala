// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.common;

/**
 * Generator of consecutively numbered integers to be used as ids
 * by subclasses of Id, like so:
 * IdGenerator<TupleId> tupleIdGenerator = ...;
 * TupleId id = new TupleId(idGenerator);
 * The templatization prevents getting id generators that are used for different
 * Id subclasses mixed up.
 *
 */
public class IdGenerator<IdType extends Id<IdType>> {
  private int nextId = 0;

  public IdGenerator() {}

  public int getNextId() {
    return nextId++;
  }
}
