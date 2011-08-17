// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;


/**
 * Internal representation of column-related metadata.
 * Owned by Catalog instance.
 */
public class Column {
  protected final String name;
  protected final PrimitiveType type;
  protected int position;  // in table

  public Column(String name, PrimitiveType type, int position) {
    this.name = name;
    this.type = type;
    this.position = position;
  }

  public String getName() {
    return name;
  }

  public PrimitiveType getType() {
    return type;
  }

  public int getPosition() {
    return position;
  }

  public void setPosition(int position) {
    this.position = position;
  }

}
