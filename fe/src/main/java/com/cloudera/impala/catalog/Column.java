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

package com.cloudera.impala.catalog;


/**
 * Internal representation of column-related metadata.
 * Owned by Catalog instance.
 */
public class Column {
  private final String name;
  private final PrimitiveType type;
  private final String comment;
  private int position;  // in table

  public Column(String name, PrimitiveType type, int position) {
    this(name, type, null, position);
  }

  public Column(String name, PrimitiveType type, String comment, int position) {
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.position = position;
  }

  public String getComment() {
    return comment;
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
