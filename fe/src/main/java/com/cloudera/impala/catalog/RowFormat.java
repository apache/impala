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
 * Defines the physical (on-disk) format for a table's data. This is used when creating
 * a new table to specify how to interpret the fields (columns) and lines (rows) in a 
 * a data file.
 */
public class RowFormat {
  // Default row format
  public final static RowFormat DEFAULT_ROW_FORMAT = new RowFormat(null, null);

  private final String fieldDelimiter;
  private final String lineDelimiter;

  public RowFormat(String fieldDelimiter, String lineDelimiter) {
    this.fieldDelimiter = fieldDelimiter;
    this.lineDelimiter = lineDelimiter;
  }

  public String getFieldDelimiter() {
    return fieldDelimiter;
  }

  public String getLineDelimiter() {
    return lineDelimiter;
  }
}
