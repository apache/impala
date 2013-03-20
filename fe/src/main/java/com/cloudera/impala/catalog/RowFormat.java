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

import com.cloudera.impala.analysis.StringLiteral;
import com.cloudera.impala.thrift.TTableRowFormat;

/**
 * Defines the physical (on-disk) format for a table's data. This is used when creating
 * a new table to specify how to interpret the fields (columns) and lines (rows) in a 
 * a data file.
 */
public class RowFormat {
  // Default row format
  public final static RowFormat DEFAULT_ROW_FORMAT = new RowFormat(null, null, null);

  private final String fieldDelimiter;
  private final String lineDelimiter;
  private final String escapeChar;

  private RowFormat(String fieldDelimiter, String lineDelimiter, String escapeChar,
      boolean unescape) {
    if (unescape) {
      this.fieldDelimiter = getUnescapedValueOrNull(fieldDelimiter);
      this.lineDelimiter = getUnescapedValueOrNull(lineDelimiter);
      this.escapeChar = getUnescapedValueOrNull(escapeChar);
    } else {
      this.fieldDelimiter = fieldDelimiter;
      this.lineDelimiter = lineDelimiter;
      this.escapeChar = escapeChar;
    }
  }

  /**
   * Creates a new instance of the RowFormat class, unescaping the values of
   * field delimiter, line delimiter, and escape char.
   */
  public RowFormat(String fieldDelimiter, String lineDelimiter, String escapeChar) {
    this(fieldDelimiter, lineDelimiter, escapeChar, true);
  }

  public String getFieldDelimiter() {
    return fieldDelimiter;
  }

  public String getLineDelimiter() {
    return lineDelimiter;
  }

  public String getEscapeChar() {
    return escapeChar;
  }

  private static String getUnescapedValueOrNull(String value) {
    return value == null ? null : new StringLiteral(value).getUnescapedValue();
  }

  public TTableRowFormat toThrift() {
    TTableRowFormat tableRowFormat = new TTableRowFormat();
    tableRowFormat.setField_terminator(getFieldDelimiter());
    tableRowFormat.setLine_terminator(getLineDelimiter());
    tableRowFormat.setEscaped_by(getEscapeChar());
    return tableRowFormat;
  }

  public static RowFormat fromThrift(TTableRowFormat tableRowFormat) {
    if (tableRowFormat == null) {
      return RowFormat.DEFAULT_ROW_FORMAT;
    }
    // When creating a RowFormat from thrift, don't unescape the values, they should have
    // already been unescaped.
    return new RowFormat(tableRowFormat.getField_terminator(),
        tableRowFormat.getLine_terminator(), tableRowFormat.getEscaped_by(), false);
  }
}
