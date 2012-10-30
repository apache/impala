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

package com.cloudera.impala.analysis;

import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TLiteralPredicate;

public class LiteralPredicate extends Predicate {
  private final boolean value;
  private final boolean isNull;

  static public LiteralPredicate True() {
    return new LiteralPredicate(true, false);
  }

  static public LiteralPredicate False() {
    return new LiteralPredicate(false, false);
  }

  static public LiteralPredicate Null() {
    return new LiteralPredicate(false, true);
  }

  private LiteralPredicate(boolean val, boolean isNull) {
    super();
    this.value = val;
    this.isNull = isNull;
  }

  public boolean isNull() {
    return isNull;
  }

  public boolean getValue() {
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    return ((LiteralPredicate) obj).value == value;
  }

  @Override
  public String toSql() {
    if (isNull) {
      return "NULL";
    } else {
      return (value ? "TRUE" : "FALSE");
    }
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.LITERAL_PRED;
    msg.literal_pred = new TLiteralPredicate(value, isNull);
  }

}
