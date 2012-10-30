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

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.NotImplementedException;
import com.google.common.base.Preconditions;

public abstract class LiteralExpr extends Expr {

  public static LiteralExpr create(String value, PrimitiveType type) throws AnalysisException {
    Preconditions.checkArgument(type != PrimitiveType.INVALID_TYPE);
    switch (type) {
      case BOOLEAN:
        return new BoolLiteral(value);
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
        return new IntLiteral(value);
      case FLOAT:
      case DOUBLE:
        return new FloatLiteral(value);
      case STRING:
        return new StringLiteral(value);
      case DATE:
      case DATETIME:
      case TIMESTAMP:
        throw new AnalysisException("DATE/DATETIME/TIMESTAMP literals not supported: " + value);
    }
    return null;
  }

  // Swaps the sign of numeric literals.
  // Throws for non-numeric literals.
  public void swapSign() throws NotImplementedException {
    throw new NotImplementedException("swapSign() only implemented for numeric literals");
  }
}
