/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.operators;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.NlsString;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.catalog.Type;
import org.apache.impala.calcite.type.ImpalaTypeConverter;

import java.math.BigDecimal;

/**
 * Factory for row expressions.
 *
 * See the base class in Calcite for the details about RexBuilder. This extension
 * allows Impala to override functions that need extra work to make RexBuilder
 * Impala compatible.
 *
 * This RexBuilder is used at RelNodeConverter time. The extra 'makeLiteral'
 * method causes numbers like '3' to be created as a tinyint rather than a
 * an int (see CALCITE-7120). We do not want to use this at optimization time.
 * When we do constant folding, we want to create literals with the type that
 * Impala provides, not the default type.
 *
 * For instance, take the expression '1 + 2'.  At parsing time, we want the
 * individual literals to be tinyints here.  However, after constant folding,
 * the '3' will be a smallint, as defined by the '+' coercing rules. So we
 * want the provided 'smallint' type to be used for '3' and thus do not want
 * to call the makeLiteral in this class.
 *
 */
public class ImpalaRexBuilder extends RexBuilder {

  private boolean postAnalysis_ = false;

  public ImpalaRexBuilder(RelDataTypeFactory typeFactory) {
    super(typeFactory);
  }

  public void setPostAnalysis() {
    postAnalysis_ = true;
  }

  @Override
  public RexLiteral makeLiteral(Comparable o, RelDataType type, SqlTypeName typeName) {
    // CALCITE-7120: Calcite always creates tinyint and smallint literals as Integer, but
    // Impala needs them as tinyints and smallints. This should only be done during
    // analysis and pre-optimization. At optimization time, the literal has been analyzed
    // and the type is explicitly set by the Impala caller. For instance, in constant
    // folding, if a '1 + 1' is folded to a '2', the '2' will be explicitly set to a
    // smallint rather than a tinyint because of the "+" operation.
    if (!postAnalysis_) {
      if (SqlTypeUtil.isIntType(type) && o instanceof BigDecimal) {
        BigDecimal bd0 = (BigDecimal) o;
        type = ImpalaTypeConverter.getLiteralDataType(bd0, type);
      } else if (SqlTypeUtil.inCharFamily(typeName)) {
        // The last parameter is for unescaping. We only want to unescape on the
        // conversion from AST literals to Rex literals, right before we move
        // on to the post analysis phase.
        return makeStringLiteral(o, typeName, !postAnalysis_);
      }
    }

    return super.makeLiteral(o, type, typeName);
  }

  public RexLiteral makeStringLiteral(Comparable o, SqlTypeName typeName,
      boolean unescape) {
    RelDataType type = ImpalaTypeConverter.getRelDataType(Type.STRING, false);
    if (unescape) {
      if (o instanceof NlsString) {
        NlsString nlsString = (NlsString) o;
        // Same decoding StringLiteral applies when it sends the literal to
        // the backend. '\061' and '1' are then the same value.
        String unescaped = new StringLiteral(nlsString.getValue(), Type.STRING,
            true).unescapeIfNeeded();
        o = new NlsString(unescaped, nlsString.getCharsetName(),
            nlsString.getCollation());
      }
    }
    return super.makeLiteral(o, type, typeName);
  }

  @Override
  public RexNode makeCast(
      SqlParserPos pos,
      RelDataType type,
      RexNode exp,
      boolean matchNullability,
      boolean safe,
      RexLiteral format) {
    // This "if" exists in Calcite, but calls makeCastBooleanToExact() which produces
    // a case statement. This override avoids this situation and just produces the
    // cast like the original planner does.
    if (exp.getType().getSqlTypeName() == SqlTypeName.BOOLEAN &&
        SqlTypeUtil.isExactNumeric(type)) {
      return makeAbstractCast(pos, type, exp, safe, format);
    }
    return super.makeCast(pos, type, exp, matchNullability, safe, format);
  }
}
