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

package org.apache.impala.calcite.validate;

import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.validate.SqlConformance;

/**
 * ImpalaConformance describes supported Calcite features.
 * For more info on the description of these methods, see:
 * https://calcite.apache.org/javadocAggregate/org/apache/calcite/sql/validate/SqlConformance.html
 */
public class ImpalaConformance implements SqlConformance {

  public static final SqlConformance INSTANCE = new ImpalaConformance();

  @Override public boolean isLiberal() {
    return true;
  }

  @Override public boolean allowCharLiteralAlias() {
    return true;
  }


  @Override public boolean isGroupByAlias() {
    return true;
  }

  @Override public boolean isGroupByOrdinal() {
    return true;
  }

  @Override public boolean isHavingAlias() {
    return true;
  }

  @Override public boolean isSortByOrdinal() {
    return true;
  }

  @Override public boolean isSortByAlias() {
    return true;
  }

  @Override public boolean isSortByAliasObscures() {
    return false;
  }

  @Override public boolean isFromRequired() {
    return false;
  }

  @Override public boolean splitQuotedTableName() {
    return false;
  }

  @Override public boolean allowHyphenInUnquotedTableName() {
    return false;
  }

  @Override public boolean isBangEqualAllowed() {
    return true;
  }

  @Override public boolean isMinusAllowed() {
    return true;
  }

  @Override public boolean isRegexReplaceCaptureGroupDollarIndexed() {
    return true;
  }

  @Override public boolean isPercentRemainderAllowed() {
    return true;
  }

  @Override public boolean isApplyAllowed() {
    return false;
  }

  @Override public boolean isInsertSubsetColumnsAllowed() {
    return false;
  }

  @Override public boolean allowNiladicParentheses() {
    return true;
  }

  @Override public boolean allowExplicitRowValueConstructor() {
    return false;
  }

  @Override public boolean allowExtend() {
    return false;
  }

  @Override public boolean isLimitStartCountAllowed() {
    return false;
  }

  @Override public boolean isOffsetLimitAllowed() {
    return false;
  }

  @Override public boolean allowGeometry() {
    return false;
  }

  @Override public boolean shouldConvertRaggedUnionTypesToVarying() {
    return false;
  }

  @Override public boolean allowExtendedTrim() {
    return false;
  }

  @Override public boolean allowPluralTimeUnits() {
    return false;
  }

  @Override public boolean allowQualifyingCommonColumn() {
    return true;
  }

  @Override public boolean allowAliasUnnestItems() {
    return false;
  }

  @Override public boolean isValueAllowed() {
    return true;
  }

  @Override public SqlLibrary semantics() {
    return SqlLibrary.STANDARD;
  }

  @Override public boolean allowLenientCoercion() {
      return false;
  }
}
