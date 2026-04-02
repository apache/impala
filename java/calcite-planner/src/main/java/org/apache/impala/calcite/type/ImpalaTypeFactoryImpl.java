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

package org.apache.impala.calcite.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeMappingRule;

import java.util.List;

/**
 * ImpalaTypeFactoryImpl overrides the SqlTypeFactoryImpl Calcite class for
 * situations where Impala rules are different from Calcite rules in how a
 * datatype is picked.
 */
public class ImpalaTypeFactoryImpl extends SqlTypeFactoryImpl {

  public static final RelDataTypeFactory INSTANCE = new ImpalaTypeFactoryImpl();

  public ImpalaTypeFactoryImpl() {
    super(new ImpalaTypeSystemImpl());
  }

  /**
   * The leastRestrictive method is called for the Values RelNode that contain more
   * than one row. The List of types will be called once for each column. The Impala
   * compatible type is called to determine which type should be used for the least
   * restrictive column across all the rows.
   */
  @Override
  public RelDataType leastRestrictive(
      List<RelDataType> types,
      SqlTypeMappingRule mappingRule) {
    for (RelDataType type : types) {
      if (type.getSqlTypeName().equals(SqlTypeName.ROW)) {
        return super.leastRestrictive(types, mappingRule);
      }
    }
    return ImpalaTypeConverter.getCompatibleType(types, this);
  }
}
