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

package org.apache.impala.calcite.service;

import com.google.common.base.Preconditions;

import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorScope.Resolve;
import org.apache.calcite.sql.validate.SqlValidatorScope.ResolvedImpl;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.calcite.schema.CalciteTable;
import org.apache.impala.calcite.schema.ImpalaViewTable;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.FeFsTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ImpalaSqlValidatorImpl is responsible for registering column-level and
 * function-level privilege requests in the given query. The methods in the class will be
 * invoked via SqlValidatorImpl#validate(SqlNode topNode).
 */
public class ImpalaSqlValidatorImpl extends SqlValidatorImpl {

  private Analyzer analyzer_;

  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaSqlValidatorImpl.class.getName());

  public ImpalaSqlValidatorImpl(SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory,
      SqlValidator.Config config, Analyzer analyzer) {
    super(opTab, catalogReader, typeFactory, config);
    analyzer_ = analyzer;
  }

  @Override public void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope) {
    super.validateIdentifier(id, scope);
    // It is a bit inefficient to execute scope.fullyQualify() again because this has
    // been done within super.validateIdentifier(id, scope). But since we need 'fqId'
    // to register the privilege request, we have to do this for now. We created
    // CALCITE-7152 to keep track of this.
    SqlQualified fqId = scope.fullyQualify(id);

    SqlIdentifier prefix = id.getComponent(0, 1);
    SqlNameMatcher nameMatcher = this.getCatalogReader().nameMatcher();
    ResolvedImpl resolved = new ResolvedImpl();
    SqlValidatorNamespace fromNs;
    scope.resolve(prefix.names, nameMatcher, false, resolved);
    if (resolved.count() == 1) {
      Resolve resolve = resolved.only();
      fromNs = resolve.namespace;
      SqlValidatorTable validatorTable = fromNs.getTable();

      if (validatorTable instanceof CalciteTable) {
        FeFsTable feFsTable = ((CalciteTable) validatorTable).getFeFsTable();
        this.analyzer_.registerPrivReq(
            builder -> builder.allOf(Privilege.SELECT)
                .onColumn(feFsTable.getDb().getName(),
                    feFsTable.getTableName().getTbl(), fqId.identifier.names.get(1),
                    feFsTable.getOwnerUser()).build()
        );
        return;
      }

      // A view would be an instance of RelOptTableImpl if the view was created via
      // CalciteDb.Builder#createViewTable().
      if (validatorTable instanceof RelOptTableImpl) {
        Preconditions.checkState(validatorTable.table() instanceof ImpalaViewTable);
        FeView view = ((ImpalaViewTable) validatorTable.table()).getFeView();
        this.analyzer_.registerPrivReq(
            builder -> builder.allOf(Privilege.SELECT)
                .onColumn(view.getDb().getName(),
                    view.getTableName().getTbl(), fqId.identifier.names.get(1),
                    view.getOwnerUser()).build()
        );
      }
    }
  }

  @Override public void validateCall(
      SqlCall call,
      SqlValidatorScope scope) {
    super.validateCall(call, scope);

    SqlOperator operator = call.getOperator();
    // It's a bit hacky to assume each SqlFunction is associated with BuiltinsDb. Ideally
    // a function could be associated with any database. IMPALA-13095 was created to keep
    // track of this.
    if (operator instanceof SqlFunction) {
      this.analyzer_.registerPrivReq(
          builder -> builder.allOf(Privilege.VIEW_METADATA)
              .onDb(BuiltinsDb.getInstance().getName(), null).build());
    }
  }
}
