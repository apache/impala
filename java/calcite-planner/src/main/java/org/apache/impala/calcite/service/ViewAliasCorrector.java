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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ViewAliasCorrector fixes an issue that the Calcite compiler has with the way Hive
 * creates the view syntax in a specific situation. Specifically...
 *
 * For the following view:
 * create view functional.sample_view (abc) as
 * select sum(bigint_col) from functional.alltypestiny
 *
 * The view syntax created in the database on a "show view" displays:
 * CREATE VIEW functional.sample_view AS
 * SELECT sample_view.`_c0` abc FROM (SELECT sum(bigint_col)
 * FROM functional.alltypestiny) sample_view
 *
 * Note that when the columns are specified in the view definition (i.e. 'abc'),
 * Impala wraps the sql with "SELECT <col> <alias> FROM ()".  Also note that the `_c0`
 * in the view is a generated name for the 'sum(bigint_col)'.  While the original Impala
 * planner knows what to do, this SQL syntax fails the analysis phase as/is.
 *
 * This class fixes the issue when found. It works in 2 validation phases.  On the first
 * validation pass, the information on the top wrap SQL and the top view SQL are gathered.
 * Eventually, if there is an actual issue, the validation on the first pass fails. The
 * second phase does the correction of the SQL when Calcite calls the "expandSelect" phase
 * which allows the SqlNode to be changed.
 */
public class ViewAliasCorrector {

  // The NOOP phase: If the current validation is not for a view, there is nothing to
  // correct for, so the validator should have its current phase as NOOP.
  public static CurrentPhase NOOP = new NoopPhase();

  /**
   * Top level for current phase of the view alias corrector.
   */
  public abstract static class CurrentPhase {

    // Current level in the select stack. There could be multiple selects. The view
    // issue only happens at the top level of the select.
    protected int selectStackCounter_ = 0;

    protected boolean validationFinished_ = false;

    public void enterSelect(SqlSelect select) {
      selectStackCounter_++;
      processSelectImpl(select);
    }

    public void exitSelect() {
      selectStackCounter_--;
    }

    public void validateFinished() {
      validationFinished_ = true;
    }

    abstract public void processSelectImpl(SqlSelect select);

    abstract public SqlNode processSelectItem(SqlNode expr);
  }

  /**
   * The gather info of the view corrector. This phase goes through the select items
   * and finds the ones that have the view alias issue.
   */
  public static class ViewGatherAliases extends CurrentPhase {

    private List<SqlNode> topLevelSqlNodes_;

    private List<SqlNode> secondLevelSqlNodes_;

    @Override
    public void processSelectImpl(SqlSelect select) {
      if (selectStackCounter_ == 1) {
        Preconditions.checkState(topLevelSqlNodes_ == null);
        topLevelSqlNodes_ = select.getSelectList();
      }
      if (selectStackCounter_ == 2) {
        Preconditions.checkState(secondLevelSqlNodes_ == null);
        secondLevelSqlNodes_ = select.getSelectList();
      }
    }

    @Override
    public SqlNode processSelectItem(SqlNode expr) {
      // nothing to process in gather phase.
      return expr;
    }

    public Set<Integer> getItemsWithAliasIssue() {
      // Should not be called until validation is complete.
      Preconditions.checkState(validationFinished_);

      // Check if any information was gathered.
      if (topLevelSqlNodes_ == null || secondLevelSqlNodes_ == null) {
        return Collections.emptySet();
      }

      // The alias issue only happens when the columns are all selected out (see top
      // level comment), so if the sizes are different, there is no issue.
      if (topLevelSqlNodes_.size() != secondLevelSqlNodes_.size()) {
        return Collections.emptySet();
      }

      Set<Integer> itemsWithAliasIssue = new HashSet<>();
      for (int i = 0; i < topLevelSqlNodes_.size(); ++i) {
        // compare the aliases of the first and second level to see if they match.
        if (itemHasAliasIssue(topLevelSqlNodes_.get(i), secondLevelSqlNodes_.get(i))) {
          itemsWithAliasIssue.add(i);
        }
      }
      return itemsWithAliasIssue;
    }

    private boolean itemHasAliasIssue(SqlNode topLevelItem, SqlNode secondLevelItem) {
      SqlBasicCall call = (SqlBasicCall) topLevelItem;
      SqlIdentifier topLevelIdentifier = (SqlIdentifier) call.getOperandList().get(0);
      return topLevelIdentifier.names.size() > 1 &&
          !topLevelIdentifier.names.get(1).equals(
              SqlValidatorUtil.alias(secondLevelItem));
    }
  }

  /**
   * The correction phase of the view alias issue. Processes the select
   * and corrects the aliases so they match.
   */
  public static class ViewAttemptAliasCorrection extends CurrentPhase {
    private final Set<Integer> itemsWithAliasIssue_;

    private int secondLevelSelectItemCounter_ = 0;

    public ViewAttemptAliasCorrection(ViewGatherAliases firstPhase) {
      this.itemsWithAliasIssue_ = firstPhase.getItemsWithAliasIssue();
    }

    @Override
    public void processSelectImpl(SqlSelect select) {
      // The correction only needs to happen on the top level.
      if (selectStackCounter_ != 1) {
        return;
      }

      List<SqlNode> fixedSelectList = new ArrayList<>();
      for (int i = 0; i < select.getSelectList().size(); ++i) {
        SqlBasicCall call = (SqlBasicCall) select.getSelectList().get(i);
        // Just add the select item if there is no issue.
        if (!itemsWithAliasIssue_.contains(i)) {
          fixedSelectList.add(call);
          continue;
        }
        SqlIdentifier identifier = (SqlIdentifier) call.getOperandList().get(0);
        // Calcite uses "EXPR$" as its generic alias.  This is all internal, so
        // the naming does not matter. The "i" portion in "EXPR$<i>" matches the
        // position number of the field in the second level select. The resulting
        // selectItem will be: "EXPR$<i> AS <alias name defined in top level view>"
        SqlIdentifier newIdentifier =
            identifier.setName(1, SqlUtil.GENERATED_EXPR_ALIAS_PREFIX + i);
        SqlNode originalAlias = call.getOperandList().get(1);
        SqlNode asNode =
            SqlStdOperatorTable.AS.createCall(
                newIdentifier.getParserPosition(),
                newIdentifier,
                originalAlias);
        fixedSelectList.add(asNode);
      }
      select.setSelectList(new SqlNodeList(fixedSelectList, SqlParserPos.ZERO));
    }

    @Override
    public SqlNode processSelectItem(SqlNode sqlNode) {
      SqlNode returnNode = sqlNode;
      if (selectStackCounter_ != 2) {
        return returnNode;
      }

      if (itemsWithAliasIssue_.contains(secondLevelSelectItemCounter_)) {
        String alias =
            SqlUtil.GENERATED_EXPR_ALIAS_PREFIX + secondLevelSelectItemCounter_;
        returnNode = SqlStdOperatorTable.AS.createCall(
            sqlNode.getParserPosition(),
            sqlNode,
            new SqlIdentifier(alias, SqlParserPos.ZERO));
      }

      secondLevelSelectItemCounter_++;
      return returnNode;
    }

    public boolean hasAliasIssue() {
      return !itemsWithAliasIssue_.isEmpty();
    }
  }

  private static class NoopPhase extends CurrentPhase {
    @Override
    public void processSelectImpl(SqlSelect select) {}

    @Override
    public SqlNode processSelectItem(SqlNode sqlNode) {
      return sqlNode;
    }
  }
}
