package com.cloudera.impala.analysis;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;

import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;

/**
 * Class to represent fully qualified function names. Names are similar to java
 * fully qualified class names e.g. <scope>.<scope>.<name>.
 * The identifier must be alphanumeric and the first character of each part of the
 * path must not be a digit.
 */
public class FunctionName {
  // Components of the eventual function path
  // e.g. {'cloudera', 'udf', 'MyUdf' }
  private final ArrayList<String> scopedNameList_;
  private String fullyQualifiedName_;

  public FunctionName(ArrayList<String> scopedNameList) {
    this.scopedNameList_ = scopedNameList;
    Preconditions.checkNotNull(scopedNameList);
    Preconditions.checkState(!scopedNameList_.isEmpty());
  }

  String getName() { return fullyQualifiedName_; }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < scopedNameList_.size(); ++i) {
      String s = scopedNameList_.get(i);
      if (s.length() == 0) {
        throw new AnalysisException("Empty path not allowed.");
      }
      if (!StringUtils.isAlphanumeric(s)) {
        throw new AnalysisException(
            "Function names must be all alphanumeric. Invalid name: " + s);
      }
      if (Character.isDigit(s.charAt(0))) {
        throw new AnalysisException("Function cannot start with a digit: " + s);
      }

      sb.append(s);
      if (i != scopedNameList_.size() - 1) sb.append('.');
    }
    fullyQualifiedName_ = sb.toString();
  }
}
