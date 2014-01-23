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

import java.util.List;

import com.cloudera.impala.analysis.ColumnType;
import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.HdfsUri;
import com.cloudera.impala.thrift.TAggregateFunction;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TFunction;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * Base class for all functions.
 */
public class Function implements CatalogObject {
  // Enum for how to compare function signatures.
  public enum CompareMode {
    // Two signatures are identical if the number of arguments and their types match
    // exactly and either both signatures are varargs or neither.
    IS_IDENTICAL,

    // Two signatures are indistinguishable if there is no way to tell them apart
    // when matching a particular instantiation. That is, their fixed arguments
    // match exactly and the remaining varargs have the same type.
    // e.g. fn(int, int, int) and fn(int...)
    IS_INDISTINGUISHABLE,

    // X is a subtype of Y if Y.arg[i] can be implicitly cast to
    // X.arg[i]. If X has vargs, the remaining arguments of Y must
    // be implicitly castable to the var arg type.
    // e.g.
    // fn(int, double, string...) is a subtype of fn(tinyint, float, string, string)
    IS_SUBTYPE,
  }

  // User specified function name e.g. "Add"
  private FunctionName name_;

  private final ColumnType retType_;
  // Array of parameter types.  empty array if this function does not have parameters.
  private ColumnType[] argTypes_;

  // If true, this function has variable arguments.
  // TODO: we don't currently support varargs with no fixed types. i.e. fn(...)
  private boolean hasVarArgs_;

  // Absolute path in HDFS for the binary that contains this function.
  // e.g. /udfs/udfs.jar
  private HdfsUri location_;
  private TFunctionBinaryType binaryType_;
  private long catalogVersion_ =  Catalog.INITIAL_CATALOG_VERSION;

  public Function(FunctionName name, ColumnType[] argTypes,
      ColumnType retType, boolean varArgs) {
    this.name_ = name;
    this.hasVarArgs_ = varArgs;
    if (argTypes == null) {
      argTypes_ = new ColumnType[0];
    } else {
      this.argTypes_ = argTypes;
    }
    this.retType_ = retType;
  }

  public Function(FunctionName name, List<ColumnType> args,
      ColumnType retType, boolean varArgs) {
    this(name, (ColumnType[])null, retType, varArgs);
    if (args.size() > 0) {
      argTypes_ = args.toArray(new ColumnType[args.size()]);
    } else {
      argTypes_ = new ColumnType[0];
    }
  }

  public FunctionName getFunctionName() { return name_; }
  public String functionName() { return name_.getFunction(); }
  public String dbName() { return name_.getDb(); }
  public ColumnType getReturnType() { return retType_; }
  public ColumnType[] getArgs() { return argTypes_; }
  // Returns the number of arguments to this function.
  public int getNumArgs() { return argTypes_.length; }
  public HdfsUri getLocation() { return location_; }
  public TFunctionBinaryType getBinaryType() { return binaryType_; }
  public boolean hasVarArgs() { return hasVarArgs_; }
  public ColumnType getVarArgsType() {
    if (!hasVarArgs_) return ColumnType.INVALID;
    Preconditions.checkState(argTypes_.length > 0);
    return argTypes_[argTypes_.length - 1];
  }

  public void setName(FunctionName name) { name_ = name; }
  public void setLocation(HdfsUri loc) { location_ = loc; }
  public void setBinaryType(TFunctionBinaryType type) { binaryType_ = type; }
  public void setHasVarArgs(boolean v) { hasVarArgs_ = v; }

  // Returns a string with the signature in human readable format:
  // FnName(argtype1, argtyp2).  e.g. Add(int, int)
  public String signatureString() {
    StringBuilder sb = new StringBuilder();
    sb.append(name_.getFunction())
      .append("(")
      .append(Joiner.on(", ").join(argTypes_));
    if (hasVarArgs_) sb.append("...");
    sb.append(")");
    return sb.toString();
  }

  // Compares this to 'other' for mode.
  public boolean compare(Function other, CompareMode mode) {
    switch (mode) {
      case IS_IDENTICAL: return isIdentical(other);
      case IS_INDISTINGUISHABLE: return isIndistinguishable(other);
      case IS_SUBTYPE: return isSubtype(other);
      default:
        Preconditions.checkState(false);
        return false;
    }
  }
  /**
   * Returns true if 'this' is a supertype of 'other'. Each argument in other must
   * be implicitly castable to the matching argument in this.
   * TODO: look into how we resolve implicitly castable functions. Is there a rule
   * for "most" compatible or maybe return an error if it is ambiguous?
   */
  private boolean isSubtype(Function other) {
    if (!this.hasVarArgs_ && other.argTypes_.length != this.argTypes_.length) {
      return false;
    }
    if (this.hasVarArgs_ && other.argTypes_.length < this.argTypes_.length) return false;
    for (int i = 0; i < this.argTypes_.length; ++i) {
      if (!ColumnType.isImplicitlyCastable(other.argTypes_[i], this.argTypes_[i])) {
        return false;
      }
    }
    // Check trailing varargs.
    if (this.hasVarArgs_) {
      for (int i = this.argTypes_.length; i < other.argTypes_.length; ++i) {
        if (!ColumnType.isImplicitlyCastable(other.argTypes_[i],
            this.getVarArgsType())) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean isIdentical(Function o) {
    if (o.argTypes_.length != this.argTypes_.length) return false;
    if (o.hasVarArgs_ != this.hasVarArgs_) return false;
    for (int i = 0; i < this.argTypes_.length; ++i) {
      if (!o.argTypes_[i].equals(this.argTypes_[i])) return false;
    }
    return true;
  }

  private boolean isIndistinguishable(Function o) {
    int minArgs = Math.min(o.argTypes_.length, this.argTypes_.length);
    // The first fully specified args must be identical.
    for (int i = 0; i < minArgs; ++i) {
      if (!o.argTypes_[i].equals(this.argTypes_[i])) return false;
    }
    if (o.argTypes_.length == this.argTypes_.length) return true;

    if (o.hasVarArgs_ && this.hasVarArgs_) {
      if (!o.getVarArgsType().equals(this.getVarArgsType())) return false;
      if (this.getNumArgs() > o.getNumArgs()) {
        for (int i = minArgs; i < this.getNumArgs(); ++i) {
          if (!this.argTypes_[i].equals(o.getVarArgsType())) return false;
        }
      } else {
        for (int i = minArgs; i < o.getNumArgs(); ++i) {
          if (!o.argTypes_[i].equals(this.getVarArgsType())) return false;
        }
      }
      return true;
    } else if (o.hasVarArgs_) {
      // o has var args so check the remaining arguments from this
      if (o.getNumArgs() > minArgs) return false;
      for (int i = minArgs; i < this.getNumArgs(); ++i) {
        if (!this.argTypes_[i].equals(o.getVarArgsType())) return false;
      }
      return true;
    } else if (this.hasVarArgs_) {
      // this has var args so check the remaining arguments from s
      if (this.getNumArgs() > minArgs) return false;
      for (int i = minArgs; i < o.getNumArgs(); ++i) {
        if (!o.argTypes_[i].equals(this.getVarArgsType())) return false;
      }
      return true;
    } else {
      // Neither has var args and the lengths don't match
      return false;
    }
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.FUNCTION; }

  @Override
  public long getCatalogVersion() { return catalogVersion_; }

  @Override
  public void setCatalogVersion(long newVersion) { catalogVersion_ = newVersion; }

  @Override
  public String getName() { return getFunctionName().toString(); }

  public TFunction toThrift() {
    TFunction fn = new TFunction();
    // TODO: this function should have a unique ID
    fn.setId(0);
    fn.setSignature(signatureString());
    fn.setName(name_.toThrift());
    fn.setBinary_type(binaryType_);
    if (location_ != null) fn.setHdfs_location(location_.toString());
    fn.setArg_types(ColumnType.toThrift(argTypes_));
    fn.setRet_type(getReturnType().toThrift());
    fn.setHas_var_args(hasVarArgs_);
    // TODO: Comment field is missing?
    // fn.setComment(comment_)
    return fn;
  }

  public static Function fromThrift(TFunction fn) {
    List<ColumnType> argTypes = Lists.newArrayList();
    for (TColumnType t: fn.getArg_types()) {
      argTypes.add(ColumnType.fromThrift(t));
    }

    Function function = null;
    if (fn.isSetScalar_fn()) {
      function = new Udf(FunctionName.fromThrift(fn.getName()), argTypes,
          ColumnType.fromThrift(fn.getRet_type()), new HdfsUri(fn.getHdfs_location()),
          fn.getScalar_fn().getSymbol());
    } else if (fn.isSetAggregate_fn()) {
      TAggregateFunction aggFn = fn.getAggregate_fn();
      function = new Uda(FunctionName.fromThrift(fn.getName()), argTypes,
          ColumnType.fromThrift(fn.getRet_type()),
          ColumnType.fromThrift(aggFn.getIntermediate_type()),
          new HdfsUri(fn.getHdfs_location()), aggFn.getUpdate_fn_symbol(),
          aggFn.getInit_fn_symbol(), aggFn.getSerialize_fn_symbol(),
          aggFn.getMerge_fn_symbol(), aggFn.getFinalize_fn_symbol());
    } else {
      throw new IllegalStateException("Expected function type to be either UDA or UDF.");
    }
    function.setBinaryType(fn.getBinary_type());
    function.setHasVarArgs(fn.isHas_var_args());
    return function;
  }

  @Override
  public boolean isLoaded() { return true; }
}
