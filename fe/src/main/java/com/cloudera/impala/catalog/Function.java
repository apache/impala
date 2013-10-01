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

import java.util.ArrayList;

import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.HdfsURI;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;


/**
 * Utility class to describe a function.
 */
public class Function {
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

  private final PrimitiveType retType_;
  // Array of parameter types.  empty array if this function does not have parameters.
  private PrimitiveType[] argTypes_;

  // If true, this function has variable arguments.
  // TODO: we don't currently support varargs with no fixed types. i.e. fn(...)
  private boolean hasVarArgs_;

  // Absolute path in HDFS for the binary that contains this function.
  // e.g. /udfs/udfs.jar
  private HdfsURI location_;

  private TFunctionBinaryType binaryType_;

  public Function(FunctionName name, PrimitiveType[] argTypes,
      PrimitiveType retType, boolean varArgs) {
    this.name_ = name;
    this.hasVarArgs_ = varArgs;
    if (argTypes == null) {
      argTypes_ = new PrimitiveType[0];
    } else {
      this.argTypes_ = argTypes;
    }
    this.retType_ = retType;
  }

  public Function(FunctionName name, ArrayList<PrimitiveType> args,
      PrimitiveType retType, boolean varArgs) {
    this(name, (PrimitiveType[])null, retType, varArgs);
    if (args.size() > 0) {
      argTypes_ = args.toArray(new PrimitiveType[args.size()]);
    } else {
      argTypes_ = new PrimitiveType[0];
    }
  }

  public FunctionName getName() { return name_; }
  public String functionName() { return name_.getFunction(); }
  public String dbName() { return name_.getDb(); }
  public PrimitiveType getReturnType() { return retType_; }
  public PrimitiveType[] getArgs() { return argTypes_; }
  // Returns the number of arguments to this function.
  public int getNumArgs() { return argTypes_.length; }
  public HdfsURI getLocation() { return location_; }
  public TFunctionBinaryType getBinaryType() { return binaryType_; }
  public boolean hasVarArgs() { return hasVarArgs_; }
  public PrimitiveType getVarArgsType() {
    if (!hasVarArgs_) return PrimitiveType.INVALID_TYPE;
    Preconditions.checkState(argTypes_.length > 0);
    return argTypes_[argTypes_.length - 1];
  }

  public void setName(FunctionName name) { name_ = name; }
  public void setLocation(HdfsURI loc) { location_ = loc; }
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
      if (!PrimitiveType.isImplicitlyCastable(other.argTypes_[i], this.argTypes_[i])) {
        return false;
      }
    }
    // Check trailing varargs.
    if (this.hasVarArgs_) {
      for (int i = this.argTypes_.length; i < other.argTypes_.length; ++i) {
        if (!PrimitiveType.isImplicitlyCastable(other.argTypes_[i],
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
      if (o.argTypes_[i] != this.argTypes_[i]) return false;
    }
    return true;
  }

  private boolean isIndistinguishable(Function o) {
    int minArgs = Math.min(o.argTypes_.length, this.argTypes_.length);
    // The first fully specified args must be identical.
    for (int i = 0; i < minArgs; ++i) {
      if (o.argTypes_[i] != this.argTypes_[i]) return false;
    }
    if (o.argTypes_.length == this.argTypes_.length) return true;

    if (o.hasVarArgs_ && this.hasVarArgs_) {
      if (o.getVarArgsType() != this.getVarArgsType()) return false;
      if (this.getNumArgs() > o.getNumArgs()) {
        for (int i = minArgs; i < this.getNumArgs(); ++i) {
          if (this.argTypes_[i] != o.getVarArgsType()) return false;
        }
      } else {
        for (int i = minArgs; i < o.getNumArgs(); ++i) {
          if (o.argTypes_[i] != this.getVarArgsType()) return false;
        }
      }
      return true;
    } else if (o.hasVarArgs_) {
      // o has var args so check the remaining arguments from this
      if (o.getNumArgs() > minArgs) return false;
      for (int i = minArgs; i < this.getNumArgs(); ++i) {
        if (this.argTypes_[i] != o.getVarArgsType()) return false;
      }
      return true;
    } else if (this.hasVarArgs_) {
      // this has var args so check the remaining arguments from s
      if (this.getNumArgs() > minArgs) return false;
      for (int i = minArgs; i < o.getNumArgs(); ++i) {
        if (o.argTypes_[i] != this.getVarArgsType()) return false;
      }
      return true;
    } else {
      // Neither has var args and the lengths don't match
      return false;
    }
  }
}
