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


/**
 * Utility class to describe a function.
 */
public class Function {
  // User specified function name e.g. "Add"
  private FunctionName name_;

  private final PrimitiveType retType_;
  // Array of parameter types.  empty array if this function does not have parameters.
  private PrimitiveType[] argTypes_;
  private final boolean varArgs_;

  // Absolute path in HDFS for the binary that contains this function.
  // e.g. /udfs/udfs.jar
  private HdfsURI location_;

  private TFunctionBinaryType binaryType_;

  public Function(FunctionName name, PrimitiveType[] argTypes,
      PrimitiveType retType, boolean varArgs) {
    this.name_ = name;
    this.varArgs_ = varArgs;
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

  public void setName(FunctionName name) { name_ = name; }
  public void setLocation(HdfsURI loc) { location_ = loc; }
  public void setBinaryType(TFunctionBinaryType type) { binaryType_ = type; }

  // Returns a string with the signature in human readable format:
  // FnName(argtype1, argtyp2).  e.g. Add(int, int)
  public String signatureString() {
    StringBuilder sb = new StringBuilder();
    sb.append(name_.getFunction())
      .append("(")
      .append(Joiner.on(", ").join(argTypes_))
      .append(")");
    return sb.toString();
  }

  /**
   * Returns true if 'this' is a supertype of 'other'. Each argument in other must
   * be implicitly castable to the matching argument in this.
   * TODO: look into how we resolve implicitly castable functions. Is there a rule
    * for "most" compatible or maybe return an error if it is ambiguous?
   */
  public boolean isSupertype(Function other) {
    if (!varArgs_ && other.argTypes_.length != this.argTypes_.length) return false;
    if (varArgs_ && other.argTypes_.length < this.argTypes_.length) return false;
    for (int i = 0; i < this.argTypes_.length; ++i) {
      if (!PrimitiveType.isImplicitlyCastable(other.argTypes_[i], this.argTypes_[i])) {
        return false;
      }
    }
    // Check trailing varargs.
    if (varArgs_) {
      for (int i = this.argTypes_.length; i < other.argTypes_.length; ++i) {
        if (!PrimitiveType.isImplicitlyCastable(other.argTypes_[i],
            this.argTypes_[this.argTypes_.length - 1])) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  /**
    * Signatures are equal with C++/Java function-signature semantics.  They are
    * equal if the operation and all the arguments are the same. The return
    * type is ignored.
    */
  public boolean equals(Object o) {
    if (o == null || !(o instanceof Function)) return false;

    Function s = (Function) o;
    if (s.argTypes_.length != this.argTypes_.length) return false;

    for (int i = 0; i < this.argTypes_.length; ++i) {
      if (s.argTypes_[i] != this.argTypes_[i]) return false;
    }
    return true;
  }
}
