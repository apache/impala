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


/**
 * Utility class to describe a function.
 */
public class Function {
  // User specified function name e.g. "Add"
  private String name_;

  private final PrimitiveType retType_;
  // Array of parameter types.  null if this function does not have parameters.
  private PrimitiveType[] argTypes_;
  private final boolean varArgs_;

  public Function(String name, PrimitiveType[] argTypes,
      PrimitiveType retType, boolean varArgs) {
    this.name_ = name.toLowerCase();
    this.varArgs_ = varArgs;
    this.argTypes_ = (argTypes != null && argTypes.length > 0) ? argTypes : null;
    this.retType_ = retType;
  }

  public Function(String name, ArrayList<PrimitiveType> args,
      PrimitiveType retType, boolean varArgs) {
    this(name, (PrimitiveType[])null, retType, varArgs);
    if (args.size() > 0) {
      argTypes_ = args.toArray(new PrimitiveType[args.size()]);
    }
  }

  public String getName() { return name_; }
  public PrimitiveType getReturnType() { return retType_; }
  public PrimitiveType[] getArgs() { return argTypes_; }
  // Returns the number of arguments to this function.
  public int getNumArgs() { return argTypes_ == null ? 0 : argTypes_.length; }
  public void setName(String name) { name_ = name; }

  // Returns a string with the signature in human readable format:
  // FnName(argtype1, argtyp2).  e.g. Add(int, int)
  public String signatureString() {
    StringBuilder sb = new StringBuilder();
    sb.append(name_);
    sb.append("(");
    for (int i = 0; argTypes_ != null && i <argTypes_.length; ++i) {
      sb.append(argTypes_[i]);
      if (i != argTypes_.length - 1) sb.append(", ");
    }
    sb.append(")");
    return sb.toString();
  }

  /**
    * Returns true if the 'this' signature is compatible with the 'other' signature. The
    * number of arguments must match and it must be allowed to implicitly cast
    * each argument of this signature to the matching argument in 'other'.
    * TODO: look into how we resolve implicitly castable functions. Is there a rule
    * for "most" compatible or maybe return an error if it is ambiguous?
    */
  public boolean isCompatible(Function other) {
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
    if (s.argTypes_ == null && this.argTypes_ == null) return true;
    if (s.argTypes_ == null || this.argTypes_ == null) return false;
    if (s.argTypes_.length != this.argTypes_.length) return false;

    for (int i = 0; i < this.argTypes_.length; ++i) {
      if (s.argTypes_[i] != this.argTypes_[i]) return false;
    }
    return true;
  }
}
