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

package org.apache.impala.catalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang.NotImplementedException;
import org.apache.impala.analysis.FunctionName;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TAggregateFunction;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TFunctionCategory;
import org.apache.impala.thrift.TScalarFunction;
import org.apache.impala.thrift.TSymbolLookupParams;
import org.apache.impala.thrift.TSymbolLookupResult;
import org.apache.impala.thrift.TSymbolType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;


/**
 * Base class for all functions.
 * Each function can be of the following 4 types.
 * - Native/IR stored in db params (persisted, visible to Impala)
 * - Hive UDFs stored in the HMS (visible to Hive + Impala)
 * - Java UDFs which are not persisted (visible to Impala but not Hive)
 * - Builtin functions, which are recreated after every restart of the
 *   catalog. (persisted, visible to Impala)
 */
public class Function extends CatalogObjectImpl {
  // Enum for how to compare function signatures.
  // For decimal types, the type in the function can be a wildcard, i.e. decimal(*,*).
  // The wildcard can *only* exist as function type, the caller will always be a
  // fully specified decimal.
  // For the purposes of function type resolution, decimal(*,*) will match exactly
  // with any fully specified decimal (i.e. fn(decimal(*,*)) matches identically for
  // the call to fn(decimal(1,0)).
  public enum CompareMode {
    // Two signatures are identical if the number of arguments and their types match
    // exactly and either both signatures are varargs or neither.
    IS_IDENTICAL,

    // Two signatures are indistinguishable if there is no way to tell them apart
    // when matching a particular instantiation. That is, their fixed arguments
    // match exactly and the remaining varargs have the same type.
    // e.g. fn(int, int, int) and fn(int...)
    // Argument types that are NULL are ignored when doing this comparison.
    // e.g. fn(NULL, int) is indistinguishable from fn(int, int)
    IS_INDISTINGUISHABLE,

    // X is a supertype of Y if Y.arg[i] can be strictly implicitly cast to X.arg[i]. If
    /// X has vargs, the remaining arguments of Y must be strictly implicitly castable
    // to the var arg type. The key property this provides is that X can be used in place
    // of Y. e.g. fn(int, double, string...) is a supertype of fn(tinyint, float, string,
    // string)
    IS_SUPERTYPE_OF,

    // Nonstrict supertypes broaden the definition of supertype to accept implicit casts
    // of arguments that may result in loss of precision - e.g. decimal to float.
    IS_NONSTRICT_SUPERTYPE_OF,
  }

  // User specified function name e.g. "Add"
  protected final FunctionName name_;

  protected final Type retType_;
  // Array of parameter types.  empty array if this function does not have parameters.
  protected final Type[] argTypes_;

  // If true, this function has variable arguments.
  // TODO: we don't currently support varargs with no fixed types. i.e. fn(...)
  protected boolean hasVarArgs_;

  // If true (default), this function is called directly by the user. For operators,
  // this is false. If false, it also means the function is not visible from
  // 'show functions'.
  private boolean userVisible_;

  // Absolute path in HDFS for the binary that contains this function.
  // e.g. /udfs/udfs.jar
  protected HdfsUri location_;
  protected TFunctionBinaryType binaryType_;

  // Set to true for functions that survive service restarts, including all builtins,
  // native and IR functions, but only Java functions created without a signature.
  private boolean isPersistent_;

  // Functions with specific parameters can be marked as unsupported so that during
  // analysis the query can be rejected without Impala trying to cast parameters to a
  // different type that would be supported.
  protected boolean isUnsupported_;

  public Function(FunctionName name, Type[] argTypes,
      Type retType, boolean varArgs) {
    this.name_ = name;
    this.hasVarArgs_ = varArgs;
    if (argTypes == null) {
      argTypes_ = new Type[0];
    } else {
      this.argTypes_ = argTypes;
    }
    if (retType == null) {
      this.retType_ = ScalarType.INVALID;
    } else {
      this.retType_ = retType;
    }
    this.userVisible_ = true;
    this.isUnsupported_ = false;
  }

  public Function(FunctionName name, List<Type> args,
      Type retType, boolean varArgs) {
    this(name,
        (args != null && args.size() > 0)
          ? args.toArray(new Type[args.size()]) : new Type[0],
        retType, varArgs);
  }

  /**
   * Static helper method to create a function with a given TFunctionBinaryType.
   */
  public static Function createFunction(String db, String fnName, List<Type> args,
      Type retType, boolean varArgs, TFunctionBinaryType fnType) {
    Function fn =
        new Function(new FunctionName(db, fnName), args, retType, varArgs);
    fn.setBinaryType(fnType);
    return fn;
  }

  public FunctionName getFunctionName() { return name_; }
  public String functionName() { return name_.getFunction(); }
  public String dbName() { return name_.getDb(); }
  public Type getReturnType() { return retType_; }
  public Type[] getArgs() { return argTypes_; }
  // Returns the number of arguments to this function.
  public int getNumArgs() { return argTypes_.length; }
  public HdfsUri getLocation() { return location_; }
  public TFunctionBinaryType getBinaryType() { return binaryType_; }
  public boolean hasVarArgs() { return hasVarArgs_; }
  public boolean isPersistent() { return isPersistent_; }
  public boolean userVisible() { return userVisible_; }
  public Type getVarArgsType() {
    if (!hasVarArgs_) return Type.INVALID;
    Preconditions.checkState(argTypes_.length > 0);
    return argTypes_[argTypes_.length - 1];
  }
  public boolean isUnsupported() { return isUnsupported_; }

  public void setLocation(HdfsUri loc) { location_ = loc; }
  public void setBinaryType(TFunctionBinaryType type) { binaryType_ = type; }
  public void setHasVarArgs(boolean v) { hasVarArgs_ = v; }
  public void setIsPersistent(boolean v) { isPersistent_ = v; }
  public void setUserVisible(boolean b) { userVisible_ = b; }
  protected void setUnsupported() { isUnsupported_ = true; }

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

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Function)) return false;
    return compare((Function)o, CompareMode.IS_IDENTICAL);
  }

  @Override
  public int hashCode() {
    // Use a rough hashing based on name to avoid dealing with different comparison modes.
    return Objects.hash(name_);
  }

  // Compares this to 'other' for 'mode'.
  public boolean compare(Function other, CompareMode mode) {
    return calcMatchScore(other, mode) >= 0;
  }

  /* Compares this to 'other' for 'mode' and calculates the matching score.
   * If comparison was not successful, returns -1.
   * Otherwise returns the number of arguments whose types are an exact match or a
   * wildcard variant.
   */
  public int calcMatchScore(Function other, CompareMode mode) {
    switch (mode) {
      case IS_IDENTICAL: return calcIdenticalMatchScore(other);
      case IS_INDISTINGUISHABLE: return calcIndistinguishableMatchScore(other);
      case IS_SUPERTYPE_OF:
        return calcSuperTypeOfMatchScore(other, TypeCompatibility.ALL_STRICT);
      case IS_NONSTRICT_SUPERTYPE_OF:
        return calcSuperTypeOfMatchScore(other, TypeCompatibility.DEFAULT);
      default:
        Preconditions.checkState(false);
        return -1;
    }
  }

  /**
   * If this function has variable arguments and the number of its formal arguments is
   * less than 'length', return an array of 'length' size that contains all the argument
   * types to this function with the last argument type extended over the remaining part
   * of the array.
   * Otherwise, return the original array of argument types.
   */
  private Type[] tryExtendArgTypesToLength(int length) {
    if (!hasVarArgs_ || argTypes_.length >= length) return argTypes_;

    Type[] ret = Arrays.copyOf(argTypes_, length);
    Arrays.fill(ret, argTypes_.length, length, getVarArgsType());
    return ret;
  }

  /**
   * Checks if 'this' is a supertype of 'other'. Each argument in other must be implicitly
   * castable to the matching argument in this. If strict is true, only consider
   * conversions where there is no loss of precision.
   * If 'this' is not a supertype of 'other' returns -1.
   * Otherwise returns the number of arguments whose types are an exact match or a
   * wildcard variant.
   */
  private int calcSuperTypeOfMatchScore(Function other, TypeCompatibility compatibility) {
    if (!other.name_.equals(name_)) return -1;
    if (!this.hasVarArgs_ && other.argTypes_.length != this.argTypes_.length) {
      return -1;
    }
    if (this.hasVarArgs_ && other.argTypes_.length < this.argTypes_.length) return -1;

    Type[] extendedArgTypes = tryExtendArgTypesToLength(other.argTypes_.length);
    int num_matches = 0;
    for (int i = 0; i < extendedArgTypes.length; ++i) {
      if (other.argTypes_[i].matchesType(extendedArgTypes[i])) {
        num_matches++;
        continue;
      }
      if (!Type.isImplicitlyCastable(
              other.argTypes_[i], extendedArgTypes[i], compatibility)) {
        return -1;
      }
    }
    return num_matches;
  }

  /**
   * Converts any CHAR arguments to be STRING arguments
   */
  public Function promoteCharsToStrings() {
    Type[] promoted = argTypes_.clone();
    for (int i = 0; i < promoted.length; ++i) {
      if (promoted[i].isScalarType(PrimitiveType.CHAR)) promoted[i] = ScalarType.STRING;
    }
    return new Function(name_, promoted, retType_, hasVarArgs_);
  }

  private int calcIdenticalMatchScore(Function o) {
    if (!o.name_.equals(name_)) return -1;
    if (o.argTypes_.length != this.argTypes_.length) return -1;
    if (o.hasVarArgs_ != this.hasVarArgs_) return -1;
    for (int i = 0; i < this.argTypes_.length; ++i) {
      if (!o.argTypes_[i].matchesType(this.argTypes_[i])) return -1;
    }
    return this.argTypes_.length;
  }

  private int calcIndistinguishableMatchScore(Function o) {
    if (!o.name_.equals(name_)) return -1;
    int minArgs = Math.min(o.argTypes_.length, this.argTypes_.length);
    // The first fully specified args must be identical.
    int num_matches = 0;
    for (int i = 0; i < minArgs; ++i) {
      if (o.argTypes_[i].isNull() || this.argTypes_[i].isNull()) continue;
      if (!o.argTypes_[i].matchesType(this.argTypes_[i])) return -1;
      num_matches++;
    }
    if (o.argTypes_.length == this.argTypes_.length) return num_matches;

    if (o.hasVarArgs_ && this.hasVarArgs_) {
      if (!o.getVarArgsType().matchesType(this.getVarArgsType())) return -1;
      if (this.getNumArgs() > o.getNumArgs()) {
        for (int i = minArgs; i < this.getNumArgs(); ++i) {
          if (this.argTypes_[i].isNull()) continue;
          if (!this.argTypes_[i].matchesType(o.getVarArgsType())) return -1;
          num_matches++;
        }
      } else {
        for (int i = minArgs; i < o.getNumArgs(); ++i) {
          if (o.argTypes_[i].isNull()) continue;
          if (!o.argTypes_[i].matchesType(this.getVarArgsType())) return -1;
          num_matches++;
        }
      }
      return num_matches;
    } else if (o.hasVarArgs_) {
      // o has var args so check the remaining arguments from this
      if (o.getNumArgs() > minArgs) return -1;
      for (int i = minArgs; i < this.getNumArgs(); ++i) {
        if (this.argTypes_[i].isNull()) continue;
        if (!this.argTypes_[i].matchesType(o.getVarArgsType())) return -1;
        num_matches++;
      }
      return num_matches;
    } else if (this.hasVarArgs_) {
      // this has var args so check the remaining arguments from s
      if (this.getNumArgs() > minArgs) return -1;
      for (int i = minArgs; i < o.getNumArgs(); ++i) {
        if (o.argTypes_[i].isNull()) continue;
        if (!o.argTypes_[i].matchesType(this.getVarArgsType())) return -1;
        num_matches++;
      }
      return num_matches;
    } else {
      // Neither has var args and the lengths don't match
      return -1;
    }
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.FUNCTION; }
  @Override
  public String getName() { return getFunctionName().toString(); }

  // Child classes must override this function.
  // If this class is created directly, it is only as a search key to
  // find a function and is not, itself, a valid function for SQL geneation.
  public String toSql(boolean ifNotExists) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void setTCatalogObject(TCatalogObject catalogObject) {
    catalogObject.setFn(toThrift());
  }

  public TFunction toThrift() {
    TFunction fn = new TFunction();
    fn.setSignature(signatureString());
    fn.setName(name_.toThrift());
    fn.setBinary_type(binaryType_);
    if (location_ != null) fn.setHdfs_location(location_.toString());
    fn.setArg_types(Type.toThrift(argTypes_));
    fn.setRet_type(getReturnType().toThrift());
    fn.setHas_var_args(hasVarArgs_);
    fn.setIs_persistent(isPersistent_);
    // TODO: Comment field is missing?
    // fn.setComment(comment_)
    return fn;
  }

  public static Function fromThrift(TFunction fn) {
    Preconditions.checkArgument(fn.isSetBinary_type());
    Preconditions.checkArgument(fn.isSetArg_types());
    Preconditions.checkArgument(fn.isSetRet_type());
    Preconditions.checkArgument(fn.isSetHas_var_args());
    List<Type> argTypes = new ArrayList<>();
    for (TColumnType t: fn.getArg_types()) {
      argTypes.add(Type.fromThrift(t));
    }

    Function function = null;
    if (fn.isSetScalar_fn()) {
      TScalarFunction scalarFn = fn.getScalar_fn();
      function = new ScalarFunction(FunctionName.fromThrift(fn.getName()), argTypes,
          Type.fromThrift(fn.getRet_type()), new HdfsUri(fn.getHdfs_location()),
          scalarFn.getSymbol(), scalarFn.getPrepare_fn_symbol(),
          scalarFn.getClose_fn_symbol());
    } else if (fn.isSetAggregate_fn()) {
      TAggregateFunction aggFn = fn.getAggregate_fn();
      function = new AggregateFunction(FunctionName.fromThrift(fn.getName()), argTypes,
          Type.fromThrift(fn.getRet_type()),
          Type.fromThrift(aggFn.getIntermediate_type()),
          new HdfsUri(fn.getHdfs_location()), aggFn.getUpdate_fn_symbol(),
          aggFn.getInit_fn_symbol(), aggFn.getSerialize_fn_symbol(),
          aggFn.getMerge_fn_symbol(), aggFn.getGet_value_fn_symbol(),
          null, aggFn.getFinalize_fn_symbol());
    } else {
      // In the case where we are trying to look up the object, we only have the
      // signature.
      function = new Function(FunctionName.fromThrift(fn.getName()),
          argTypes, Type.fromThrift(fn.getRet_type()), fn.isHas_var_args());
    }
    function.setBinaryType(fn.getBinary_type());
    function.setHasVarArgs(fn.isHas_var_args());
    if (fn.isSetIs_persistent()) {
      function.setIsPersistent(fn.isIs_persistent());
    } else {
      function.setIsPersistent(false);
    }
    return function;
  }

  protected final TSymbolLookupParams buildLookupParams(String symbol,
      TSymbolType symbolType, Type retArgType, boolean hasVarArgs, boolean needsRefresh,
      Type... argTypes) {
    TSymbolLookupParams lookup = new TSymbolLookupParams();
    // Builtin functions do not have an external library, they are loaded directly from
    // the running process
    lookup.location =
        binaryType_ != TFunctionBinaryType.BUILTIN ? location_.toString() : "";
    lookup.symbol = symbol;
    lookup.symbol_type = symbolType;
    lookup.fn_binary_type = binaryType_;
    lookup.arg_types = Type.toThrift(argTypes);
    lookup.has_var_args = hasVarArgs;
    lookup.needs_refresh = needsRefresh;
    if (retArgType != null) lookup.setRet_arg_type(retArgType.toThrift());
    return lookup;
  }

  protected TSymbolLookupParams getLookupParams() {
    throw new NotImplementedException(
        "getLookupParams not implemented for " + getClass().getSimpleName());
  }

  // Looks up the last time the function's source file was updated as recorded in its
  // backend lib-cache entry. Returns -1 if a modified time is not applicable.
  // If an error occurs and the mtime cannot be retrieved, an IllegalStateException is
  // thrown.
  public final long getLastModifiedTime() {
    if (!isBuiltinOrJava()) {
      TSymbolLookupParams lookup = Preconditions.checkNotNull(getLookupParams());
      try {
        TSymbolLookupResult result = FeSupport.LookupSymbol(lookup);
        return result.last_modified_time;
      } catch (Exception e) {
        throw new IllegalStateException(
            "Unable to get last modified time for lib file: " + getLocation().toString(),
            e);
      }
    }
    return -1;
  }

  /**
   * Returns true for BUILTINs, and JAVA functions when location is either null or empty.
   *
   * @return boolean
   */
  private boolean isBuiltinOrJava() {
    return getBinaryType() == TFunctionBinaryType.BUILTIN ||
        (getBinaryType() == TFunctionBinaryType.JAVA &&
            (getLocation() == null || getLocation().toString().isEmpty()));
  }

  // Returns the resolved symbol in the binary. The BE will do a lookup of 'symbol'
  // in the binary and try to resolve unmangled names.
  // If this function is expecting a return argument, retArgType is that type. It should
  // be null if this function isn't expecting a return argument.
  public String lookupSymbol(String symbol, TSymbolType symbolType, Type retArgType,
      boolean hasVarArgs, Type... argTypes) throws AnalysisException {
    if (symbol.length() == 0) {
      if (binaryType_ == TFunctionBinaryType.BUILTIN) {
        // We allow empty builtin symbols in order to stage work in the FE before its
        // implemented in the BE
        return symbol;
      }
      throw new AnalysisException("Could not find symbol ''");
    }

    TSymbolLookupParams lookup =
        buildLookupParams(symbol, symbolType, retArgType, hasVarArgs, true, argTypes);

    try {
      TSymbolLookupResult result = FeSupport.LookupSymbol(lookup);
      switch (result.result_code) {
        case SYMBOL_FOUND:
          return result.symbol;
        case BINARY_NOT_FOUND:
          Preconditions.checkState(binaryType_ != TFunctionBinaryType.BUILTIN);
          throw new AnalysisException(
              "Could not load binary: " + location_.getLocation() + "\n" +
              result.error_msg);
        case SYMBOL_NOT_FOUND:
          throw new AnalysisException(result.error_msg);
        default:
          // Should never get here.
          throw new AnalysisException("Internal Error");
      }
    } catch (InternalException e) {
      // Should never get here.
      e.printStackTrace();
      throw new AnalysisException("Could not find symbol: " + symbol, e);
    }
  }

  public String lookupSymbol(String symbol, TSymbolType symbolType)
      throws AnalysisException {
    Preconditions.checkState(
        symbolType == TSymbolType.UDF_PREPARE || symbolType == TSymbolType.UDF_CLOSE);
    return lookupSymbol(symbol, symbolType, null, false);
  }

  public static String getUdfType(Type t) {
    switch (t.getPrimitiveType()) {
    case BOOLEAN:
      return "BooleanVal";
    case TINYINT:
      return "TinyIntVal";
    case SMALLINT:
      return "SmallIntVal";
    case INT:
      return "IntVal";
    case BIGINT:
      return "BigIntVal";
    case FLOAT:
      return "FloatVal";
    case DOUBLE:
      return "DoubleVal";
    case DATE:
      return "DateVal";
    case STRING:
    case VARCHAR:
    case CHAR:
    case FIXED_UDA_INTERMEDIATE:
    case BINARY:
      // These types are marshaled into a StringVal.
      return "StringVal";
    case TIMESTAMP:
      return "TimestampVal";
    case DECIMAL:
      return "DecimalVal";
    default:
      Preconditions.checkState(false, t.toString());
      return "";
    }
  }

  /**
   * Returns true if the given function matches the specified category.
   */
  public static boolean categoryMatch(Function fn, TFunctionCategory category) {
    Preconditions.checkNotNull(category);
    return (category == TFunctionCategory.SCALAR && fn instanceof ScalarFunction)
        || (category == TFunctionCategory.AGGREGATE
            && fn instanceof AggregateFunction
            && ((AggregateFunction)fn).isAggregateFn())
        || (category == TFunctionCategory.ANALYTIC
            && fn instanceof AggregateFunction
            && ((AggregateFunction)fn).isAnalyticFn());
  }
}
