package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.opcode.FunctionRegistry;
import com.cloudera.impala.thrift.TExprOpcode;
import com.google.common.collect.Maps;

/**
 * The OpcodeRegistry provides a mapping between function signatures and opcodes. The
 * supported functions are code-gen'ed and added to the registry with an assigned opcode.
 * The opcode is shared with the backend.  The frontend can use the registry to look up
 * a function's opcode.
 *
 * The OpcodeRegistry also contains a mapping between function names (as strings) to
 * operators.
 *
 * The OpcodeRegistry is a singleton.
 *
 * TODO: The opcode registry should be versioned in the FE/BE.
 */
public class OpcodeRegistry {

  private final static Logger LOG = LoggerFactory.getLogger(OpcodeRegistry.class);
  private static OpcodeRegistry instance = new OpcodeRegistry();

  /**
   * This is a mapping of Operator,#args to signatures.  The signature is defined by
   * the operator enum and the arguments and is a one to one mapping to opcodes.
   * The map is structured this way to more efficiently look for signature matches.
   * Signatures that have the same number of arguments have a potential to be matches
   * by allowing types to be implicitly cast.
   */
  private final Map<Pair<FunctionOperator, Integer>, List<Signature>> operations;

  /**
   * This contains a mapping of function names to a FunctionOperator enum.  This is used
   * by FunctionCallExpr to go from the parser input to function opcodes.
   * This is a many to one mapping (i.e. substr and substring both map to the same
   * operation).
   * The mappings are filled in in FunctionRegistry.java which is auto-generated.
   */
  private final HashMap<String, FunctionOperator> functionNameMap;

  // Singleton interface
  public static OpcodeRegistry instance() {
    return instance;
  }

  /**
   * Static utility functions
   */
  public static boolean isBitwiseOperation(FunctionOperator operator) {
    return operator == FunctionOperator.BITAND || operator == FunctionOperator.BITNOT ||
           operator == FunctionOperator.BITOR || operator == FunctionOperator.BITXOR;
  }

  /**
   * Contains all the information about a function signature.
   */
  public static class Signature {
    public TExprOpcode opcode;
    public FunctionOperator operator;
    public PrimitiveType returnType;
    public PrimitiveType argTypes[];

    // Constructor for searching, specifying the op and arguments
    public Signature(FunctionOperator operator, PrimitiveType[] args) {
      this.operator = operator;
      this.argTypes = args;
    }

    private Signature(TExprOpcode opcode, FunctionOperator operator,
        PrimitiveType ret, PrimitiveType[] args) {
      this.operator = operator;
      this.opcode = opcode;
      this.returnType = ret;
      this.argTypes = args;
    }

    /**
     * Returns if the 'this' signature is compatible with the 'other' signature. The op
     * and number of arguments must match and it must be allowed to implicitly cast
     * each argument of this signature to the matching argument in 'other'
     */
    public boolean isCompatible(Signature other) {
      if (other.argTypes.length != this.argTypes.length) {
        return false;
      }
      for (int i = 0; i < this.argTypes.length; ++i) {
        if (!PrimitiveType.isImplicitlyCastable(this.argTypes[i], other.argTypes[i])) {
          return false;
        }
      }
      return true;
    }

    @Override
    /**
     * Signature are equal with C++/Java function signature semantics.  They are
     * equal if the operation and all the arguments are the same.
     */
    public boolean equals(Object o) {
      if (o == null || !(o instanceof Signature)) {
        return false;
      }
      Signature s = (Signature) o;
      if (s.argTypes.length != this.argTypes.length) {
        return false;
      }
      if (s.argTypes == null && this.argTypes == null) {
        return true;
      }

      for (int i = 0; i < this.argTypes.length; ++i) {
        if (s.argTypes[i] != this.argTypes[i]) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Returns the function operator enum.  The lookup is case insensitive.
   * (i.e. "Substring" --> TExprOperator.STRING_SUBSTR).
   * Returns INVALID_OP is that function name is unknown.
   */
  public FunctionOperator getFunctionOperator(String fnName) {
    String lookup = fnName.toLowerCase();
    if (functionNameMap.containsKey(lookup)) {
      return functionNameMap.get(lookup);
    }
    return FunctionOperator.INVALID_OPERATOR;
  }

  /**
   * Query for a function in the registry, specifying the operation, 'op', and the arguments.
   * If there is no matching signature, null will be returned.
   * If there is a match, the matching signature will be returned.  The matching signature does
   * not have to match the input identically, implicit type promotion is allowed.
   */
  public Signature getFunctionInfo(FunctionOperator op, PrimitiveType ... argTypes) {
    Pair<FunctionOperator, Integer> lookup = Pair.create(op, argTypes.length);
    if (operations.containsKey(lookup)) {
      List<Signature> signatures = operations.get(lookup);
      Signature compatibleMatch = null;
      Signature search = new Signature(op, argTypes);
      for (Signature signature : signatures) {
        if (search.equals(signature)) {
          return signature;
        } else if (compatibleMatch == null && search.isCompatible(signature)) {
          compatibleMatch = signature;
        }
      }
      return compatibleMatch;
    }
    return null;
  }

  /**
   * Add a function with the specified opcode/signature to the registry.
   */
  public boolean add(FunctionOperator op, TExprOpcode opcode, PrimitiveType retType, PrimitiveType ... args) {
    List<Signature> signatures;
    Pair<FunctionOperator, Integer> lookup = Pair.create(op, args.length);
    if (operations.containsKey(lookup)) {
      signatures = operations.get(lookup);
    } else {
      signatures = new ArrayList<Signature>();
      operations.put(lookup, signatures);
    }

    Signature signature = new Signature(opcode, op, retType, args);
    if (signatures.contains(signature)) {
      LOG.error("OpcodeRegistry: Function already exists: " + opcode);
      return false;
    }
    signatures.add(signature);

    return true;
  }

  public boolean addFunctionMapping(String functionName, FunctionOperator op) {
    if (functionNameMap.containsKey(functionName)) {
      LOG.error("OpcodeRegistry: Function mapping already exists: " + functionName);
      return false;
    }
    functionNameMap.put(functionName, op);
    return true;
  }

  // Singleton interface, don't call the constructor
  private OpcodeRegistry() {
    operations = Maps.newHashMap();
    functionNameMap = Maps.newHashMap();

    // Add all the function signatures to the registry and the function name(string)
    // to FunctionOperator mapping
    FunctionRegistry.InitFunctions(this);
  }
}
