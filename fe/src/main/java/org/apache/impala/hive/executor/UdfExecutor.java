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

package org.apache.impala.hive.executor;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.thrift.THiveUdfExecutorCtorParams;
import org.apache.impala.thrift.TPrimitiveType;
import org.apache.impala.util.UnsafeUtil;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

// Wrapper object to run hive UDFs. This class works with UdfCallExpr in the
// backend to marshall data back and forth between the execution engine and
// the java UDF class.
// See the comments in be/src/exprs/hive-udf-call.h for more details.
// TODO: should we cache loaded jars and classes?
@SuppressWarnings("restriction")
public class UdfExecutor {
  private static final Logger LOG = Logger.getLogger(UdfExecutor.class);
  // By convention, the function in the class must be called evaluate()
  public static final String UDF_FUNCTION_NAME = "evaluate";

  // Object to deserialize ctor params from BE.
  private final static TBinaryProtocol.Factory PROTOCOL_FACTORY =
    new TBinaryProtocol.Factory();

  // TODO UDF is deprecated in Hive and newer implementation of built-in functions using
  // GenericUDF interface, we should consider supporting GenericUDFs in the future
  private UDF udf_;
  // setup by init() and cleared by close()
  private Method method_;
  // setup by init() and cleared by close()
  private URLClassLoader classLoader_;

  // Return and argument types of the function inferred from the udf method signature.
  // The JavaUdfDataType enum maps it to corresponding primitive type.
  private JavaUdfDataType[] argTypes_;
  private JavaUdfDataType retType_;

  // Input buffer from the backend. This is valid for the duration of an evaluate() call.
  // These buffers are allocated in the BE.
  private final long inputBufferPtr_;
  private final long inputNullsPtr_;

  // This is the byte offset in inputBufferPtr to the start of the input argument.
  // e.g. *inputBufferPtr_[inputBufferOffsets[i]] is the ith input argument.
  private final int[] inputBufferOffsets_;

  // Output buffer to return non-string values. This buffers are allocated in the BE.
  private final long outputBufferPtr_;
  private final long outputNullPtr_;

  // For StringValue return types, outputBufferPtr_ is the location of the 16-byte
  // StringValue object. StringValue.ptr is set to outBufferStringPtr_. This buffer
  // grows as necessary to fit the return string.
  // This is allocated from the FE.
  private long outBufferStringPtr_;

  // Size of outBufferStringPtr_.
  private int outBufferCapacity_;

  // Preconstructed input objects for the UDF. This minimizes object creation overhead
  // as these objects are reused across calls to evaluate().
  private Object[] inputObjects_;
  private Object[] inputArgs_; // inputArgs_[i] is either inputObjects_[i] or null

  // Data types that are supported as return or argument types in Java UDFs.
  public enum JavaUdfDataType {
    INVALID_TYPE("INVALID_TYPE", TPrimitiveType.INVALID_TYPE),
    BOOLEAN("BOOLEAN", TPrimitiveType.BOOLEAN),
    BOOLEAN_WRITABLE("BOOLEAN_WRITABLE", TPrimitiveType.BOOLEAN),
    TINYINT("TINYINT", TPrimitiveType.TINYINT),
    BYTE_WRITABLE("BYTE_WRITABLE", TPrimitiveType.TINYINT),
    SMALLINT("SMALLINT", TPrimitiveType.SMALLINT),
    SHORT_WRITABLE("SHORT_WRITABLE", TPrimitiveType.SMALLINT),
    INT("INT", TPrimitiveType.INT),
    INT_WRITABLE("INT_WRITABLE", TPrimitiveType.INT),
    BIGINT("BIGINT", TPrimitiveType.BIGINT),
    LONG_WRITABLE("LONG_WRITABLE", TPrimitiveType.BIGINT),
    FLOAT("FLOAT", TPrimitiveType.FLOAT),
    FLOAT_WRITABLE("FLOAT_WRITABLE", TPrimitiveType.FLOAT),
    DOUBLE("DOUBLE", TPrimitiveType.DOUBLE),
    DOUBLE_WRITABLE("DOUBLE", TPrimitiveType.DOUBLE),
    STRING("STRING", TPrimitiveType.STRING),
    TEXT("TEXT", TPrimitiveType.STRING),
    BYTES_WRITABLE("BYTES_WRITABLE", TPrimitiveType.STRING),
    BYTE_ARRAY("BYTE_ARRAY", TPrimitiveType.STRING);

    private final String description_;
    private final TPrimitiveType thriftType_;

    private JavaUdfDataType(String description, TPrimitiveType thriftType) {
      description_ = description;
      thriftType_ = thriftType;
    }

    @Override
    public String toString() { return description_; }

    public TPrimitiveType getPrimitiveType() { return thriftType_; }

    public static JavaUdfDataType getType(Class<?> c) {
      if (c == BooleanWritable.class) {
        return JavaUdfDataType.BOOLEAN_WRITABLE;
      } else if (c == boolean.class || c == Boolean.class) {
        return JavaUdfDataType.BOOLEAN;
      } else if (c == ByteWritable.class) {
        return JavaUdfDataType.BYTE_WRITABLE;
      } else if (c == byte.class || c == Byte.class) {
        return JavaUdfDataType.TINYINT;
      } else if (c == ShortWritable.class) {
        return JavaUdfDataType.SHORT_WRITABLE;
      } else if (c == short.class || c == Short.class) {
        return JavaUdfDataType.SMALLINT;
      } else if (c == IntWritable.class) {
        return JavaUdfDataType.INT_WRITABLE;
      } else if (c == int.class || c == Integer.class) {
        return JavaUdfDataType.INT;
      } else if (c == LongWritable.class) {
        return JavaUdfDataType.LONG_WRITABLE;
      } else if (c == long.class || c == Long.class) {
        return JavaUdfDataType.BIGINT;
      } else if (c == FloatWritable.class) {
        return JavaUdfDataType.FLOAT_WRITABLE;
      } else if (c == float.class || c == Float.class) {
        return JavaUdfDataType.FLOAT;
      } else if (c == DoubleWritable.class) {
        return JavaUdfDataType.DOUBLE_WRITABLE;
      } else if (c == double.class || c == Double.class) {
        return JavaUdfDataType.DOUBLE;
      } else if (c == byte[].class) {
        return JavaUdfDataType.BYTE_ARRAY;
      } else if (c == BytesWritable.class) {
        return JavaUdfDataType.BYTES_WRITABLE;
      } else if (c == Text.class) {
        return JavaUdfDataType.TEXT;
      } else if (c == String.class) {
        return JavaUdfDataType.STRING;
      }
      return JavaUdfDataType.INVALID_TYPE;
    }

    public static boolean isSupported(Type t) {
      for(JavaUdfDataType javaType: JavaUdfDataType.values()) {
        if (javaType == JavaUdfDataType.INVALID_TYPE) continue;
        if (javaType.getPrimitiveType() == t.getPrimitiveType().toThrift()) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Create a UdfExecutor, using parameters from a serialized thrift object. Used by
   * the backend.
   */
  public UdfExecutor(byte[] thriftParams) throws ImpalaException {
    THiveUdfExecutorCtorParams request = new THiveUdfExecutorCtorParams();
    JniUtil.deserializeThrift(PROTOCOL_FACTORY, request, thriftParams);

    String className = request.fn.scalar_fn.symbol;
    String jarFile = request.local_location;
    Type retType = Type.fromThrift(request.fn.ret_type);
    Type[] parameterTypes = new Type[request.fn.arg_types.size()];
    for (int i = 0; i < request.fn.arg_types.size(); ++i) {
      parameterTypes[i] = Type.fromThrift(request.fn.arg_types.get(i));
    }
    inputBufferPtr_ = request.input_buffer_ptr;
    inputNullsPtr_ = request.input_nulls_ptr;
    outputBufferPtr_ = request.output_buffer_ptr;
    outputNullPtr_ = request.output_null_ptr;
    outBufferStringPtr_ = 0;
    outBufferCapacity_ = 0;
    inputBufferOffsets_ = new int[request.input_byte_offsets.size()];
    for (int i = 0; i < request.input_byte_offsets.size(); ++i) {
      inputBufferOffsets_[i] = request.input_byte_offsets.get(i).intValue();
    }

    init(jarFile, className, retType, parameterTypes);
  }

  @Override
  protected void finalize() throws Throwable {
    close();
    super.finalize();
  }

  /**
   * Releases any resources allocated off the native heap and close the class
   * loader we may have created.
   */
  public void close() {
    UnsafeUtil.UNSAFE.freeMemory(outBufferStringPtr_);
    outBufferStringPtr_ = 0;
    outBufferCapacity_ = 0;

    if (classLoader_ != null) {
      try {
        classLoader_.close();
      } catch (IOException e) {
        // Log and ignore.
        LOG.debug("Error closing the URLClassloader.", e);
      }
    }
    // We are now un-usable (because the class loader has been
    // closed), so null out method_ and classLoader_.
    method_ = null;
    classLoader_ = null;
  }

  /**
   * evaluate function called by the backend. The inputs to the UDF have
   * been serialized to 'input'
   */
  public void evaluate() throws ImpalaRuntimeException {
    try {
      for (int i = 0; i < argTypes_.length; ++i) {
        if (UnsafeUtil.UNSAFE.getByte(inputNullsPtr_ + i) == 0) {
          switch (argTypes_[i]) {
            case BOOLEAN_WRITABLE:
            case BYTE_WRITABLE:
            case SHORT_WRITABLE:
            case INT_WRITABLE:
            case LONG_WRITABLE:
            case FLOAT_WRITABLE:
            case DOUBLE_WRITABLE:
            case BYTE_ARRAY:
            case BYTES_WRITABLE:
            case TEXT:
              inputArgs_[i] = inputObjects_[i];
              break;
            case BOOLEAN:
              inputArgs_[i] = ((ImpalaBooleanWritable)inputObjects_[i]).get();
              break;
            case TINYINT:
              inputArgs_[i] = ((ImpalaTinyIntWritable)inputObjects_[i]).get();
              break;
            case SMALLINT:
              inputArgs_[i] = ((ImpalaSmallIntWritable)inputObjects_[i]).get();
              break;
            case INT:
              inputArgs_[i] = ((ImpalaIntWritable)inputObjects_[i]).get();
              break;
            case BIGINT:
              inputArgs_[i] = ((ImpalaBigIntWritable)inputObjects_[i]).get();
              break;
            case FLOAT:
              inputArgs_[i] = ((ImpalaFloatWritable)inputObjects_[i]).get();
              break;
            case DOUBLE:
              inputArgs_[i] = ((ImpalaDoubleWritable)inputObjects_[i]).get();
              break;
            case STRING:
              Preconditions.checkState(inputObjects_[i] instanceof ImpalaBytesWritable);
              inputArgs_[i] =
                  new String(((ImpalaBytesWritable)inputObjects_[i]).getBytes());
              break;
          }
        } else {
          inputArgs_[i] = null;
        }
      }
      evaluate(inputArgs_);
    } catch (Exception e) {
      e.printStackTrace(System.err);
      throw new ImpalaRuntimeException("UDF::evaluate() ran into a problem.", e);
    }
  }

  /**
   * Evalutes the UDF with 'args' as the input to the UDF. This is exposed
   * for testing and not the version of evaluate() the backend uses.
   */
  public long evaluateForTesting(Object... args) throws ImpalaRuntimeException {
    try {
      Object[] inputArgs = new Object[args.length];
      for (int i = 0; i < args.length; ++i) {
        switch (argTypes_[i]) {
          case BOOLEAN_WRITABLE:
          case BYTE_WRITABLE:
          case SHORT_WRITABLE:
          case INT_WRITABLE:
          case LONG_WRITABLE:
          case FLOAT_WRITABLE:
          case DOUBLE_WRITABLE:
          case TEXT:
          case BYTE_ARRAY:
          case BYTES_WRITABLE:
          case STRING:
            inputArgs[i] = args[i];
            break;
          case BOOLEAN:
            inputArgs[i] = ((ImpalaBooleanWritable)args[i]).get();
            break;
          case TINYINT:
            inputArgs[i] = ((ImpalaTinyIntWritable)args[i]).get();
            break;
          case SMALLINT:
            inputArgs[i] = ((ImpalaSmallIntWritable)args[i]).get();
            break;
          case INT:
            inputArgs[i] = ((ImpalaIntWritable)args[i]).get();
            break;
          case BIGINT:
            inputArgs[i] = ((ImpalaBigIntWritable)args[i]).get();
            break;
          case FLOAT:
            inputArgs[i] = ((ImpalaFloatWritable)args[i]).get();
            break;
          case DOUBLE:
            inputArgs[i] = ((ImpalaDoubleWritable)args[i]).get();
            break;
        }
      }
      return evaluate(inputArgs);
    } catch (Exception e) {
      e.printStackTrace(System.err);
      throw new ImpalaRuntimeException("UDF::evaluate() ran into a problem.", e);
    }
  }

  /**
   * Evalutes the UDF with 'args' as the input to the UDF.
   * Returns 0 if the udf returned NULL. (the result is a ptr so this is okay).
   */
  private long evaluate(Object... args) throws ImpalaRuntimeException {
    try {
      storeUdfResult(method_.invoke(udf_, args));
      if (UnsafeUtil.UNSAFE.getByte(outputNullPtr_) == 1) return 0;
      return outputBufferPtr_;
    } catch (IllegalArgumentException e) {
      throw new ImpalaRuntimeException("UDF failed to evaluate", e);
    } catch (IllegalAccessException e) {
      throw new ImpalaRuntimeException("UDF failed to evaluate", e);
    } catch (InvocationTargetException e) {
      throw new ImpalaRuntimeException("UDF failed to evaluate", e);
    }
  }

  public Method getMethod() { return method_; }

  // Sets the result object 'obj' into the outputBufferPtr_
  private void storeUdfResult(Object obj) throws ImpalaRuntimeException {
    if (obj == null) {
      UnsafeUtil.UNSAFE.putByte(outputNullPtr_, (byte)1);
      return;
    }

    UnsafeUtil.UNSAFE.putByte(outputNullPtr_, (byte)0);
    switch (retType_) {
      case BOOLEAN_WRITABLE: {
        BooleanWritable val = (BooleanWritable)obj;
        UnsafeUtil.UNSAFE.putByte(outputBufferPtr_, val.get() ? (byte)1 : 0);
        return;
      }
      case BOOLEAN: {
        UnsafeUtil.UNSAFE.putByte(outputBufferPtr_, (boolean)obj ? (byte)1 : 0);
        return;
      }
      case BYTE_WRITABLE: {
        ByteWritable val = (ByteWritable)obj;
        UnsafeUtil.UNSAFE.putByte(outputBufferPtr_, val.get());
        return;
      }
      case TINYINT: {
        UnsafeUtil.UNSAFE.putByte(outputBufferPtr_, (byte)obj);
        return;
      }
      case SHORT_WRITABLE: {
        ShortWritable val = (ShortWritable)obj;
        UnsafeUtil.UNSAFE.putShort(outputBufferPtr_, val.get());
        return;
      }
      case SMALLINT: {
        UnsafeUtil.UNSAFE.putShort(outputBufferPtr_, (short)obj);
        return;
      }
      case INT_WRITABLE: {
        IntWritable val = (IntWritable)obj;
        UnsafeUtil.UNSAFE.putInt(outputBufferPtr_, val.get());
        return;
      }
      case INT: {
        UnsafeUtil.UNSAFE.putInt(outputBufferPtr_, (int)obj);
        return;
      }
      case LONG_WRITABLE: {
        LongWritable val = (LongWritable)obj;
        UnsafeUtil.UNSAFE.putLong(outputBufferPtr_, val.get());
        return;
      }
      case BIGINT: {
        UnsafeUtil.UNSAFE.putLong(outputBufferPtr_, (long)obj);
        return;
      }
      case FLOAT_WRITABLE: {
        FloatWritable val = (FloatWritable)obj;
        UnsafeUtil.UNSAFE.putFloat(outputBufferPtr_, val.get());
        return;
      }
      case FLOAT: {
        UnsafeUtil.UNSAFE.putFloat(outputBufferPtr_, (float)obj);
        return;
      }
      case DOUBLE_WRITABLE: {
        DoubleWritable val = (DoubleWritable)obj;
        UnsafeUtil.UNSAFE.putDouble(outputBufferPtr_, val.get());
        return;
      }
      case DOUBLE: {
        UnsafeUtil.UNSAFE.putDouble(outputBufferPtr_, (double)obj);
        return;
      }
      case TEXT: {
        copyBytesToOutputBuffer(((Text)obj).copyBytes());
        return;
      }
      case BYTE_ARRAY: {
        copyBytesToOutputBuffer((byte[]) obj);
        return;
      }
      case BYTES_WRITABLE: {
        copyBytesToOutputBuffer(((BytesWritable)obj).copyBytes());
        return;
      }
      case STRING: {
        copyBytesToOutputBuffer(((String)obj).getBytes());
        return;
      }
      default:
        throw new ImpalaRuntimeException("Unsupported return type: " + retType_);
    }
  }

  private void copyBytesToOutputBuffer(byte[] bytes) {
    if (bytes.length > outBufferCapacity_) {
      outBufferStringPtr_ =
          UnsafeUtil.UNSAFE.reallocateMemory(outBufferStringPtr_, bytes.length);
      outBufferCapacity_ = bytes.length;
      UnsafeUtil.UNSAFE.putLong(outputBufferPtr_, outBufferStringPtr_);
    }
    UnsafeUtil.Copy(outBufferStringPtr_, bytes, 0, bytes.length);
    UnsafeUtil.UNSAFE.putInt(
        outputBufferPtr_ + ImpalaStringWritable.STRING_VALUE_LEN_OFFSET,
        bytes.length);
  }

  // Preallocate the input objects that will be passed to the underlying UDF.
  // These objects are allocated once and reused across calls to evaluate()
  private void allocateInputObjects() throws ImpalaRuntimeException {
    inputObjects_ = new Writable[argTypes_.length];
    inputArgs_ = new Object[argTypes_.length];

    for (int i = 0; i < argTypes_.length; ++i) {
      int offset = inputBufferOffsets_[i];
      switch (argTypes_[i]) {
        case BOOLEAN:
        case BOOLEAN_WRITABLE:
          inputObjects_[i] = new ImpalaBooleanWritable(inputBufferPtr_ + offset);
          break;
        case TINYINT:
        case BYTE_WRITABLE:
          inputObjects_[i] = new ImpalaTinyIntWritable(inputBufferPtr_ + offset);
          break;
        case SMALLINT:
        case SHORT_WRITABLE:
          inputObjects_[i] = new ImpalaSmallIntWritable(inputBufferPtr_ + offset);
          break;
        case INT:
        case INT_WRITABLE:
          inputObjects_[i] = new ImpalaIntWritable(inputBufferPtr_ + offset);
          break;
        case BIGINT:
        case LONG_WRITABLE:
          inputObjects_[i] = new ImpalaBigIntWritable(inputBufferPtr_ + offset);
          break;
        case FLOAT:
        case FLOAT_WRITABLE:
          inputObjects_[i] = new ImpalaFloatWritable(inputBufferPtr_ + offset);
          break;
        case DOUBLE:
        case DOUBLE_WRITABLE:
          inputObjects_[i] = new ImpalaDoubleWritable(inputBufferPtr_ + offset);
          break;
        case TEXT:
          inputObjects_[i] = new ImpalaTextWritable(inputBufferPtr_ + offset);
          break;
        case BYTES_WRITABLE:
          inputObjects_[i] = new ImpalaBytesWritable(inputBufferPtr_ + offset);
          break;
        case STRING:
          // String can be mapped to any String-like Writable class.
          inputObjects_[i] = new ImpalaBytesWritable(inputBufferPtr_ + offset);
          break;
        default:
          throw new ImpalaRuntimeException("Unsupported argument type: " + argTypes_[i]);
      }
    }
  }

  private URLClassLoader getClassLoader(String jarPath) throws MalformedURLException {
    URL url = new File(jarPath).toURI().toURL();
    return URLClassLoader.newInstance(new URL[] { url }, getClass().getClassLoader());
  }

  /**
   * Sets the return type of a Java UDF. Returns true if the return type is compatible
   * with the return type from the function definition. Throws an ImpalaRuntimeException
   * if the return type is not supported.
   */
  private boolean setReturnType(Type retType, Class<?> udfReturnType)
      throws ImpalaRuntimeException {
    if (!JavaUdfDataType.isSupported(retType)) {
      throw new ImpalaRuntimeException("Unsupported return type: " + retType.toSql());
    }
    JavaUdfDataType javaType = JavaUdfDataType.getType(udfReturnType);
    // Check if the evaluate method return type is compatible with the return type from
    // the function definition. This happens when both of them map to the same primitive
    // type.
    if (retType.getPrimitiveType().toThrift() != javaType.getPrimitiveType()) {
      return false;
    }
    retType_ = javaType;
    return true;
  }

  /**
   * Sets the argument types of a Java UDF. Returns true if the argument types specified
   * in the UDF are compatible with the argument types of the evaluate() function loaded
   * from the associated JAR file.
   */
  private boolean setArgTypes(Type[] parameterTypes, Class<?>[] udfArgTypes) {
    Preconditions.checkNotNull(argTypes_);
    for (int i = 0; i < udfArgTypes.length; ++i) {
      argTypes_[i] = JavaUdfDataType.getType(udfArgTypes[i]);
      if (argTypes_[i].getPrimitiveType()
          != parameterTypes[i].getPrimitiveType().toThrift()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Initializes the UdfExecutor validating the UDF has the proper signature.
   * This uses reflection to look up the "evaluate" function in the UDF class.
   */
  private void init(String jarPath, String udfPath,
      Type retType, Type... parameterTypes) throws
      ImpalaRuntimeException {
    ArrayList<String> signatures = Lists.newArrayList();
    try {
      LOG.debug("Loading UDF '" + udfPath + "' from " + jarPath);
      ClassLoader loader;
      if (jarPath != null) {
        // Save for cleanup.
        classLoader_ = getClassLoader(jarPath);
        loader = classLoader_;
      } else {
        loader = ClassLoader.getSystemClassLoader();
      }
      Class<?> c = Class.forName(udfPath, true, loader);
      Class<? extends UDF> udfClass = c.asSubclass(UDF.class);
      Constructor<? extends UDF> ctor = udfClass.getConstructor();
      udf_ = ctor.newInstance();
      argTypes_ = new JavaUdfDataType[parameterTypes.length];
      Method[] methods = udfClass.getMethods();
      for (Method m: methods) {
        // By convention, the udf must contain the function "evaluate"
        if (!m.getName().equals(UDF_FUNCTION_NAME)) continue;
        signatures.add(m.toGenericString());
        Class<?>[] methodTypes = m.getParameterTypes();

        // Try to match the arguments
        if (methodTypes.length != parameterTypes.length) continue;
        if (methodTypes.length == 0 && parameterTypes.length == 0) {
          // Special case where the UDF doesn't take any input args
          method_ = m;
          if (!setReturnType(retType, m.getReturnType())) continue;
          setArgTypes(parameterTypes, methodTypes);
          LOG.debug("Loaded UDF '" + udfPath + "' from " + jarPath);
          return;
        }

        method_ = m;
        if (!setReturnType(retType, m.getReturnType())) continue;
        if (!setArgTypes(parameterTypes, methodTypes)) continue;
        allocateInputObjects();
        LOG.debug("Loaded UDF '" + udfPath + "' from " + jarPath);
        return;
      }

      StringBuilder sb = new StringBuilder();
      sb.append("Unable to find evaluate function with the correct signature: ")
        .append(udfPath + ".evaluate(")
        .append(Joiner.on(", ").join(parameterTypes))
        .append(")\n")
        .append("UDF contains: \n    ")
        .append(Joiner.on("\n    ").join(signatures));
      throw new ImpalaRuntimeException(sb.toString());
    } catch (MalformedURLException e) {
      throw new ImpalaRuntimeException("Unable to load jar.", e);
    } catch (SecurityException e) {
      throw new ImpalaRuntimeException("Unable to load function.", e);
    } catch (ClassNotFoundException e) {
      throw new ImpalaRuntimeException("Unable to find class.", e);
    } catch (NoSuchMethodException e) {
      throw new ImpalaRuntimeException(
          "Unable to find constructor with no arguments.", e);
    } catch (IllegalArgumentException e) {
      throw new ImpalaRuntimeException(
          "Unable to call UDF constructor with no arguments.", e);
    } catch (InstantiationException e) {
      throw new ImpalaRuntimeException("Unable to call create UDF instance.", e);
    } catch (IllegalAccessException e) {
      throw new ImpalaRuntimeException("Unable to call create UDF instance.", e);
    } catch (InvocationTargetException e) {
      throw new ImpalaRuntimeException("Unable to call create UDF instance.", e);
    }
  }
}
