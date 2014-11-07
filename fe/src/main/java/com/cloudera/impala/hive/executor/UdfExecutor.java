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

package com.cloudera.impala.hive.executor;

import java.io.File;
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
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.thrift.THiveUdfExecutorCtorParams;
import com.cloudera.impala.util.UnsafeUtil;
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
  private static final String UDF_FUNCTION_NAME = "evaluate";

  // Object to deserialize ctor params from BE.
  private final static TBinaryProtocol.Factory protocolFactory =
    new TBinaryProtocol.Factory();

  private UDF udf_;
  private Method method_;
  private Type[] argTypes_;
  private Type retType_;

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
  // True if inputArgs_[i] is the java String type (and not a writable). We need
  // to make a String object before calling the UDF.
  // TODO: is there a unsafe way to make string objects?
  private boolean[] isArgString_;

  // Allocations made from the native heap that need to be cleaned when this object
  // is GC'ed.
  ArrayList<Long> allocations_ = Lists.newArrayList();

  /**
   * Create a UdfExecutor, using parameters from a serialized thrift object. Used by
   * the backend.
   */
  public UdfExecutor(byte[] thriftParams) throws ImpalaException {
    THiveUdfExecutorCtorParams request = new THiveUdfExecutorCtorParams();
    JniUtil.deserializeThrift(protocolFactory, request, thriftParams);

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

  /**
   * Creates a UdfExecutor object, loading the class and validating it
   * has the proper function. This constructor is only used for testing.
   *
   * @param jarFile: Path to jar containing the UDF. null indicates to use the current
   * jar file.
   * @param udfPath: fully qualified class path for the UDF
   */
  public UdfExecutor(String jarFile, String udfPath,
      Type retType, Type... parameterTypes)
      throws ImpalaRuntimeException {

    inputBufferOffsets_ = new int[parameterTypes.length];

    int inputBufferSize = 0;
    for (int i = 0; i < parameterTypes.length; ++i) {
      inputBufferOffsets_[i] = inputBufferSize;
      inputBufferSize += parameterTypes[i].getSlotSize();
    }

    inputBufferPtr_ = UnsafeUtil.UNSAFE.allocateMemory(inputBufferSize);
    inputNullsPtr_ = UnsafeUtil.UNSAFE.allocateMemory(parameterTypes.length);
    outputBufferPtr_ = UnsafeUtil.UNSAFE.allocateMemory(retType.getSlotSize());
    outputNullPtr_ = UnsafeUtil.UNSAFE.allocateMemory(1);
    allocations_.add(inputBufferPtr_);
    allocations_.add(inputNullsPtr_);
    allocations_.add(outputBufferPtr_);
    allocations_.add(outputNullPtr_);
    outBufferStringPtr_ = 0;
    outBufferCapacity_ = 0;

    init(jarFile, udfPath, retType, parameterTypes);
  }


  @Override
  protected void finalize() throws Throwable {
    close();
    super.finalize();
  }

  /**
   * Releases any resources allocated off the native heap.
   */
  public void close() {
    UnsafeUtil.UNSAFE.freeMemory(outBufferStringPtr_);
    outBufferStringPtr_ = 0;
    outBufferCapacity_ = 0;

    for (long ptr: allocations_) {
      UnsafeUtil.UNSAFE.freeMemory(ptr);
    }
    allocations_.clear();
  }

  /**
   * evaluate function called by the backend. The inputs to the UDF have
   * been serialized to 'input'
   */
  public void evaluate() throws ImpalaRuntimeException {
    try {
      for (int i = 0; i < argTypes_.length; ++i) {
        if (UnsafeUtil.UNSAFE.getByte(inputNullsPtr_ + i) == 0) {
          if (isArgString_[i]) {
            Preconditions.checkState(inputObjects_[i] instanceof ImpalaBytesWritable);
            inputArgs_[i] =
                new String(((ImpalaBytesWritable)inputObjects_[i]).getBytes());
          } else {
            inputArgs_[i] = inputObjects_[i];
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
   * Returns 0 if the udf returned NULL. (the result is a ptr so this is okay).
   */
  public long evaluate(Object... args) throws ImpalaRuntimeException {
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

  // Returns the primitive type that c is for. 'c' is expected to be
  // a subclass of Writable. This is a many to one mapping: e.g. many
  // writables map to the same type.
  private PrimitiveType getPrimitiveType(Class<?> c) {
    // TODO: do we need to check c is a subclass of *Writable.class?
    if (c == BooleanWritable.class) {
      return PrimitiveType.BOOLEAN;
    } else if (c == ByteWritable.class) {
      return PrimitiveType.TINYINT;
    } else if (c == ShortWritable.class) {
      return PrimitiveType.SMALLINT;
    } else if (c == IntWritable.class) {
      return PrimitiveType.INT;
    } else if (c == LongWritable.class) {
      return PrimitiveType.BIGINT;
    } else if (c == FloatWritable.class) {
      return PrimitiveType.FLOAT;
    } else if (c == DoubleWritable.class) {
      return PrimitiveType.DOUBLE;
    } else if (c == BytesWritable.class || c == Text.class || c == String.class) {
      // TODO: we don't distinguish between these types and will pick between them
      // arbitrarily. This can be problematic, if for example, the UDF has different
      // behavior for Bytes vs. Text.
      return PrimitiveType.STRING;
    }
    return PrimitiveType.INVALID_TYPE;
  }

  // Sets the result object 'obj' into the outputBufferPtr_
  private void storeUdfResult(Object obj) throws ImpalaRuntimeException {
    if (obj == null) {
      UnsafeUtil.UNSAFE.putByte(outputNullPtr_, (byte)1);
      return;
    }

    UnsafeUtil.UNSAFE.putByte(outputNullPtr_, (byte)0);

    switch (retType_.getPrimitiveType()) {
      case BOOLEAN: {
        BooleanWritable val = (BooleanWritable)obj;
        UnsafeUtil.UNSAFE.putByte(outputBufferPtr_, val.get() ? (byte)1 : 0);
        return;
      }
      case TINYINT: {
        ByteWritable val = (ByteWritable)obj;
        UnsafeUtil.UNSAFE.putByte(outputBufferPtr_, val.get());
        return;
      }
      case SMALLINT: {
        ShortWritable val = (ShortWritable)obj;
        UnsafeUtil.UNSAFE.putShort(outputBufferPtr_, val.get());
        return;
      }
      case INT: {
        IntWritable val = (IntWritable)obj;
        UnsafeUtil.UNSAFE.putInt(outputBufferPtr_, val.get());
        return;
      }
      case BIGINT: {
        LongWritable val = (LongWritable)obj;
        UnsafeUtil.UNSAFE.putLong(outputBufferPtr_, val.get());
        return;
      }
      case FLOAT: {
        FloatWritable val = (FloatWritable)obj;
        UnsafeUtil.UNSAFE.putFloat(outputBufferPtr_, val.get());
        return;
      }
      case DOUBLE: {
        DoubleWritable val = (DoubleWritable)obj;
        UnsafeUtil.UNSAFE.putDouble(outputBufferPtr_, val.get());
        return;
      }
      case STRING: {
        byte[] bytes = null;
        if (obj instanceof byte[]) {
          bytes = (byte[]) obj;
        } else if (obj instanceof BytesWritable) {
          bytes = ((BytesWritable)obj).copyBytes();
        } else if (obj instanceof Text) {
          bytes = ((Text)obj).copyBytes();
        } else if (obj instanceof String) {
          bytes = ((String)obj).getBytes();
        } else {
          throw new ImpalaRuntimeException("Unexpected return type: " + obj.getClass());
        }

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
        return;
      }
      case TIMESTAMP:
      default:
        throw new ImpalaRuntimeException("Unsupported argument type: " + retType_);
    }
  }

  // Preallocate the input objects that will be passed to the underlying UDF.
  // These objects are allocated once and reused across calls to evaluate()
  private void allocateInputObjects() throws ImpalaRuntimeException {
    inputObjects_ = new Writable[argTypes_.length];
    inputArgs_ = new Object[argTypes_.length];
    isArgString_ = new boolean[argTypes_.length];

    for (int i = 0; i < argTypes_.length; ++i) {
      int offset = inputBufferOffsets_[i];
      switch (argTypes_[i].getPrimitiveType()) {
        case BOOLEAN:
          inputObjects_[i] = new ImpalaBooleanWritable(inputBufferPtr_ + offset);
          break;
        case TINYINT:
          inputObjects_[i] = new ImpalaTinyIntWritable(inputBufferPtr_ + offset);
          break;
        case SMALLINT:
          inputObjects_[i] = new ImpalaSmallIntWritable(inputBufferPtr_ + offset);
          break;
        case INT:
          inputObjects_[i] = new ImpalaIntWritable(inputBufferPtr_ + offset);
          break;
        case BIGINT:
          inputObjects_[i] = new ImpalaBigIntWritable(inputBufferPtr_ + offset);
          break;
        case FLOAT:
          inputObjects_[i] = new ImpalaFloatWritable(inputBufferPtr_ + offset);
          break;
        case DOUBLE:
          inputObjects_[i] = new ImpalaDoubleWritable(inputBufferPtr_ + offset);
          break;
        case STRING:
          if (method_.getParameterTypes()[i] == Text.class) {
            ImpalaTextWritable w = new ImpalaTextWritable(inputBufferPtr_ + offset);
            inputObjects_[i] = w;
          } else  if (method_.getParameterTypes()[i] == BytesWritable.class) {
            ImpalaBytesWritable w = new ImpalaBytesWritable(inputBufferPtr_ + offset);
            inputObjects_[i] = w;
          } else if (method_.getParameterTypes()[i] == String.class) {
            isArgString_[i] = true;
            // String can be mapped to any String-like Writable class.
            ImpalaBytesWritable w = new ImpalaBytesWritable(inputBufferPtr_ + offset);
            inputObjects_[i] = w;
          } else {
            throw new ImpalaRuntimeException(
                "Unsupported argument type: " + method_.getParameterTypes()[i]);
          }
          break;
        case TIMESTAMP:
        default:
          throw new ImpalaRuntimeException("Unsupported argument type: " + argTypes_[i]);
        }
    }
  }

  private ClassLoader getClassLoader(String jarPath) throws MalformedURLException {
    if (jarPath == null) {
      return ClassLoader.getSystemClassLoader();
    } else {
      URL url = new File(jarPath).toURI().toURL();
      return URLClassLoader.newInstance(new URL[] { url }, getClass().getClassLoader());
    }
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
      ClassLoader loader = getClassLoader(jarPath);
      Class<?> c = Class.forName(udfPath, true, loader);
      Class<? extends UDF> udfClass = c.asSubclass(UDF.class);
      Constructor<? extends UDF> ctor = udfClass.getConstructor();
      udf_ = ctor.newInstance();
      retType_ = retType;
      argTypes_ = parameterTypes;
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
          LOG.debug("Loaded UDF '" + udfPath + "' from " + jarPath);
          return;
        }

        boolean incompatible = false;
        for (int i = 0; i < methodTypes.length; ++i) {
          if (getPrimitiveType(methodTypes[i]) != parameterTypes[i].getPrimitiveType()) {
            incompatible = true;
            break;
          }
        }

        if (incompatible) continue;
        method_ = m;
        allocateInputObjects();
        LOG.debug("Loaded UDF '" + udfPath + "' from " + jarPath);
        return;
      }

      StringBuilder sb = new StringBuilder();
      sb.append("Unable to find evaluate function with the correct signature: ")
        .append(udfPath + ".evaluate(")
        .append(Joiner.on(", ").join(argTypes_))
        .append(")\n")
        .append("UDF contains: \n    ")
        .append(Joiner.on("\n    ").join(signatures));
      throw new ImpalaRuntimeException(sb.toString());
    } catch (MalformedURLException e) {
      throw new ImpalaRuntimeException("Unable load jar.", e);
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
