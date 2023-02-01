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

import sun.misc.Unsafe;

import java.lang.reflect.Method;

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
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.THiveUdfExecutorCtorParams;
import org.apache.impala.util.UnsafeUtil;
import org.apache.log4j.Logger;

// Base Object to run hive UDFs. Hive has two ways in which UDFs are supported.
// 1) Deriving off of the UDF class (legacy)
// 2) Deriving off of the GenericUDF class.
// The base class here supports portions that are common to both implementations.
public abstract class HiveUdfExecutor {

  private static final Logger LOG = Logger.getLogger(HiveUdfExecutor.class);

  // Return and argument types of the function inferred from the udf method signature.
  // The JavaUdfDataType enum maps it to corresponding primitive type.
  private final JavaUdfDataType[] argTypes_;
  private final JavaUdfDataType retType_;

  // Input buffer from the backend. This is valid for the duration of an evaluate() call.
  // These buffers are allocated in the BE.
  private final long inputBufferPtr_;
  private final long inputNullsPtr_;

  // This is the byte offset in inputBufferPtr to the start of the input argument.
  // e.g. *inputBufferPtr_[inputBufferOffsets[i]] is the ith input argument.
  private final int[] inputBufferOffsets_;

  // Output buffer to return non-string values. These buffers are allocated in the BE.
  private final long outputBufferPtr_;
  private final long outputNullPtr_;

  // For StringValue return types, outputBufferPtr_ is the location of the 16-byte
  // StringValue object. StringValue.ptr is set to outBufferStringPtr_. This buffer
  // grows as necessary to fit the return string.
  // This is allocated from the FE.
  private long outBufferStringPtr_;

  // Size of outBufferStringPtr_.
  private int outBufferStringCapacity_;

  // Preconstructed input objects for the UDF. This minimizes object creation overhead
  // as these objects are reused across calls to evaluate().
  private final Writable[] inputObjects_;

  protected HiveUdfExecutor(
      THiveUdfExecutorCtorParams request,
      JavaUdfDataType retType, JavaUdfDataType[] argTypes) throws ImpalaRuntimeException {
    retType_ = retType;
    argTypes_ = argTypes;
    inputBufferPtr_ = request.input_buffer_ptr;
    inputNullsPtr_ = request.input_nulls_ptr;
    outputBufferPtr_ = request.output_buffer_ptr;
    outputNullPtr_ = request.output_null_ptr;
    outBufferStringPtr_ = 0;
    outBufferStringCapacity_ = 0;
    inputBufferOffsets_ = new int[request.input_byte_offsets.size()];
    for (int i = 0; i < request.input_byte_offsets.size(); ++i) {
      inputBufferOffsets_[i] = request.input_byte_offsets.get(i).intValue();
    }
    inputObjects_ = new Writable[argTypes_.length];
    allocateInputObjects();
  }

  /**
   * Releases any resources allocated off the native heap and close the class
   * loader we may have created.
   */
  public void close() {
    UnsafeUtil.UNSAFE.freeMemory(outBufferStringPtr_);
    outBufferStringPtr_ = 0;
    outBufferStringCapacity_ = 0;
    closeDerived();
  }

  /**
   * Evaluate function called by the backend. The inputs to the UDF have
   * been serialized to 'inputObjects_'
   */
  public long evaluate() throws ImpalaRuntimeException {
    return storeUdfResult(evaluateDerived(argTypes_, inputNullsPtr_, inputObjects_));
  }

  /**
   * Evaluates the UDF with 'args' as the input to the UDF. This is exposed
   * for testing and not the version of evaluate() the backend uses.
   */
  public long evaluateForTesting(Object... args) throws ImpalaRuntimeException {
    return storeUdfResult(evaluateDerived(argTypes_, inputNullsPtr_, args));
  }

  // Sets the result object 'obj' into the outputBufferPtr_
  // Returns the 0L (null) if the return value is null, otherwise return
  // outputBufferPtr_  which contains the long value of the pointer.
  protected long storeUdfResult(Object obj) throws ImpalaRuntimeException {
    if (obj == null) {
      UnsafeUtil.UNSAFE.putByte(outputNullPtr_, (byte)1);
      return 0L;
    }

    UnsafeUtil.UNSAFE.putByte(outputNullPtr_, (byte)0);
    switch (retType_) {
      case BOOLEAN_WRITABLE: {
        BooleanWritable val = (BooleanWritable)obj;
        UnsafeUtil.UNSAFE.putByte(outputBufferPtr_, val.get() ? (byte)1 : 0);
        return outputBufferPtr_;
      }
      case BOOLEAN: {
        UnsafeUtil.UNSAFE.putByte(outputBufferPtr_, (boolean)obj ? (byte)1 : 0);
        return outputBufferPtr_;
      }
      case BYTE_WRITABLE: {
        ByteWritable val = (ByteWritable)obj;
        UnsafeUtil.UNSAFE.putByte(outputBufferPtr_, val.get());
        return outputBufferPtr_;
      }
      case TINYINT: {
        UnsafeUtil.UNSAFE.putByte(outputBufferPtr_, (byte)obj);
        return outputBufferPtr_;
      }
      case SHORT_WRITABLE: {
        ShortWritable val = (ShortWritable)obj;
        UnsafeUtil.UNSAFE.putShort(outputBufferPtr_, val.get());
        return outputBufferPtr_;
      }
      case SMALLINT: {
        UnsafeUtil.UNSAFE.putShort(outputBufferPtr_, (short)obj);
        return outputBufferPtr_;
      }
      case INT_WRITABLE: {
        IntWritable val = (IntWritable)obj;
        UnsafeUtil.UNSAFE.putInt(outputBufferPtr_, val.get());
        return outputBufferPtr_;
      }
      case INT: {
        UnsafeUtil.UNSAFE.putInt(outputBufferPtr_, (int)obj);
        return outputBufferPtr_;
      }
      case LONG_WRITABLE: {
        LongWritable val = (LongWritable)obj;
        UnsafeUtil.UNSAFE.putLong(outputBufferPtr_, val.get());
        return outputBufferPtr_;
      }
      case BIGINT: {
        UnsafeUtil.UNSAFE.putLong(outputBufferPtr_, (long)obj);
        return outputBufferPtr_;
      }
      case FLOAT_WRITABLE: {
        FloatWritable val = (FloatWritable)obj;
        UnsafeUtil.UNSAFE.putFloat(outputBufferPtr_, val.get());
        return outputBufferPtr_;
      }
      case FLOAT: {
        UnsafeUtil.UNSAFE.putFloat(outputBufferPtr_, (float)obj);
        return outputBufferPtr_;
      }
      case DOUBLE_WRITABLE: {
        DoubleWritable val = (DoubleWritable)obj;
        UnsafeUtil.UNSAFE.putDouble(outputBufferPtr_, val.get());
        return outputBufferPtr_;
      }
      case DOUBLE: {
        UnsafeUtil.UNSAFE.putDouble(outputBufferPtr_, (double)obj);
        return outputBufferPtr_;
      }
      case TEXT: {
        copyBytesToOutputBuffer(((Text)obj).copyBytes());
        return outputBufferPtr_;
      }
      case BYTE_ARRAY: {
        copyBytesToOutputBuffer((byte[]) obj);
        return outputBufferPtr_;
      }
      case BYTES_WRITABLE: {
        copyBytesToOutputBuffer(((BytesWritable)obj).copyBytes());
        return outputBufferPtr_;
      }
      case STRING: {
        copyBytesToOutputBuffer(((String)obj).getBytes());
        return outputBufferPtr_;
      }
      default:
        throw new ImpalaRuntimeException("Unsupported return type: " + retType_);
    }
  }

  protected int getNumParams() {
    return inputObjects_.length;
  }

  protected Object getInputObject(int i) {
    return inputObjects_[i];
  }

  private void copyBytesToOutputBuffer(byte[] bytes) {
    if (bytes.length > outBufferStringCapacity_) {
      outBufferStringPtr_ =
          UnsafeUtil.UNSAFE.reallocateMemory(outBufferStringPtr_, bytes.length);
      outBufferStringCapacity_ = bytes.length;
      UnsafeUtil.UNSAFE.putLong(outputBufferPtr_, outBufferStringPtr_);
    }
    UnsafeUtil.Copy(outBufferStringPtr_, bytes, 0, bytes.length);
    UnsafeUtil.UNSAFE.putInt(
        outputBufferPtr_ + JavaUdfDataType.STRING_VALUE_LEN_OFFSET, bytes.length);
  }

  // Preallocate the input objects that will be passed to the underlying UDF.
  // These objects are allocated once and reused across calls to evaluate()
  private void allocateInputObjects() throws ImpalaRuntimeException {
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

  public static Type getRetType(THiveUdfExecutorCtorParams request) {
    return Type.fromThrift(request.fn.ret_type);
  }

  public static Type[] getParameterTypes(THiveUdfExecutorCtorParams request) {
    Type[] parameterTypes = new Type[request.fn.arg_types.size()];
    for (int i = 0; i < request.fn.arg_types.size(); ++i) {
      parameterTypes[i] = Type.fromThrift(request.fn.arg_types.get(i));
    }
    return parameterTypes;
  }

  /**
   * Abstract method allowing derived class to deinitialize what it needs to.
   */
  abstract protected void closeDerived();

  /**
   * Abstract method allowing derived class to evaluate the function.
   */
  abstract protected Object evaluateDerived(JavaUdfDataType[] argTypes,
      long inputNullsPtr, Object[] inputObjectArgs) throws ImpalaRuntimeException;

  /**
   * Abstract method returning the Java reflection Method type of the 'evaluate' method.
   */
  abstract public Method getMethod();

}
