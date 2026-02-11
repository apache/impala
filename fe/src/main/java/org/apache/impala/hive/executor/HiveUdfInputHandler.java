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

import com.google.common.base.Preconditions;

import org.apache.hadoop.io.Writable;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.THiveUdfExecutorCtorParams;
import org.apache.impala.util.UnsafeUtil;
import org.apache.log4j.Logger;

public class HiveUdfInputHandler {

  private static final Logger LOG = Logger.getLogger(HiveUdfInputHandler.class);

  // Argument types of the function inferred from the udf method signature.
  // The JavaUdfDataType enum maps it to corresponding primitive type.
  private final JavaUdfDataType[] argTypes_;

  // Input buffer from the backend. This is valid for the duration of an evaluate() call.
  // These buffers are allocated in the BE.
  private final long inputBufferPtr_;
  private final long inputNullsPtr_;

  // This is the byte offset in inputBufferPtr to the start of the input argument.
  // e.g. *inputBufferPtr_[inputBufferOffsets[i]] is the ith input argument.
  private final int[] inputBufferOffsets_;

  private final boolean[] isArgConst_;

  // Preconstructed input objects for the UDF. This minimizes object creation overhead
  // as these objects are reused across calls to evaluate().
  private final Writable[] inputObjects_;

  private final Object[] constObjects_;

  public HiveUdfInputHandler(
      THiveUdfExecutorCtorParams request,
      JavaUdfDataType[] argTypes) throws ImpalaRuntimeException {
    argTypes_ = argTypes;
    inputBufferPtr_ = request.input_buffer_ptr;
    inputNullsPtr_ = request.input_nulls_ptr;
    inputBufferOffsets_ = new int[request.input_byte_offsets.size()];
    for (int i = 0; i < request.input_byte_offsets.size(); ++i) {
      inputBufferOffsets_[i] = request.input_byte_offsets.get(i).intValue();
    }
    isArgConst_ = new boolean[request.fn.arg_types.size()];
    if (request.is_constant_arg != null) {
      for (int i = 0; i < request.is_constant_arg.size(); i++) {
        isArgConst_[i] = request.is_constant_arg.get(i);
      }
    }
    inputObjects_ = allocateInputObjects(argTypes_, inputBufferOffsets_, inputBufferPtr_);
    constObjects_ = new Object[isArgConst_.length];
    fillArgArray(constObjects_, inputObjects_, true);
  }

  public int getNumParams() {
    return inputObjects_.length;
  }

  public Object getInputObject(int i) {
    return inputObjects_[i];
  }

  public Writable[] getInputObjects() {
    return inputObjects_;
  }

  public boolean isArgConst(int i) {
    return isArgConst_[i];
  }

  public Object getConstObj(int i) {
    return constObjects_[i];
  }

  public long getInputNullsPtr() {
    return inputNullsPtr_;
  }

  public JavaUdfDataType[] getArgTypes() {
    return argTypes_;
  }

  // Preallocate the input objects that will be passed to the underlying UDF.
  // These objects are allocated once and reused across calls to evaluate()
  public Writable[] allocateInputObjects(JavaUdfDataType[] argTypes,
      int[] inputBufferOffsets, long inputBufferPtr) throws ImpalaRuntimeException {
    Writable[] inputObjects =  new Writable[argTypes.length];
    for (int i = 0; i < argTypes.length; ++i) {
      int offset = inputBufferOffsets[i];
      switch (argTypes[i]) {
        case BOOLEAN:
        case BOOLEAN_WRITABLE:
          inputObjects[i] = new ImpalaBooleanWritable(inputBufferPtr + offset);
          break;
        case TINYINT:
        case BYTE_WRITABLE:
          inputObjects[i] = new ImpalaTinyIntWritable(inputBufferPtr + offset);
          break;
        case SMALLINT:
        case SHORT_WRITABLE:
          inputObjects[i] = new ImpalaSmallIntWritable(inputBufferPtr + offset);
          break;
        case INT:
        case INT_WRITABLE:
          inputObjects[i] = new ImpalaIntWritable(inputBufferPtr+ offset);
          break;
        case BIGINT:
        case LONG_WRITABLE:
          inputObjects[i] = new ImpalaBigIntWritable(inputBufferPtr + offset);
          break;
        case FLOAT:
        case FLOAT_WRITABLE:
          inputObjects[i] = new ImpalaFloatWritable(inputBufferPtr + offset);
          break;
        case DOUBLE:
        case DOUBLE_WRITABLE:
          inputObjects[i] = new ImpalaDoubleWritable(inputBufferPtr + offset);
          break;
        case TEXT:
          inputObjects[i] = new ImpalaTextWritable(inputBufferPtr + offset);
          break;
        case BYTES_WRITABLE:
          inputObjects[i] = new ImpalaBytesWritable(inputBufferPtr + offset);
          break;
        case STRING:
          // String can be mapped to any String-like Writable class.
          inputObjects[i] = new ImpalaBytesWritable(inputBufferPtr + offset);
          break;
        default:
          throw new ImpalaRuntimeException("Unsupported argument type: " + argTypes[i]);
      }
    }
    return inputObjects;
  }

  public void fillArgArray(Object[] inputArgs, Object[] inputObjects, boolean onlyConst)
      throws ImpalaRuntimeException {
    try {
      for (int i = 0; i < argTypes_.length; ++i) {
        if (onlyConst && !isArgConst_[i]) continue;
        if (UnsafeUtil.UNSAFE.getByte(inputNullsPtr_ + i) == 0) {
          switch (argTypes_[i]) {
            case BOOLEAN_WRITABLE:
            case BYTE_WRITABLE:
            case SHORT_WRITABLE:
            case INT_WRITABLE:
            case LONG_WRITABLE:
            case FLOAT_WRITABLE:
            case DOUBLE_WRITABLE: inputArgs[i] = inputObjects[i]; break;
            case BYTE_ARRAY:
            case BYTES_WRITABLE:
              ((ImpalaBytesWritable) inputObjects[i]).reload();
              inputArgs[i] = inputObjects[i];
              break;
            case TEXT:
              ((ImpalaTextWritable) inputObjects[i]).reload();
              inputArgs[i] = inputObjects[i];
              break;
            case BOOLEAN:
              inputArgs[i] = ((ImpalaBooleanWritable)inputObjects[i]).get();
              break;
            case TINYINT:
              inputArgs[i] = ((ImpalaTinyIntWritable)inputObjects[i]).get();
              break;
            case SMALLINT:
              inputArgs[i] = ((ImpalaSmallIntWritable)inputObjects[i]).get();
              break;
            case INT:
              inputArgs[i] = ((ImpalaIntWritable)inputObjects[i]).get();
              break;
            case BIGINT:
              inputArgs[i] = ((ImpalaBigIntWritable)inputObjects[i]).get();
              break;
            case FLOAT:
              inputArgs[i] = ((ImpalaFloatWritable)inputObjects[i]).get();
              break;
            case DOUBLE:
              inputArgs[i] = ((ImpalaDoubleWritable)inputObjects[i]).get();
              break;
            case STRING:
              Preconditions.checkState(inputObjects[i] instanceof ImpalaBytesWritable);
              ImpalaBytesWritable inputObject = (ImpalaBytesWritable) inputObjects[i];
              inputObject.reload();
              inputArgs[i] = new String(inputObject.getBytes());
              break;
          }
        } else {
          inputArgs[i] = null;
        }
      }
    } catch (Exception e) {
      throw new ImpalaRuntimeException(
          "HiveUdfInputHandler::fillArgArray() ran into a problem.", e);
    }
  }

}
