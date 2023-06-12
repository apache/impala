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

package org.apache.impala;

import java.text.ParseException;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * This class intends to test a UDF that manipulates array-backed writables and checks
 * their data retention behavior. Before IMPALA-11854, every getBytes() method call
 * directly read the native heap and there was no option to store intermediate results in
 * the writable buffers. IMPALA-11854 changed this behavior, now every data manipulation
 * operation on array-backed writables loads the native heap data before the 'evaluate'
 * phase and stores it in the writable, any subsequent manipulations use the writable's
 * interface.
 */
public class BufferAlteringUdf extends UDF {

  /**
   * Increments the first byte by one in a BytesWritable and returns the result as a
   * BytesWritable
   */
  public BytesWritable evaluate(BytesWritable bytesWritable) throws ParseException {
    if (null == bytesWritable) {
      return null;
    }
    byte[] bytes = bytesWritable.getBytes();

    incrementByteArray(bytes);
    return bytesWritable;
  }

  /**
   * Copies the source BytesWritable to the target Text, implicitly resizing it.
   * After the copy, the first byte of the target Text is incremented by one.
   */
  public Text evaluate(Text target, BytesWritable source) throws ParseException {
    if (null == source || null == target) {
      return null;
    }
    byte[] sourceArray = source.getBytes();
    target.set(sourceArray, 0, source.getLength());

    byte[] targetArray = target.getBytes();
    incrementByteArray(targetArray);

    return target;
  }

  private void incrementByteArray(byte[] array) {
    if (array.length > 0) {
      array[0] += 1;
    }
  }
}
