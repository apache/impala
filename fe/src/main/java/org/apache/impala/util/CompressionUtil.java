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

package org.apache.impala.util;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CompressionUtil {

  private final static Logger LOG = LoggerFactory.getLogger(CompressionUtil.class);

  /**
   * Compresses a given input byte array using DeflaterOutputStream. Returns null if
   * the input is null or if there is an error compressing the input.
   */
  public static byte[] deflateCompress(byte[] input) {
    if (input == null) return null;
    ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);
    // Experiments on a wide partitioned table with incremental stats showed that the
    // Deflater with 'BEST_SPEED' level provided reasonable compression ratios at much
    // faster speeds compared to other modes like BEST_COMPRESSION/DEFAULT_COMPRESSION.
    DeflaterOutputStream stream =
        new DeflaterOutputStream(bos, new Deflater(Deflater.BEST_SPEED)) {
          // IMPALA-11753: to avoid CatalogD OOM we invoke def.end() which frees
          // the natively allocated memory by the Deflater. See details in Jira.
          @Override
          public void close() throws IOException {
            try {
              super.close();
            } finally {
              def.end();
            }
          }
        };
    try {
      stream.write(input);
      stream.close();
    } catch (IOException e) {
      LOG.error("Error compressing input bytes.", e);
      return null;
    }
    return bos.toByteArray();
  }

  /**
   * Decompresses a deflate-compressed byte array and returns the output bytes. Returns
   * null if the input is null or if there is an error decompressing the input.
   */
  public static byte[] deflateDecompress(byte[] input) {
    if (input == null) return null;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try{
      IOUtils.copy(new InflaterInputStream(new ByteArrayInputStream(input)), out);
    } catch(IOException e){
      LOG.error("Error decompressing input bytes.", e);
      return null;
    }
    return out.toByteArray();
  }
}
