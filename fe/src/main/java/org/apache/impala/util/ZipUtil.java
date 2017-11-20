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

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.impala.common.InternalException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.thrift.TExtractFromZipParams;
import org.apache.thrift.protocol.TBinaryProtocol;

public class ZipUtil {
  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();

  /**
   * Extracts files from a Zip file to a destination directory in the local filesystem.
   * Accepts serialized TExtractFromZipParams.
   */
  public static void extractFiles(byte[] serializedParams) throws ImpalaException {
    TExtractFromZipParams params = new TExtractFromZipParams();
    JniUtil.deserializeThrift(protocolFactory_, params, serializedParams);
    extractFiles(params.archive_file, params.destination_dir);
  }

  /**
   * Extracts files from 'archiveFilePath' Zip file to 'destDirPath' destination directory
   * in the local filesystem.
   */
  public static void extractFiles(String archiveFilePath, String destDirPath)
      throws ImpalaException {
    try (ZipFile zip = new ZipFile(archiveFilePath)) {
      File destDir = new File(destDirPath);
      String canonicalDestDirPath = destDir.getCanonicalPath();

      Enumeration enumEntries = zip.entries();
      while (enumEntries.hasMoreElements()) {
        ZipEntry entry = (ZipEntry) enumEntries.nextElement();
        File destFile = new File(destDir, entry.getName());

        // Fix for Zip-Slip vulnerability: Make sure that the destination file's path is
        // under params.destination_dir.
        String canonicalDestFilePath = destFile.getCanonicalPath();
        if (!canonicalDestFilePath.startsWith(canonicalDestDirPath + File.separator)) {
          throw new IllegalStateException("Invalid destination path in the archive:" +
              canonicalDestFilePath);
        }

        if (entry.isDirectory()) {
          if (!destFile.exists() && !destFile.mkdirs()) {
            throw new IllegalStateException("Couldn't create dir: " + destFile);
          }
          continue;
        }

        File parent = destFile.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
          throw new IllegalStateException("Couldn't create dir: " + parent);
        }

        try (InputStream is = zip.getInputStream(entry);
            FileOutputStream fos = new FileOutputStream(destFile)) {
          while (is.available() > 0) {
            fos.write(is.read());
          }
        }
      }
    } catch (Exception e) {
      throw new InternalException(e.getMessage());
    }
  }
}
