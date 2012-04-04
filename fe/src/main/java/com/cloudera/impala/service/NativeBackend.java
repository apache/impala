// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.thrift.TResultRow;

// Interface to C++ local executor
public class NativeBackend {
  private final static Logger LOG = LoggerFactory.getLogger(NativeBackend.class);

  // Select queries return a queue of tuples.
  protected native static void ExecQuery(byte[] thriftQueryExecRequest,
      List<String> errorLog, Map<String, Integer> fileErrors,
      BlockingQueue<TResultRow> resultQueue,
      InsertResult insertResult) throws ImpalaException;

  public native static boolean EvalPredicate(byte[] thriftPredicate);

  static {
    // The path to the backend lib must be set in the java.library.path system property.
    // For some reason the following calls just hang:
    // System.loadLibrary("libbackend.so");
    // System.loadLibrary("libbackend");
    // System.loadLibrary("backend");
    // There seem to be some known issues with System.loadLibrary() that cause deadlock.

    // Search for libbackend.so in all library paths.
    String libPath = System.getProperty("java.library.path");
    LOG.info("trying to load libbackend.so from " + libPath);
    String[] paths = libPath.split(":");
    boolean found = false;
    for (String path : paths) {
      String filePath = path + File.separator + "libbackend.so";
      File libFile = new File(filePath);
      if (libFile.exists()) {
        LOG.info("loading " + filePath);
        System.load(filePath);
        found = true;
        break;
      }
    }
    if (!found) {
      LOG.error("Failed to load libbackend.so from given java.library.paths ("
          + libPath + ").");
    }
  }
}

