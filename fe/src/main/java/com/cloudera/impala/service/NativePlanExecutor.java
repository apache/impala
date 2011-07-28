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
class NativePlanExecutor {
  private final static Logger LOG = LoggerFactory.getLogger(NativePlanExecutor.class);

  // Call this once before making any ExecPlan() calls.
  protected native static void Init();

  protected native static void ExecPlan(byte[] thriftExecutePlanRequest,
      boolean abortOnError, int maxErrors, List<String> errorLog, Map<String, Integer> fileErrors,
      boolean asAscii, BlockingQueue<TResultRow> resultQueue) throws ImpalaException;

  static {
    // The path to the backend lib must be set in the java.library.path system property.
    // For some reason the following calls just hang:
    // System.loadLibrary("libplanexec.so");
    // System.loadLibrary("libplanexec");
    // System.loadLibrary("planexec");
    // There seem to be some known issues with System.loadLibrary() that cause deadlock.

    // Search for libplanexec.so in all library paths.
    String libPath = System.getProperty("java.library.path");
    String[] paths = libPath.split(":");
    boolean found = false;
    for (String path : paths) {
      String filePath = path + File.separator + "libplanexec.so";
      File libFile = new File(filePath);
      if (libFile.exists()) {
        System.load(filePath);
        found = true;
        break;
      }
    }
    if (!found) {
      LOG.error("Failed to load libplanexec.so from given java.library.paths (" + libPath + ").");
    }
  }
}

