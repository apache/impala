// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.io.File;
import java.util.concurrent.BlockingQueue;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.thrift.TResultRow;

// Interface to C++ local executor
class NativePlanExecutor {
  protected native static void ExecPlan(byte[] thriftExecutePlanRequest,
      boolean asAscii, BlockingQueue<TResultRow> resultQueue) throws ImpalaException;

  static {
    // The path to the backend lib must be set in the java.library.path system property.
    // For some reason the following calls just hang:
    // System.loadLibrary("libplanexec.so");
    // System.loadLibrary("libplanexec");
    // System.loadLibrary("planexec");
    // There seem to be some known issues with System.loadLibrary() that cause deadlock.
    String libPath = System.getProperty("java.library.path");
    System.load(libPath + File.separator + "libplanexec.so");
  }
}

