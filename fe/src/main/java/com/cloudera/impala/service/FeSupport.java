// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides the Impala executor functionality to the FE.
 * fe-support.cc implements all the native calls.
 * If the planner is executed inside Impalad, Impalad would have registered all the JNI
 * native functions already. There's no need to load the shared library.
 * For unit test (mvn test), load the shared library because
 *   1. loading the shared library would started an in-process Impala Server for the test
 *      client to connect to.
 *   2. the native function has not been loaded yet.
 */
public class FeSupport {
  private final static Logger LOG = LoggerFactory.getLogger(FeSupport.class);

  private static boolean loaded = false;

  public native static boolean NativeEvalPredicate(byte[] thriftPredicate);

  public static boolean EvalPredicate(byte[] thriftPredicate) {
    try {
      return NativeEvalPredicate(thriftPredicate);
    } catch (UnsatisfiedLinkError e) {
      loadLibrary();
    }
    return NativeEvalPredicate(thriftPredicate);
  }

  /**
   * This function should only be called explicitly by the test util to ensure that
   * the in-process impala server has already started.
   */
  public static synchronized void loadLibrary() {
    if (loaded) {
      return;
    }
    loaded = true;

    // Search for libfesupport.so in all library paths.
    String libPath = System.getProperty("java.library.path");
    LOG.info("trying to load libfesupport.so from " + libPath);
    String[] paths = libPath.split(":");
    boolean found = false;
    for (String path : paths) {
      String filePath = path + File.separator + "libfesupport.so";
      File libFile = new File(filePath);
      if (libFile.exists()) {
        LOG.info("loading " + filePath);
        System.load(filePath);
        found = true;
        break;
      }
    }
    if (!found) {
      LOG.error("Failed to load libfesupport.so from given java.library.paths ("
          + libPath + ").");
    }
  }
}

