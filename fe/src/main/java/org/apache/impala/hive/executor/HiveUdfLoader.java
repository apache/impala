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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.FunctionUtils.UDFClassType;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.common.FileSystemUtil;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.UUID;

import com.google.common.base.Preconditions;

import org.apache.log4j.Logger;

/**
 * Class responsible for the Java reflection needed to fetch the UDF
 * class and function.
 */
public class HiveUdfLoader implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(HiveUdfLoader.class);
  private final Class<?> udfClass_;

  private final UDFClassType classType_;

  private final ClassLoader classLoader_;


  private boolean isClassLoaderClosed_;

  /**
   * Creates a HiveUdfLoader object responsible for loading a jar and class
   * for a UDF.
   * @param localJarPath: the file system path where the jar file is located.
   * @param className: the class name of the UDF
   * @param persistLoader: true if loader needs to be persisted after loading. At
   *     more classes, so we allow this flexibility. In this case, the caller is
   *     responsible to call "close" on this object.
   */
  public HiveUdfLoader(String localJarPath, String className) throws CatalogException {
    LOG.debug("Loading UDF '" + className + "' from " + localJarPath);
    // If the localJarPath is not set, we use the System ClassLoader which
    // does not need to be tracked for closing.
    classLoader_ = getClassLoader(localJarPath);
    udfClass_  = loadUDFClass(className, classLoader_);
    classType_ = FunctionUtils.getUDFClassType(udfClass_);
  }

  public Class<?> getUDFClass() {
    return udfClass_;
  }

  @Override
  public void close() {
    // We only need to close URLClassLoaders. If no jar was present at instantiation,
    // it uses the SystemClassLoader (leaving this in for legacy purposes, but I'm not
    // sure this is even possible).
    if (!(classLoader_ instanceof URLClassLoader)) {
      return;
    }

    if (isClassLoaderClosed_) {
      return;
    }

    URLClassLoader urlClassLoader = (URLClassLoader) classLoader_;
    try {
      urlClassLoader.close();
    } catch (IOException e) {
      LOG.warn("Failed to close classloader: " + e.getMessage());
    }
    isClassLoaderClosed_ = true;
  }

  public UDFClassType getUDFClassType() {
    return classType_;
  }

  private static Class<?> loadUDFClass(String className,
      ClassLoader classLoader) throws CatalogException {
    try {
      return classLoader.loadClass(className);
    } catch (ClassNotFoundException c) {
      String errorMsg = className + " not found in Jar.";
      throw new CatalogException(errorMsg, c);
    } catch (LinkageError e) {
      String errorMsg = "Error resolving dependencies.";
      throw new CatalogException("Error resolving dependencies for class " + className
          + ".", e);
    } catch (Exception e) {
      throw new CatalogException("Error loading class " + className +".", e);
    }
  }

  private static ClassLoader getClassLoader(String jarPath) throws CatalogException {
    try {
      if (jarPath == null) {
        return ClassLoader.getSystemClassLoader();
      }
      URL[] classLoaderUrls = new URL[] {new URL(jarPath)};
      return new URLClassLoader(classLoaderUrls);
    } catch (MalformedURLException e) {
      throw new CatalogException("Unable to load jar " + jarPath, e);
    }
  }

  public static HiveUdfLoader createWithLocalPath(String localLibPath, Function fn)
      throws CatalogException {
    Path localJarPath = null;
    String fullFunctionName = fn.getDbName() + "." + fn.getFunctionName();
    String uri = fn.getResourceUris().get(0).getUri();
    try {
      // TODO(todd): cache these jars based on the mtime and file ID of the
      // remote JAR? Can we share a cache with the backend?
      String localJarPathString = null;
      if (uri != null) {
        localJarPath = new Path("file://" + localLibPath,
            UUID.randomUUID().toString() + ".jar");
        Preconditions.checkNotNull(localJarPath);
        try {
          FileSystemUtil.copyToLocal(new Path(uri), localJarPath);
        } catch (IOException e) {
          String errorMsg = "Couldn't copy " + uri + " to local path: " +
              localJarPath.toString();
          LOG.error(errorMsg, e);
          throw new CatalogException(errorMsg);
        }
        localJarPathString = localJarPath.toString();
      }
      return new HiveUdfLoader(localJarPathString, fn.getClassName());
    } catch (Exception e) {
      String errorMsg = "Could not load class " + fn.getClassName() + " from "
          + "jar " + uri + ": " + e.getMessage();
      throw new CatalogException(errorMsg, e);
    } finally {
      if (localJarPath != null) {
        FileSystemUtil.deleteIfExists(localJarPath);
      }
    }
  }
}
