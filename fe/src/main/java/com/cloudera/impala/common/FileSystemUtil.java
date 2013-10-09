// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Common utility functions for operating on FileSystem objects.
 */
public class FileSystemUtil {
  private static final Configuration CONF = new Configuration();
  private static final Logger LOG = Logger.getLogger(FileSystemUtil.class);

  /**
   * Performs a non-recursive delete of all visible (non-hidden) files in a given
   * directory. Returns the number of files deleted as part of this operation.
   */
  public static int deleteAllVisibleFiles(Path directory)
      throws IOException {
    FileSystem fs = directory.getFileSystem(CONF);
    Preconditions.checkState(fs.getFileStatus(directory).isDirectory());
    int numFilesDeleted = 0;
    for (FileStatus fStatus: fs.listStatus(directory)) {
      // Only delete files that are not hidden.
      if (fStatus.isFile() && !isHiddenFile(fStatus.getPath().getName())) {
        LOG.debug("Removing: " + fStatus.getPath());
        fs.delete(fStatus.getPath(), false);
        ++numFilesDeleted;
      }
    }
    return numFilesDeleted;
  }

  /**
   * Returns the total number of visible (non-hidden) files in a directory.
   */
  public static int getTotalNumVisibleFiles(Path directory) throws IOException {
    FileSystem fs = directory.getFileSystem(CONF);
    Preconditions.checkState(fs.getFileStatus(directory).isDirectory());
    int numFiles = 0;
    for (FileStatus fStatus: fs.listStatus(directory)) {
      // Only delete files that are not hidden.
      if (fStatus.isFile() && !isHiddenFile(fStatus.getPath().getName())) {
        ++numFiles;
      }
    }
    return numFiles;
  }

  /**
   * Moves all visible (non-hidden) files from a source directory to a destination
   * directory. Any sub-directories within the source directory are skipped.
   * Returns the number of files moved as part of this operation.
   */
  public static int moveAllVisibleFiles(Path sourceDir, Path destDir)
      throws IOException {
    FileSystem fs = destDir.getFileSystem(CONF);
    Preconditions.checkState(fs.isDirectory(destDir));
    Preconditions.checkState(fs.isDirectory(sourceDir));

    // Use the same UUID to resolve all file name conflicts. This helps mitigate problems
    // that might happen if there is a conflict moving a set of files that have
    // dependent file names. For example, foo.lzo and foo.lzo_index.
    UUID uuid = UUID.randomUUID();

    // Enumerate all the files in the source
    int numFilesMoved = 0;
    for (FileStatus fStatus: fs.listStatus(sourceDir)) {
      if (fStatus.isDirectory()) {
        LOG.debug("Skipping copy of directory: " + fStatus.getPath());
        continue;
      } else if (isHiddenFile(fStatus.getPath().getName())) {
        continue;
      }

      Path destFile = new Path(destDir, fStatus.getPath().getName());
      if (fs.exists(destFile)) {
        destFile = new Path(destDir,
            appendToBaseFileName(destFile.getName(), uuid.toString()));
      }
      FileSystemUtil.moveFile(fStatus.getPath(), destFile, false);
      ++numFilesMoved;
    }
    return numFilesMoved;
  }

  /**
   * Moves (renames) the given file to a new location (either another directory or a
   * file. If renameIfAlreadyExists is true, no error will be thrown if a file with the
   * same name already exists in the destination location. Instead, a UUID will be
   * appended to the base file name, preserving the the existing file extension.
   * If renameIfAlreadyExists is false, an IOException will be thrown if there is a
   * file name conflict.
   */
  public static void moveFile(Path sourceFile, Path dest,
      boolean renameIfAlreadyExists) throws IOException {
    FileSystem fs = dest.getFileSystem(CONF);

    Path destFile = fs.isDirectory(dest) ? new Path(dest, sourceFile.getName()) : dest;
    // If a file with the same name does not already exist in the destination location
    // then use the same file name. Otherwise, generate a unique file name.
    if (renameIfAlreadyExists && fs.exists(destFile)) {
      Path destDir = fs.isDirectory(dest) ? dest : dest.getParent();
      destFile = new Path(destDir,
          appendToBaseFileName(destFile.getName(), UUID.randomUUID().toString()));
    }
    LOG.debug(String.format(
        "Moving '%s' to '%s'", sourceFile.toString(), destFile.toString()));
    // Move (rename) the file.
    fs.rename(sourceFile, destFile);
  }

  /**
   * Reads the file at path and returns the contents.
   */
  public static String readFile(Path file) throws IOException {
    FileSystem fs = file.getFileSystem(CONF);
    InputStream fileStream = fs.open(file);
    try {
      return IOUtils.toString(fileStream);
    } finally {
      IOUtils.closeQuietly(fileStream);
    }
  }

  /**
   * Builds a new file name based on a base file name. This is done by inserting
   * the given appendStr into the base file name, preserving the file extension (if
   * one exists).
   * For example, this could be passed a UUID string to uniquify files:
   * file1.snap -> file1_<uuid>.snap
   * file1 -> file1_<uuid>
   */
  private static String appendToBaseFileName(String baseFileName, String appendStr) {
    StringBuilder sb = new StringBuilder(baseFileName);
    // Insert the string to append, preserving the file extension.
    int extensionIdx = baseFileName.lastIndexOf('.');
    if (extensionIdx != -1) {
      sb.replace(extensionIdx, extensionIdx + 1, "_" + appendStr + ".");
    } else {
      sb.append("_" + appendStr);
    }
    return sb.toString();
  }

  /**
   * Returns true if the given Path contains any sub directories, otherwise false.
   */
  public static boolean containsSubdirectory(Path directory)
      throws FileNotFoundException, IOException {
    FileSystem fs = directory.getFileSystem(CONF);
    // Enumerate all the files in the source
    for (FileStatus fStatus: fs.listStatus(directory)) {
      if (fStatus.isDirectory()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Makes a temporary unique directory within the given directory.
   */
  public static Path makeTmpSubdirectory(Path directory) throws IOException {
    FileSystem fs = directory.getFileSystem(CONF);
    Path tmpDir = new Path(directory, ".tmp_" + UUID.randomUUID().toString());
    fs.mkdirs(tmpDir);
    return tmpDir;
  }

  public static boolean isHiddenFile(String fileName) {
    // Hidden files start with . or _
    return fileName.startsWith(".") || fileName.startsWith("_");
  }

  public static DistributedFileSystem getDistributedFileSystem(Path path)
      throws IOException {
    FileSystem fs = path.getFileSystem(CONF);
    Preconditions.checkState(fs instanceof DistributedFileSystem);
    return (DistributedFileSystem) fs;
  }

  /**
   * Fully-qualifies the given path based on the FileSystem configuration. If the given
   * path is already fully qualified, a new Path object with the same location will be
   * returned.
   */
  public static Path createFullyQualifiedPath(Path location) {
    return location.makeQualified(FileSystem.getDefaultUri(CONF), location);
  }

  /**
   * Returns the configuration.
   */
  public static Configuration getConfiguration() {
    return CONF;
  }
}
