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
import java.net.URI;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3.S3FileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
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
   * Returns true if path p1 and path p2 are in the same encryption zone.
   */
  private static boolean arePathsInSameEncryptionZone(FileSystem fs, Path p1,
      Path p2) throws IOException {
    HdfsAdmin hdfsAdmin = new HdfsAdmin(fs.getUri(), CONF);
    EncryptionZone z1 = hdfsAdmin.getEncryptionZoneForPath(p1);
    EncryptionZone z2 = hdfsAdmin.getEncryptionZoneForPath(p2);
    if (z1 == null && z2 == null) return true;
    if (z1 == null || z2 == null) return false;
    return z1.equals(z2);
  }

  /**
   * Relocates all visible (non-hidden) files from a source directory to a destination
   * directory. Files are moved (renamed) to the new location unless the source and
   * destination directories are in different encryption zones, in which case the files
   * are copied so that they are decrypted and/or encrypted. Naming conflicts are
   * resolved by appending a UUID to the base file name. Any sub-directories within the
   * source directory are skipped. Returns the number of files relocated as part of this
   * operation.
   */
  public static int relocateAllVisibleFiles(Path sourceDir, Path destDir)
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
      FileSystemUtil.relocateFile(fStatus.getPath(), destFile, false);
      ++numFilesMoved;
    }
    return numFilesMoved;
  }

  /**
   * Relocates the given file to a new location (either another directory or a
   * file. The file is moved (renamed) to the new location unless the source and
   * destination are in different encryption zones, in which case the file is copied
   * so that the file can be decrypted and/or encrypted. If renameIfAlreadyExists
   * is true, no error will be thrown if a file with the same name already exists in the
   * destination location. Instead, a UUID will be appended to the base file name,
   * preserving the the existing file extension. If renameIfAlreadyExists is false, an
   * IOException will be thrown if there is a file name conflict.
   */
  public static void relocateFile(Path sourceFile, Path dest,
      boolean renameIfAlreadyExists) throws IOException {
    FileSystem fs = dest.getFileSystem(CONF);
    // TODO: Handle moving between file systems
    Preconditions.checkArgument(isPathOnFileSystem(sourceFile, fs));

    Path destFile = fs.isDirectory(dest) ? new Path(dest, sourceFile.getName()) : dest;
    // If a file with the same name does not already exist in the destination location
    // then use the same file name. Otherwise, generate a unique file name.
    if (renameIfAlreadyExists && fs.exists(destFile)) {
      Path destDir = fs.isDirectory(dest) ? dest : dest.getParent();
      destFile = new Path(destDir,
          appendToBaseFileName(destFile.getName(), UUID.randomUUID().toString()));
    }

    if (arePathsInSameEncryptionZone(fs, sourceFile, destFile)) {
      LOG.debug(String.format(
          "Moving '%s' to '%s'", sourceFile.toString(), destFile.toString()));
      // Move (rename) the file.
      fs.rename(sourceFile, destFile);
    } else {
      // We must copy rather than move if the source and dest are in different encryption
      // zones. A move would return an error from the NN because a move is a metadata-only
      // operation and the files would not be encrypted/decrypted properly on the DNs.
      LOG.info(String.format(
          "Copying source '%s' to '%s' because HDFS encryption zones are different",
          sourceFile, destFile));
      FileUtil.copy(sourceFile.getFileSystem(CONF), sourceFile, fs, destFile,
          true, true, CONF);
    }
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
   * Returns true if the given Path contains any visible sub directories, otherwise false.
   */
  public static boolean containsVisibleSubdirectory(Path directory)
      throws FileNotFoundException, IOException {
    FileSystem fs = directory.getFileSystem(CONF);
    // Enumerate all the files in the source
    for (FileStatus fStatus: fs.listStatus(directory)) {
      String pathName = fStatus.getPath().getName();
      if (fStatus.isDirectory() && !isHiddenFile(pathName)) {
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
    // Hidden files start with '.' or '_'. The '.copying' suffix is used by some
    // filesystem utilities (e.g. hdfs put) as a temporary destination when copying
    // files. The '.tmp' suffix is Flume's default for temporary files.
    String lcFileName = fileName.toLowerCase();
    return lcFileName.startsWith(".") || lcFileName.startsWith("_") ||
        lcFileName.endsWith(".copying") || lcFileName.endsWith(".tmp");
  }

  /**
   * Returns true if the filesystem might override getFileBlockLocations().
   */
  public static boolean hasGetFileBlockLocations(FileSystem fs) {
    // Common case.
    if (isDistributedFileSystem(fs)) return true;
    // Blacklist FileSystems that are known to not implement getFileBlockLocations().
    return !(fs instanceof S3AFileSystem || fs instanceof NativeS3FileSystem ||
        fs instanceof S3FileSystem || fs instanceof LocalFileSystem);
  }

  /**
   * Returns true iff the filesystem is an instance of LocalFileSystem.
   */
  public static boolean isLocalFileSystem(FileSystem fs) {
    return fs instanceof LocalFileSystem;
  }

  /**
   * Return true iff path is on a local filesystem.
   */
  public static boolean isLocalFileSystem(Path path) throws IOException {
    return isLocalFileSystem(path.getFileSystem(CONF));
  }

  /**
   * Returns true iff the filesystem is a DistributedFileSystem.
   */
  public static boolean isDistributedFileSystem(FileSystem fs) {
    return fs instanceof DistributedFileSystem;
  }

  /**
   * Return true iff path is on a DFS filesystem.
   */
  public static boolean isDistributedFileSystem(Path path) throws IOException {
    return isDistributedFileSystem(path.getFileSystem(CONF));
  }

  public static DistributedFileSystem getDistributedFileSystem() throws IOException {
    Path path = new Path(FileSystem.getDefaultUri(CONF));
    FileSystem fs = path.getFileSystem(CONF);
    Preconditions.checkState(fs instanceof DistributedFileSystem);
    return (DistributedFileSystem) fs;
  }

  /**
   * Fully-qualifies the given path based on the FileSystem configuration.
   */
  public static Path createFullyQualifiedPath(Path location) {
    URI defaultUri = FileSystem.getDefaultUri(CONF);
    URI locationUri = location.toUri();
    // Use the default URI only if location has no scheme or it has the same scheme as
    // the default URI.  Otherwise, Path.makeQualified() will incorrectly use the
    // authority from the default URI even though the schemes don't match.  See HDFS-7031.
    if (locationUri.getScheme() == null ||
        locationUri.getScheme().equalsIgnoreCase(defaultUri.getScheme())) {
      return location.makeQualified(defaultUri, location);
    }
    // Already qualified (has scheme).
    return location;
  }

  /**
   * Return true iff the path is on the given filesystem.
   */
  public static boolean isPathOnFileSystem(Path path, FileSystem fs) {
    try {
      // Call makeQualified() for the side-effect of FileSystem.checkPath() which will
      // throw an exception if path is not on fs.
      fs.makeQualified(path);
      return true;
    } catch (IllegalArgumentException e) {
      // Path is not on fs.
      return false;
    }
  }

  /**
   * Copies the source file to a destination path on the local filesystem and returns true
   * if successful.
   */
   public static boolean copyToLocal(Path source, Path dest) {
     try {
       FileSystem fs = source.getFileSystem(CONF);
       fs.copyToLocalFile(source, dest);
     } catch (IOException e) {
       return false;
     }
     return true;
   }

  /**
   * Return true if the path can be reached, false for all other cases
   * File doesn't exist, cannot access the FileSystem, etc.
   */
  public static boolean isPathReachable(Path path, FileSystem fs, StringBuilder error_msg) {
    try {
      if (fs.exists(path)) {
        return true;
      } else {
        error_msg.append("Path does not exist.");
      }
    } catch (Exception e) {
      error_msg.append(e.getMessage());
    }
    return false;
  }

  /**
   * Returns true if the given path is a location which supports caching (e.g. HDFS).
   */
  public static boolean isPathCacheable(Path path) {
    try {
      return isDistributedFileSystem(path);
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Returns the configuration.
   */
  public static Configuration getConfiguration() {
    return CONF;
  }

  /**
   * Returns true iff the given location is on a filesystem that Impala can write to.
   */
  public static boolean isImpalaWritableFilesystem(String location)
      throws IOException {
    Path path = new Path(location);
    return (FileSystemUtil.isDistributedFileSystem(path) ||
        FileSystemUtil.isLocalFileSystem(path));
  }
}
