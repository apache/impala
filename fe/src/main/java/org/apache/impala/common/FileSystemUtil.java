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

package org.apache.impala.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.adl.AdlFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.impala.catalog.HdfsCompression;
import org.apache.impala.util.DebugUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;

/**
 * Common utility functions for operating on FileSystem objects.
 */
public class FileSystemUtil {

  private static final Configuration CONF = new Configuration();
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemUtil.class);

  public static final String SCHEME_ABFS = "abfs";
  public static final String SCHEME_ABFSS = "abfss";
  public static final String SCHEME_ADL = "adl";
  public static final String SCHEME_FILE = "file";
  public static final String SCHEME_HDFS = "hdfs";
  public static final String SCHEME_S3A = "s3a";
  public static final String SCHEME_O3FS = "o3fs";
  public static final String SCHEME_ALLUXIO = "alluxio";
  public static final String SCHEME_GCS = "gs";

  /**
   * Set containing all FileSystem scheme that known to supports storage UUIDs in
   * BlockLocation calls.
   */
  private static final Set<String> SCHEME_SUPPORT_STORAGE_IDS =
      ImmutableSet.<String>builder()
          .add(SCHEME_HDFS)
          .add(SCHEME_O3FS)
          .add(SCHEME_ALLUXIO)
          .build();

  /**
   * Set containing all FileSystem scheme that is writeable by Impala.
   */
  private static final Set<String> SCHEME_WRITEABLE_BY_IMPALA =
      ImmutableSet.<String>builder()
          .add(SCHEME_ABFS)
          .add(SCHEME_ABFSS)
          .add(SCHEME_ADL)
          .add(SCHEME_FILE)
          .add(SCHEME_HDFS)
          .add(SCHEME_S3A)
          .add(SCHEME_O3FS)
          .add(SCHEME_GCS)
          .build();

  /**
   * Set containing all FileSystem scheme that is supported as Impala default FileSystem.
   */
  private static final Set<String> SCHEME_SUPPORTED_AS_DEFAULT_FS =
      ImmutableSet.<String>builder()
          .add(SCHEME_ABFS)
          .add(SCHEME_ABFSS)
          .add(SCHEME_ADL)
          .add(SCHEME_HDFS)
          .add(SCHEME_S3A)
          .add(SCHEME_GCS)
          .build();

  /**
   * Set containing all FileSystem scheme that is valid as INPATH for LOAD DATA statement.
   */
  private static final Set<String> SCHEME_VALID_FOR_LOAD_INPATH =
      ImmutableSet.<String>builder()
          .add(SCHEME_ABFS)
          .add(SCHEME_ABFSS)
          .add(SCHEME_ADL)
          .add(SCHEME_HDFS)
          .add(SCHEME_S3A)
          .add(SCHEME_O3FS)
          .add(SCHEME_GCS)
          .build();

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
        if (LOG.isTraceEnabled()) LOG.trace("Removing: " + fStatus.getPath());
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
   * Returns true if path p1 and path p2 are in the same encryption zone in HDFS.
   * Returns false if they are in different encryption zones or if either of the paths
   * are not on HDFS.
   */
  private static boolean arePathsInSameHdfsEncryptionZone(FileSystem fs, Path p1,
      Path p2) throws IOException {
    // Only distributed file systems have encryption zones.
    if (!isDistributedFileSystem(p1) || !isDistributedFileSystem(p2)) return false;
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
    FileSystem destFs = destDir.getFileSystem(CONF);
    FileSystem sourceFs = sourceDir.getFileSystem(CONF);
    Preconditions.checkState(destFs.isDirectory(destDir));
    Preconditions.checkState(sourceFs.isDirectory(sourceDir));

    // Use the same UUID to resolve all file name conflicts. This helps mitigate problems
    // that might happen if there is a conflict moving a set of files that have
    // dependent file names. For example, foo.lzo and foo.lzo_index.
    UUID uuid = UUID.randomUUID();

    // Enumerate all the files in the source
    int numFilesMoved = 0;
    for (FileStatus fStatus: sourceFs.listStatus(sourceDir)) {
      if (fStatus.isDirectory()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Skipping copy of directory: " + fStatus.getPath());
        }
        continue;
      } else if (isHiddenFile(fStatus.getPath().getName())) {
        continue;
      }

      Path destFile = new Path(destDir, fStatus.getPath().getName());
      if (destFs.exists(destFile)) {
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
   * file in the same or different filesystem). The file is generally moved (renamed) to
   * the new location. However, the file is copied if the source and destination are in
   * different encryption zones so that the file can be decrypted and/or encrypted, or if
   * the source and destination are in different filesystems. If renameIfAlreadyExists is
   * true, no error will be thrown if a file with the same name already exists in the
   * destination location. Instead, a UUID will be appended to the base file name,
   * preserving the existing file extension. If renameIfAlreadyExists is false, an
   * IOException will be thrown if there is a file name conflict.
   */
  public static void relocateFile(Path sourceFile, Path dest,
      boolean renameIfAlreadyExists) throws IOException {
    FileSystem destFs = dest.getFileSystem(CONF);
    FileSystem sourceFs = sourceFile.getFileSystem(CONF);

    Path destFile =
        destFs.isDirectory(dest) ? new Path(dest, sourceFile.getName()) : dest;
    // If a file with the same name does not already exist in the destination location
    // then use the same file name. Otherwise, generate a unique file name.
    if (renameIfAlreadyExists && destFs.exists(destFile)) {
      Path destDir = destFs.isDirectory(dest) ? dest : dest.getParent();
      destFile = new Path(destDir,
          appendToBaseFileName(destFile.getName(), UUID.randomUUID().toString()));
    }
    boolean sameFileSystem = isPathOnFileSystem(sourceFile, destFs);
    boolean destIsDfs = isDistributedFileSystem(destFs);

    // If the source and the destination are on different file systems, or in different
    // encryption zones, files can't be moved from one location to the other and must be
    // copied instead.
    boolean sameEncryptionZone =
        arePathsInSameHdfsEncryptionZone(destFs, sourceFile, destFile);
    // We can do a rename if the src and dst are in the same encryption zone in the same
    // distributed filesystem.
    boolean doRename = destIsDfs && sameFileSystem && sameEncryptionZone;
    // Alternatively, we can do a rename if the src and dst are on the same
    // non-distributed filesystem.
    if (!doRename) doRename = !destIsDfs && sameFileSystem;
    if (doRename) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format(
            "Moving '%s' to '%s'", sourceFile.toString(), destFile.toString()));
      }
      // Move (rename) the file.
      destFs.rename(sourceFile, destFile);
      return;
    }
    if (destIsDfs && sameFileSystem) {
      Preconditions.checkState(!doRename);
      // We must copy rather than move if the source and dest are in different
      // encryption zones. A move would return an error from the NN because a move is a
      // metadata-only operation and the files would not be encrypted/decrypted properly
      // on the DNs.
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format(
            "Copying source '%s' to '%s' because HDFS encryption zones are different.",
            sourceFile, destFile));
      }
    } else {
      Preconditions.checkState(!sameFileSystem);
      if (LOG.isTraceEnabled()) {
        LOG.trace(String.format("Copying '%s' to '%s' between filesystems.",
            sourceFile, destFile));
      }
    }
    FileUtil.copy(sourceFs, sourceFile, destFs, destFile, true, true, CONF);
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
   * Returns true if the file corresponding to 'fileStatus' is a valid data file as
   * per Impala's partitioning rules. A fileStatus is considered invalid if its a
   * directory/hidden file/LZO index file. LZO index files are skipped because they are
   * read by the scanner directly. Currently Impala doesn't allow subdirectories in the
   * partition paths.
   */
  public static boolean isValidDataFile(FileStatus fileStatus) {
    String fileName = fileStatus.getPath().getName();
    return !(fileStatus.isDirectory() || FileSystemUtil.isHiddenFile(fileName) ||
        HdfsCompression.fromFileName(fileName) == HdfsCompression.LZO_INDEX);
  }

  /**
   * Returns true if the filesystem supports storage UUIDs in BlockLocation calls.
   */
  public static boolean supportsStorageIds(FileSystem fs) {
    return SCHEME_SUPPORT_STORAGE_IDS.contains(fs.getScheme());
  }

  /**
   * Returns true iff the filesystem is a S3AFileSystem.
   */
  public static boolean isS3AFileSystem(FileSystem fs) {
    return hasScheme(fs, SCHEME_S3A);
  }

  /**
   * Returns true iff the path is on a S3AFileSystem.
   */
  public static boolean isS3AFileSystem(Path path) throws IOException {
    return hasScheme(path, SCHEME_S3A);
  }

  /**
   * Returns true iff the filesystem is a GoogleHadoopFileSystem.
   */
  public static boolean isGCSFileSystem(FileSystem fs) {
    return hasScheme(fs, SCHEME_GCS);
  }

  /**
   * Returns true iff the filesystem is AdlFileSystem.
   */
  public static boolean isADLFileSystem(FileSystem fs) {
    return hasScheme(fs, SCHEME_ADL);
  }

  /**
   * Returns true iff the path is on AdlFileSystem.
   */
  public static boolean isADLFileSystem(Path path) throws IOException {
    return hasScheme(path, SCHEME_ADL);
  }

  /**
   * Returns true iff the filesystem is AzureBlobFileSystem or
   * SecureAzureBlobFileSystem. This function is unique in that there are 2
   * distinct classes it checks for, but the ony functional difference is the
   * use of wire encryption. Some features like OAuth authentication do require
   * wire encryption but that does not matter in usages of this function.
   */
  public static boolean isABFSFileSystem(FileSystem fs) {
    return hasScheme(fs, SCHEME_ABFS) || hasScheme(fs, SCHEME_ABFSS);
  }

  /**
   * Returns true iff the path is on AzureBlobFileSystem or
   * SecureAzureBlobFileSystem.
   */
  public static boolean isABFSFileSystem(Path path) throws IOException {
    return hasScheme(path, SCHEME_ABFS) || hasScheme(path, SCHEME_ABFSS);
  }

  /**
   * Returns true iff the filesystem is an instance of LocalFileSystem.
   */
  public static boolean isLocalFileSystem(FileSystem fs) {
    return hasScheme(fs, SCHEME_FILE);
  }

  /**
   * Return true iff path is on a local filesystem.
   */
  public static boolean isLocalFileSystem(Path path) throws IOException {
    return hasScheme(path, SCHEME_FILE);
  }

  /**
   * Returns true iff the filesystem is a DistributedFileSystem.
   */
  public static boolean isDistributedFileSystem(FileSystem fs) {
    return hasScheme(fs, SCHEME_HDFS);
  }

  /**
   * Return true iff path is on a DFS filesystem.
   */
  public static boolean isDistributedFileSystem(Path path) throws IOException {
    return hasScheme(path, SCHEME_HDFS);
  }

  /**
   * Returns true iff the filesystem is a OzoneFileSystem.
   */
  public static boolean isOzoneFileSystem(FileSystem fs) {
    return hasScheme(fs, SCHEME_O3FS);
  }

  /**
   * Returns true iff the path is on OzoneFileSystem.
   */
  public static boolean isOzoneFileSystem(Path path) throws IOException {
    return hasScheme(path, SCHEME_O3FS);
  }

  /**
   * Returns true if the filesystem protocol match the scheme.
   */
  private static boolean hasScheme(FileSystem fs, String scheme) {
    return scheme.equals(fs.getScheme());
  }

  /**
   * Returns true if the given path match the scheme.
   */
  private static boolean hasScheme(Path path, String scheme) {
    return scheme.equals(path.toUri().getScheme());
  }

  /**
   * Represents the type of filesystem being used. Typically associated with a
   * {@link org.apache.hadoop.fs.FileSystem} instance that is used to read data.
   *
   * <p>
   *   Unlike the {@code is*FileSystem} methods above. A FsType is more
   *   generic in that it is capable of grouping different filesystems to the
   *   same type. For example, the FsType {@link FsType#ADLS} maps to
   *   multiple filesystems: {@link AdlFileSystem},
   *   {@link AzureBlobFileSystem}, and {@link SecureAzureBlobFileSystem}.
   * </p>
   */
  public enum FsType {
    ADLS,
    HDFS,
    LOCAL,
    S3,
    OZONE,
    ALLUXIO,
    GCS;

    private static final Map<String, FsType> SCHEME_TO_FS_MAPPING =
        ImmutableMap.<String, FsType>builder()
            .put(SCHEME_ABFS, ADLS)
            .put(SCHEME_ABFSS, ADLS)
            .put(SCHEME_ADL, ADLS)
            .put(SCHEME_FILE, LOCAL)
            .put(SCHEME_HDFS, HDFS)
            .put(SCHEME_S3A, S3)
            .put(SCHEME_O3FS, OZONE)
            .put(SCHEME_ALLUXIO, ALLUXIO)
            .put(SCHEME_GCS, GCS)
            .build();

    /**
     * Provides a mapping between filesystem schemes and filesystems types. This can be
     * useful as there are often multiple filesystem connectors for a give fs, each
     * with its own scheme (e.g. abfs, abfss, adl are all ADLS connectors).
     * Returns the {@link FsType} associated with a given filesystem scheme (e.g. local,
     * hdfs, s3a, etc.)
     */
    public static FsType getFsType(String scheme) {
      return SCHEME_TO_FS_MAPPING.get(scheme);
    }
  }

  public static FileSystem getDefaultFileSystem() throws IOException {
    Path path = new Path(FileSystem.getDefaultUri(CONF));
    FileSystem fs = path.getFileSystem(CONF);
    return fs;
  }

  /**
   * Returns the FileSystem object for a given path using the cached config.
   */
  public static FileSystem getFileSystemForPath(Path p) throws IOException {
    return p.getFileSystem(CONF);
  }

  public static DistributedFileSystem getDistributedFileSystem() throws IOException {
    FileSystem fs = getDefaultFileSystem();
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
   * Copies the source file to a destination path on the local filesystem.
   * Throws IOException on failure.
   */
  public static void copyToLocal(Path source, Path dest) throws IOException {
    FileSystem fs = source.getFileSystem(CONF);
    fs.copyToLocalFile(source, dest);
  }

  /**
   * Delete the file at 'path' if it exists.
   */
  public static void deleteIfExists(Path path) {
    try {
      FileSystem fs = path.getFileSystem(CONF);
      if (!fs.exists(path)) return;
      fs.delete(path);
    } catch (IOException e) {
      LOG.warn("Encountered an exception deleting file at path " + path.toString(), e);
    }
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
    String scheme = path.toUri().getScheme();
    return SCHEME_WRITEABLE_BY_IMPALA.contains(scheme);
  }

  /**
   * Returns true iff the given filesystem is supported as Impala default filesystem.
   */
  public static boolean isValidDefaultFileSystem(FileSystem fs) {
    return SCHEME_SUPPORTED_AS_DEFAULT_FS.contains(fs.getScheme());
  }

  /**
   * Returns true iff the given filesystem is valid as INPATH for LOAD DATA statement.
   */
  public static boolean isValidLoadDataInpath(FileSystem fs) {
    return SCHEME_VALID_FOR_LOAD_INPATH.contains(fs.getScheme());
  }

  /**
   * Return list of FileSystem protocol scheme that is valid as INPATH for LOAD DATA
   * statement, delimited by comma.
   */
  public static String getValidLoadDataInpathSchemes() {
    return String.join(", ", SCHEME_VALID_FOR_LOAD_INPATH);
  }

  /**
   * Wrapper around FileSystem.listStatus() that specifically handles the case when
   * the path does not exist. This helps simplify the caller code in cases where
   * the file does not exist and also saves an RPC as the caller need not do a separate
   * exists check for the path. Returns null if the path does not exist.
   *
   * If 'recursive' is true, all underlying files and directories will be yielded.
   * Note that the order (breadth-first vs depth-first, sorted vs not) is undefined.
   */
  public static RemoteIterator<? extends FileStatus> listStatus(FileSystem fs, Path p,
      boolean recursive, String debugAction) throws IOException {
    try {
      if (recursive) {
        // The Hadoop FileSystem API doesn't provide a recursive listStatus call that
        // doesn't also fetch block locations, and fetching block locations is expensive.
        // Here, our caller specifically doesn't need block locations, so we don't want to
        // call the expensive 'listFiles' call on HDFS. Instead, we need to "manually"
        // recursively call FileSystem.listStatusIterator().
        //
        // Note that this "manual" recursion is not actually any slower than the recursion
        // provided by the HDFS 'listFiles(recursive=true)' API, since the HDFS wire
        // protocol doesn't provide any such recursive support anyway. In other words,
        // the API that looks like a single recursive call is just as bad as what we're
        // doing here.
        //
        // However, S3 actually implements 'listFiles(recursive=true)' with a faster path
        // which natively recurses. In that case, it's quite preferable to use 'listFiles'
        // even though it returns LocatedFileStatus objects with "fake" blocks which we
        // will ignore.
        if (isS3AFileSystem(fs)) {
          return listFiles(fs, p, true, debugAction);
        }
        DebugUtils.executeDebugAction(debugAction, DebugUtils.REFRESH_HDFS_LISTING_DELAY);
        return new FilterIterator(p, new RecursingIterator(fs, p));
      }
      DebugUtils.executeDebugAction(debugAction, DebugUtils.REFRESH_HDFS_LISTING_DELAY);
      return new FilterIterator(p, listStatusIterator(fs, p));
    } catch (FileNotFoundException e) {
      if (LOG.isWarnEnabled()) LOG.warn("Path does not exist: " + p.toString(), e);
      return null;
    }
  }

  /**
   * Wrapper around FileSystem.listFiles(), similar to the listStatus() wrapper above.
   */
  public static RemoteIterator<? extends FileStatus> listFiles(FileSystem fs, Path p,
      boolean recursive, String debugAction) throws IOException {
    try {
      DebugUtils.executeDebugAction(debugAction, DebugUtils.REFRESH_HDFS_LISTING_DELAY);
      return new FilterIterator(p, fs.listFiles(p, recursive));
    } catch (FileNotFoundException e) {
      if (LOG.isWarnEnabled()) LOG.warn("Path does not exist: " + p.toString(), e);
      return null;
    }
  }

  /**
   * Wrapper around FileSystem.listStatusIterator() to make sure the path exists.
   *
   * @throws FileNotFoundException if <code>p</code> does not exist
   * @throws IOException if any I/O error occurredd
   */
  public static RemoteIterator<FileStatus> listStatusIterator(FileSystem fs, Path p)
      throws IOException {
    RemoteIterator<FileStatus> iterator = fs.listStatusIterator(p);
    // Some FileSystem implementations like GoogleHadoopFileSystem doesn't check
    // existence of the start path when creating the RemoteIterator. Instead, their
    // iterators throw the FileNotFoundException in the first call of hasNext() when
    // the start path doesn't exist. Here we call hasNext() to ensure start path exists.
    iterator.hasNext();
    return iterator;
  }

  /**
   * Returns true if the path 'p' is a directory, false otherwise.
   */
  public static boolean isDir(Path p) throws IOException {
    FileSystem fs = getFileSystemForPath(p);
    return fs.isDirectory(p);
  }

  /**
   * Returns true if the path 'p' is a file, false if not. Throws if path does not exist.
   */
  public static boolean isFile(Path p) throws IOException, FileNotFoundException {
    FileSystem fs = getFileSystemForPath(p);
    return fs.getFileStatus(p).isFile();
  }

  /**
   * Return the path of 'path' relative to the startPath. This may
   * differ from simply the file name in the case of recursive listings.
   */
  public static String relativizePath(Path path, Path startPath) {
    URI relUri = startPath.toUri().relativize(path.toUri());
    if (relUri.isAbsolute() || relUri.getPath().startsWith("/")) {
      throw new RuntimeException("FileSystem returned an unexpected path " +
          path + " for a file within " + startPath);
    }
    return relUri.getPath();
  }

  /**
   * Util method to check if the given file status relative to its parent is contained
   * in a ignored directory. This is useful to ignore the files which seemingly are valid
   * just by themselves but should still be ignored if they are contained in a
   * directory which needs to be ignored
   *
   * @return true if the fileStatus should be ignored, false otherwise
   */
  @VisibleForTesting
  static boolean isInIgnoredDirectory(Path parent, FileStatus fileStatus) {
    Preconditions.checkNotNull(fileStatus);
    Path currentPath = fileStatus.isDirectory() ? fileStatus.getPath() :
        fileStatus.getPath().getParent();
    while (currentPath != null && !currentPath.equals(parent)) {
      if (isIgnoredDir(currentPath)) {
        LOG.debug("Ignoring {} since it is either in a hidden directory or a temporary "
                + "staging directory {}", fileStatus.getPath(), currentPath);
        return true;
      }
      currentPath = currentPath.getParent();
    }
    return false;
  }

  /**
   * Prefix string used by hive to write certain temporary or "non-data" files in the
   * table location
   */
  public static final String HIVE_TEMP_FILE_PREFIX = "_tmp.";

  public static final String DOT = ".";

  /**
   * Util method used to filter out hidden and temporary staging directories
   * which tools like Hive create in the table/partition directories when a query is
   * inserting data into them.
   */
  @VisibleForTesting
  static boolean isIgnoredDir(Path path) {
    String filename = path.getName();
    return filename.startsWith(DOT) || filename.startsWith(HIVE_TEMP_FILE_PREFIX);
  }

  /**
   * A remote iterator which takes in another Remote Iterator and a start path and filters
   * all the ignored directories
   * (See {@link org.apache.impala.common.FileSystemUtil#isInIgnoredDirectory}) from the
   * listing of the remote iterator
   */
  static class FilterIterator implements RemoteIterator<FileStatus> {
    private final RemoteIterator<? extends FileStatus> baseIterator_;
    private FileStatus curFile_ = null;
    private final Path startPath_;

    FilterIterator(Path startPath, RemoteIterator<? extends FileStatus> baseIterator) {
      startPath_ = Preconditions.checkNotNull(startPath);
      baseIterator_ = Preconditions.checkNotNull(baseIterator);
    }

    @Override
    public boolean hasNext() throws IOException {
      // Pull the next file to be returned into 'curFile'. If we've already got one,
      // we don't need to do anything (extra calls to hasNext() must not affect
      // state)
      while (curFile_ == null) {
        FileStatus next;
        try {
          if (!baseIterator_.hasNext()) return false;
          // if the next fileStatus is in ignored directory skip it
           next = baseIterator_.next();
        } catch (FileNotFoundException ex) {
          // in case of concurrent operations by multiple engines it is possible that
          // some temporary files are deleted while Impala is loading the table. For
          // instance, hive deletes the temporary files in the .hive-staging directory
          // after an insert query from Hive completes. If we are loading the table at
          // the same time, we may get a FileNotFoundException which is safe to ignore.
          LOG.warn(ex.getMessage());
          continue;
        }
        if (!isInIgnoredDirectory(startPath_, next)) {
          curFile_ = next;
          return true;
        }
      }
      return true;
    }

    @Override
    public FileStatus next() throws IOException {
      if (hasNext()) {
        FileStatus next = curFile_;
        curFile_ = null;
        return next;
      }
      throw new NoSuchElementException("No more entries");
    }
  }

  /**
   * Iterator which recursively visits directories on a FileSystem, yielding
   * files in an unspecified order.
   */
  private static class RecursingIterator implements RemoteIterator<FileStatus> {
    private final FileSystem fs_;
    private final Stack<RemoteIterator<FileStatus>> iters_ = new Stack<>();
    private RemoteIterator<FileStatus> curIter_;
    private FileStatus curFile_;

    private RecursingIterator(FileSystem fs, Path startPath) throws IOException {
      this.fs_ = Preconditions.checkNotNull(fs);
      curIter_ = listStatusIterator(fs, Preconditions.checkNotNull(startPath));
    }

    @Override
    public boolean hasNext() throws IOException {
      // Pull the next file to be returned into 'curFile'. If we've already got one,
      // we don't need to do anything (extra calls to hasNext() must not affect
      // state)
      while (curFile_ == null) {
        if (curIter_.hasNext()) {
          // Consume the next file or directory from the current iterator.
          handleFileStat(curIter_.next());
        } else if (!iters_.empty()) {
          // We ran out of entries in the current one, but we might still have
          // entries at a higher level of recursion.
          curIter_ = iters_.pop();
        } else {
          // No iterators left to process, so we are entirely done.
          return false;
        }
      }
      return true;
    }

    /**
     * Process the input stat.
     * If it is a file, return the file stat.
     * If it is a directory, traverse the directory if recursive is true;
     * ignore it if recursive is false.
     * @param fileStatus input status
     * @throws IOException if any IO error occurs
     */
    private void handleFileStat(FileStatus fileStatus) throws IOException {
      if (fileStatus.isFile()) {
        curFile_ = fileStatus;
        return;
      }
      RemoteIterator<FileStatus> iter = listStatusIterator(fs_, fileStatus.getPath());
      iters_.push(curIter_);
      curIter_ = iter;
      curFile_ = fileStatus;
    }

    @Override
    public FileStatus next() throws IOException {
      if (hasNext()) {
        FileStatus result = curFile_;
        // Reset back to 'null' so that hasNext() will pull a new entry on the next
        // call.
        curFile_ = null;
        return result;
      }
      throw new NoSuchElementException("No more entries");
    }
  }
}
