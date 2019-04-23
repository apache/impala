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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.adl.AdlFileSystem;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.impala.catalog.HdfsCompression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.UUID;

/**
 * Common utility functions for operating on FileSystem objects.
 */
public class FileSystemUtil {
  private static final Configuration CONF = new Configuration();
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemUtil.class);

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
    // Common case.
    if (isDistributedFileSystem(fs)) return true;
    // Blacklist FileSystems that are known to not to include storage UUIDs.
    return !(fs instanceof S3AFileSystem || fs instanceof LocalFileSystem ||
        fs instanceof AzureBlobFileSystem || fs instanceof SecureAzureBlobFileSystem ||
        fs instanceof AdlFileSystem);
  }

  /**
   * Returns true iff the filesystem is a S3AFileSystem.
   */
  public static boolean isS3AFileSystem(FileSystem fs) {
    return fs instanceof S3AFileSystem;
  }

  /**
   * Returns true iff the path is on a S3AFileSystem.
   */
  public static boolean isS3AFileSystem(Path path) throws IOException {
    return isS3AFileSystem(path.getFileSystem(CONF));
  }

  /**
   * Returns true iff the filesystem is AdlFileSystem.
   */
  public static boolean isADLFileSystem(FileSystem fs) {
    return fs instanceof AdlFileSystem;
  }

  /**
   * Returns true iff the path is on AdlFileSystem.
   */
  public static boolean isADLFileSystem(Path path) throws IOException {
    return isADLFileSystem(path.getFileSystem(CONF));
  }

  /**
   * Returns true iff the filesystem is AzureBlobFileSystem or
   * SecureAzureBlobFileSystem. This function is unique in that there are 2
   * distinct classes it checks for, but the ony functional difference is the
   * use of wire encryption. Some features like OAuth authentication do require
   * wire encryption but that does not matter in usages of this function.
   */
  public static boolean isABFSFileSystem(FileSystem fs) {
    return fs instanceof AzureBlobFileSystem
        || fs instanceof SecureAzureBlobFileSystem;
  }

  /**
   * Returns true iff the path is on AzureBlobFileSystem or
   * SecureAzureBlobFileSystem.
   */
  public static boolean isABFSFileSystem(Path path) throws IOException {
    return isABFSFileSystem(path.getFileSystem(CONF));
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
    S3;

    private static final Map<String, FsType> SCHEME_TO_FS_MAPPING =
        ImmutableMap.<String, FsType>builder()
            .put("abfs", ADLS)
            .put("abfss", ADLS)
            .put("adl", ADLS)
            .put("file", LOCAL)
            .put("hdfs", HDFS)
            .put("s3a", S3)
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
    return (FileSystemUtil.isDistributedFileSystem(path) ||
        FileSystemUtil.isLocalFileSystem(path) ||
        FileSystemUtil.isS3AFileSystem(path) ||
        FileSystemUtil.isABFSFileSystem(path) ||
        FileSystemUtil.isADLFileSystem(path));
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
      boolean recursive) throws IOException {
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
          return listFiles(fs, p, recursive);
        }

        return new RecursingIterator(fs, p);
      }

      return fs.listStatusIterator(p);
    } catch (FileNotFoundException e) {
      if (LOG.isWarnEnabled()) LOG.warn("Path does not exist: " + p.toString(), e);
      return null;
    }
  }

  /**
   * Wrapper around FileSystem.listFiles(), similar to the listStatus() wrapper above.
   */
  public static RemoteIterator<LocatedFileStatus> listFiles(FileSystem fs, Path p,
      boolean recursive) throws IOException {
    try {
      return fs.listFiles(p, recursive);
    } catch (FileNotFoundException e) {
      if (LOG.isWarnEnabled()) LOG.warn("Path does not exist: " + p.toString(), e);
      return null;
    }
  }

  /**
   * Returns true if the path 'p' is a directory, false otherwise.
   */
  public static boolean isDir(Path p) throws IOException {
    FileSystem fs = getFileSystemForPath(p);
    return fs.isDirectory(p);
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
   * Iterator which recursively visits directories on a FileSystem, yielding
   * files in an unspecified order.
   */
  static class RecursingIterator implements RemoteIterator<FileStatus> {
    private final FileSystem fs_;
    private final Stack<RemoteIterator<FileStatus>> iters_ = new Stack<>();
    private RemoteIterator<FileStatus> curIter_;
    private FileStatus curFile_;

    RecursingIterator(FileSystem fs, Path startPath) throws IOException {
      this.fs_ = Preconditions.checkNotNull(fs);
      curIter_ = fs.listStatusIterator(Preconditions.checkNotNull(startPath));
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
      iters_.push(curIter_);
      curIter_ = fs_.listStatusIterator(fileStatus.getPath());
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
