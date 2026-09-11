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

package org.apache.impala.catalog.iceberg;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.DelegatingOutputStream;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.impala.common.Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link HadoopFileIO} wrapper that opens every file with the storage credential the
 * REST catalog vended for it. Configured as the catalog's 'io-impl' when credential
 * vending is enabled; Iceberg's RESTCatalog then instantiates it per loaded table and
 * hands it the table's credentials through {@link SupportsStorageCredentials}.
 *
 * Hadoop's FileSystem cache is keyed by scheme, authority and user, so a per-table
 * credential cannot be applied to the shared per-bucket FileSystem. Instead, each open
 * resolves the credential whose prefix covers the path and uses a FileSystem instance
 * created for that credential, with the credential's Hadoop-translated config
 * ({@link Credential#toHadoopConfig}) layered on top of the base configuration. Those
 * instances are cached process-wide by bucket and credential identity, so all tables
 * and loads that share a credential share one instance and a rotated credential gets a
 * new one. When the cache is next written to, instances whose credential has expired a
 * while ago are closed, and so are instances that have no open stream and have not been
 * used for a while: this covers credentials without an expiry (e.g. static keys), whose
 * instance would otherwise live forever even after a rotation superseded it. To make
 * the open-stream accounting possible, the returned InputFiles and OutputFiles resolve
 * the FileSystem per operation and wrap the streams they open. Paths not covered by any
 * credential fall through to a plain HadoopFileIO.
 *
 * Unlike HadoopFileIO, this class does not implement the bulk and prefix operations
 * (SupportsBulkOperations, SupportsPrefixOperations): a FileIO that has them must apply
 * the credentials to them too, and passing them on to the plain HadoopFileIO would not.
 * Iceberg checks for these interfaces and uses the single-file operations instead, e.g.
 * it deletes the manifests of a failed commit one by one through deleteFile(), with the
 * credential vended for them.
 */
public class VendedCredentialsFileIO implements FileIO, HadoopConfigurable,
    SupportsStorageCredentials {
  private static final Logger LOG =
      LoggerFactory.getLogger(VendedCredentialsFileIO.class);

  /** How long after expiry a cached FileSystem is kept before it is closed. */
  private static final long EXPIRED_FS_GRACE_MS = 10L * 60 * 1000;

  /**
   * How long a cached FileSystem may go unused before it is closed, regardless of its
   * credential's expiry. Bounds the lifetime of instances created for credentials that
   * never expire, and of instances superseded by a rotated credential.
   */
  private static final long IDLE_FS_TIMEOUT_MS = 60L * 60 * 1000;

  /**
   * A FileSystem created for one credential. 'lastUsedMs_' and 'openStreams_' are only
   * read and written inside FS_CACHE compute callbacks for 'key', which serialize them
   * per key; this is what makes a lookup's touch atomic with removeStale()'s staleness
   * check and removal.
   */
  private static final class CachedFileSystem {
    final String key;
    final FileSystem fs;
    final Configuration conf;
    final long expiryMs; // 0 = never
    private long lastUsedMs_;
    // Streams opened on 'fs' and not closed yet.
    private int openStreams_;

    CachedFileSystem(String key, FileSystem fs, Configuration conf, long expiryMs,
        long nowMs) {
      this.key = key;
      this.fs = fs;
      this.conf = conf;
      this.expiryMs = expiryMs;
      lastUsedMs_ = nowMs;
    }

    void use(long nowMs, boolean openStream) {
      lastUsedMs_ = nowMs;
      if (openStream) ++openStreams_;
    }

    void closeStream(long nowMs) {
      lastUsedMs_ = nowMs;
      if (openStreams_ > 0) --openStreams_;
    }

    /**
     * True when the instance should be closed: see the class comment. An instance with
     * open streams is not idle, however long ago it was last opened; once its
     * credential has expired it is closed regardless, as the streams cannot make further
     * requests with it anyway.
     */
    boolean isStale(long nowMs) {
      if (expiryMs != 0 && expiryMs + EXPIRED_FS_GRACE_MS < nowMs) return true;
      return openStreams_ == 0 && lastUsedMs_ + IDLE_FS_TIMEOUT_MS < nowMs;
    }
  }

  /** FileSystems created for a credential, keyed by "scheme://authority\0identity". */
  private static final ConcurrentHashMap<String, CachedFileSystem> FS_CACHE =
      new ConcurrentHashMap<>();

  private SerializableSupplier<Configuration> conf_;
  private Map<String, String> properties_ = Collections.emptyMap();
  private List<StorageCredential> storageCredentials_ = Collections.emptyList();
  // The usable (Hadoop-translatable) credentials, longest prefix first.
  private List<Credential> credentials_ = Collections.emptyList();
  private volatile HadoopFileIO delegate_;

  @Override
  public void initialize(Map<String, String> properties) {
    properties_ = properties;
    delegate_ = null;
  }

  @Override
  public Map<String, String> properties() {
    return properties_;
  }

  @Override
  public void setConf(Configuration conf) {
    conf_ = new SerializableConfiguration(conf);
    delegate_ = null;
  }

  @Override
  public Configuration getConf() {
    return conf_ == null ? null : conf_.get();
  }

  @Override
  public void serializeConfWith(
      Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
    conf_ = confSerializer.apply(getConf());
  }

  @Override
  public void setCredentials(List<StorageCredential> credentials) {
    storageCredentials_ = credentials == null
        ? Collections.emptyList() : new ArrayList<>(credentials);
    List<Credential> usable = new ArrayList<>(storageCredentials_.size());
    for (StorageCredential cred : storageCredentials_) {
      Credential c = new Credential(cred.prefix(), cred.config());
      if (c.toHadoopConfig().isEmpty()) {
        LOG.warn("Ignoring vended credential for {}: unsupported storage scheme.",
            cred.prefix());
        continue;
      }
      usable.add(c);
    }
    // Longest prefix first so the first covering prefix wins.
    usable.sort(
        (a, b) -> Integer.compare(b.getPrefix().length(), a.getPrefix().length()));
    credentials_ = usable;
  }

  @Override
  public List<StorageCredential> credentials() {
    return storageCredentials_;
  }

  @Override
  public InputFile newInputFile(String path) {
    Credential cred = credentialFor(path);
    if (cred == null) return delegate().newInputFile(path);
    return new CredentialedInputFile(cred, path, -1);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    Credential cred = credentialFor(path);
    if (cred == null) return delegate().newInputFile(path, length);
    return new CredentialedInputFile(cred, path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    Credential cred = credentialFor(path);
    if (cred == null) return delegate().newOutputFile(path);
    return new CredentialedOutputFile(cred, path);
  }

  @Override
  public void deleteFile(String path) {
    Credential cred = credentialFor(path);
    if (cred == null) {
      delegate().deleteFile(path);
      return;
    }
    Path p = new Path(path);
    try {
      if (!fileSystem(cred, path, false).fs.delete(p, false)) {
        throw new RuntimeIOException("Failed to delete file: " + path);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to delete file: %s", path);
    }
  }

  @Override
  public void close() {
    // Credentialed FileSystems are shared through FS_CACHE and closed once stale (see
    // removeStale()).
  }

  /**
   * Returns the FileSystem to use for 'path': one created for the vended credential
   * covering it, or Hadoop's cached instance for the path when none does. Lets callers
   * that read table files outside of Iceberg (e.g. file listing) use the same
   * credentials as the FileIO. The returned instance only counts as used at the time of
   * this call, so callers should call this per operation rather than hold on to the
   * instance for longer than the idle timeout.
   */
  public FileSystem fileSystemFor(Path path) throws IOException {
    Credential cred = credentialFor(path.toString());
    if (cred == null) return path.getFileSystem(getConf());
    return fileSystem(cred, path.toString(), false).fs;
  }

  /** The credential with the longest prefix covering 'path', or null. */
  private Credential credentialFor(String path) {
    for (Credential cred : credentials_) {
      if (prefixCoversPath(cred.getPrefix(), path)) return cred;
    }
    return null;
  }

  /**
   * True when 'path' starts with 'prefix' and the match ends on a path-component
   * boundary, so "s3a://b/db/tbl" covers "s3a://b/db/tbl/f" but not "s3a://b/db/tbl2/f".
   */
  static boolean prefixCoversPath(String prefix, String path) {
    if (prefix.isEmpty() || !path.startsWith(prefix)) return false;
    return path.length() == prefix.length() || prefix.endsWith("/")
        || path.charAt(prefix.length()) == '/';
  }

  private HadoopFileIO delegate() {
    HadoopFileIO io = delegate_;
    if (io == null) {
      synchronized (this) {
        io = delegate_;
        if (io == null) {
          io = new HadoopFileIO(conf_);
          io.initialize(properties_);
          delegate_ = io;
        }
      }
    }
    return io;
  }

  /**
   * Returns the cached FileSystem for 'cred' and the bucket of 'path', creating it if
   * needed, and marks it as used. With 'openStream' the caller is about to open a stream
   * on it and must call closeStream() once that stream is closed (or failed to open).
   */
  private CachedFileSystem fileSystem(Credential cred, String path, boolean openStream) {
    URI uri = new Path(path).toUri();
    String key = uri.getScheme() + "://" + uri.getAuthority() + '\0' + cred.identity();
    long now = System.currentTimeMillis();
    CachedFileSystem cached = lookup(key, now, openStream);
    if (cached != null) return cached;
    // Dropped instances are closed outside of the monitor, as closing may block on I/O.
    List<CachedFileSystem> stale = Collections.emptyList();
    try {
      synchronized (FS_CACHE) {
        cached = lookup(key, now, openStream);
        if (cached != null) return cached;
        stale = removeStale(now);
        Configuration conf = new Configuration(getConf());
        for (Map.Entry<String, String> kv : cred.toHadoopConfig().entrySet()) {
          conf.set(kv.getKey(), kv.getValue());
        }
        try {
          // newInstance() bypasses Hadoop's FileSystem cache, so the credential in
          // 'conf' is honoured rather than the configuration of whichever instance was
          // created first for this bucket.
          FileSystem fs = FileSystem.newInstance(
              URI.create(uri.getScheme() + "://" + uri.getAuthority() + "/"), conf);
          cached = new CachedFileSystem(key, fs, conf, cred.getExpiryMs(), now);
        } catch (IOException e) {
          throw new RuntimeIOException(e, "Failed to create a FileSystem for %s with "
              + "the credential vended for %s", uri.getAuthority(), cred.getPrefix());
        }
        cached.use(now, openStream);
        FS_CACHE.put(key, cached);
        LOG.debug("Created FileSystem for {} with the credential vended for {} "
            + "(expiry {})", uri.getAuthority(), cred.getPrefix(), cred.getExpiryMs());
        return cached;
      }
    } finally {
      closeAll(stale);
    }
  }

  /**
   * Returns the cached FileSystem for 'key' after marking it as used, atomically with
   * respect to removeStale(), or null if there is none.
   */
  private static CachedFileSystem lookup(String key, long now, boolean openStream) {
    return FS_CACHE.computeIfPresent(key, (k, cfs) -> {
      cfs.use(now, openStream);
      return cfs;
    });
  }

  /** Records that a stream opened on 'cfs' through fileSystem() was closed. */
  private static void closeStream(CachedFileSystem cfs) {
    long now = System.currentTimeMillis();
    FS_CACHE.computeIfPresent(cfs.key, (k, cur) -> {
      if (cur == cfs) cur.closeStream(now);
      return cur;
    });
  }

  /**
   * Drops cached FileSystems whose credential expired a while ago or that have been
   * idle for IDLE_FS_TIMEOUT_MS, and returns them so the caller can close them. The
   * check and the removal are atomic per key, so an instance a concurrent lookup() just
   * handed out is never dropped as idle.
   */
  private static List<CachedFileSystem> removeStale(long now) {
    List<CachedFileSystem> stale = new ArrayList<>();
    for (String key : FS_CACHE.keySet()) {
      FS_CACHE.computeIfPresent(key, (k, cfs) -> {
        if (!cfs.isStale(now)) return cfs;
        stale.add(cfs);
        return null;
      });
    }
    return stale;
  }

  private static void closeAll(List<CachedFileSystem> stale) {
    for (CachedFileSystem cfs : stale) {
      try {
        cfs.fs.close();
      } catch (IOException e) {
        LOG.warn("Failed to close FileSystem of a stale vended credential", e);
      }
    }
  }

  /**
   * An InputFile for a path covered by a vended credential. It resolves the credential's
   * FileSystem for every operation instead of capturing it at creation, and keeps the
   * FileSystem marked as in use while a stream it opened is open.
   */
  private final class CredentialedInputFile implements InputFile {
    private final Credential cred_;
    private final String path_;
    private volatile long length_; // -1 until known

    CredentialedInputFile(Credential cred, String path, long length) {
      cred_ = cred;
      path_ = path;
      length_ = length;
    }

    private HadoopInputFile hadoopFile(CachedFileSystem cfs) {
      Path p = new Path(path_);
      long len = length_;
      return len >= 0 ? HadoopInputFile.fromPath(p, len, cfs.fs, cfs.conf)
                      : HadoopInputFile.fromPath(p, cfs.fs, cfs.conf);
    }

    @Override
    public long getLength() {
      if (length_ < 0) length_ = hadoopFile(fileSystem(cred_, path_, false)).getLength();
      return length_;
    }

    @Override
    public SeekableInputStream newStream() {
      CachedFileSystem cfs = fileSystem(cred_, path_, true);
      try {
        return new TrackedInputStream(hadoopFile(cfs).newStream(), cfs);
      } catch (RuntimeException | Error e) {
        closeStream(cfs);
        throw e;
      }
    }

    @Override
    public String location() { return path_; }

    @Override
    public boolean exists() {
      return hadoopFile(fileSystem(cred_, path_, false)).exists();
    }

    @Override
    public String toString() { return path_; }
  }

  /** The OutputFile counterpart of CredentialedInputFile. */
  private final class CredentialedOutputFile implements OutputFile {
    private final Credential cred_;
    private final String path_;

    CredentialedOutputFile(Credential cred, String path) {
      cred_ = cred;
      path_ = path;
    }

    private PositionOutputStream open(boolean overwrite) {
      CachedFileSystem cfs = fileSystem(cred_, path_, true);
      try {
        OutputFile f = HadoopOutputFile.fromPath(new Path(path_), cfs.fs, cfs.conf);
        return new TrackedOutputStream(
            overwrite ? f.createOrOverwrite() : f.create(), cfs);
      } catch (RuntimeException | Error e) {
        closeStream(cfs);
        throw e;
      }
    }

    @Override
    public PositionOutputStream create() { return open(false); }

    @Override
    public PositionOutputStream createOrOverwrite() { return open(true); }

    @Override
    public String location() { return path_; }

    @Override
    public InputFile toInputFile() { return new CredentialedInputFile(cred_, path_, -1); }

    @Override
    public String toString() { return path_; }
  }

  /** Forwards to a stream opened on 'cfs' and calls closeStream() once on close. */
  private static final class TrackedInputStream extends SeekableInputStream
      implements DelegatingInputStream {
    private final SeekableInputStream in_;
    private final CachedFileSystem cfs_;
    private final AtomicBoolean closed_ = new AtomicBoolean();

    TrackedInputStream(SeekableInputStream in, CachedFileSystem cfs) {
      in_ = in;
      cfs_ = cfs;
    }

    @Override
    public InputStream getDelegate() {
      return in_ instanceof DelegatingInputStream
          ? ((DelegatingInputStream) in_).getDelegate() : in_;
    }

    @Override
    public long getPos() throws IOException { return in_.getPos(); }

    @Override
    public void seek(long newPos) throws IOException { in_.seek(newPos); }

    @Override
    public int read() throws IOException { return in_.read(); }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return in_.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException { return in_.skip(n); }

    @Override
    public int available() throws IOException { return in_.available(); }

    @Override
    public void close() throws IOException {
      if (!closed_.compareAndSet(false, true)) return;
      try {
        in_.close();
      } finally {
        closeStream(cfs_);
      }
    }
  }

  /** Forwards to a stream opened on 'cfs' and calls closeStream() once on close. */
  private static final class TrackedOutputStream extends PositionOutputStream
      implements DelegatingOutputStream {
    private final PositionOutputStream out_;
    private final CachedFileSystem cfs_;
    private final AtomicBoolean closed_ = new AtomicBoolean();

    TrackedOutputStream(PositionOutputStream out, CachedFileSystem cfs) {
      out_ = out;
      cfs_ = cfs;
    }

    @Override
    public OutputStream getDelegate() {
      return out_ instanceof DelegatingOutputStream
          ? ((DelegatingOutputStream) out_).getDelegate() : out_;
    }

    @Override
    public long getPos() throws IOException { return out_.getPos(); }

    @Override
    public long storedLength() throws IOException { return out_.storedLength(); }

    @Override
    public void write(int b) throws IOException { out_.write(b); }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out_.write(b, off, len);
    }

    @Override
    public void flush() throws IOException { out_.flush(); }

    @Override
    public void close() throws IOException {
      if (!closed_.compareAndSet(false, true)) return;
      try {
        out_.close();
      } finally {
        closeStream(cfs_);
      }
    }
  }

  /** Number of cached credentialed FileSystems; for tests and diagnostics. */
  static int cachedFileSystemCount() {
    return FS_CACHE.size();
  }
}
