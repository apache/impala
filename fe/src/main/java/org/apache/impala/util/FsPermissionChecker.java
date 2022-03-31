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

package org.apache.impala.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hdfs.protocol.AclException;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DeprecatedKeys.DFS_PERMISSIONS_SUPERUSERGROUP_KEY;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

/**
 * Singleton class that can check whether the current user has permission to access paths
 * in a FileSystem.
 */
public class FsPermissionChecker {
  private final static Logger LOG = LoggerFactory.getLogger(FsPermissionChecker.class);
  private final static FsPermissionChecker instance_;
  private final static Configuration CONF;
  protected final String user_;
  private final Set<String> groups_ = new HashSet<String>();
  private final String supergroup_;

  static {
    CONF = new Configuration();
    try {
      instance_ = new FsPermissionChecker();
    } catch (IOException e) {
      throw new RuntimeException(
          "Error initializing FsPermissionChecker: " + e.getMessage(), e);
    }
  }

  private FsPermissionChecker() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    groups_.addAll(Arrays.asList(ugi.getGroupNames()));
    // The default value is taken from the String DFS_PERMISSIONS_SUPERUSERGROUP_DEFAULT
    // in DFSConfigKeys.java from the hadoop-hdfs jar.
    supergroup_ = CONF.get(DFS_PERMISSIONS_SUPERUSERGROUP_KEY, "supergroup");
    user_ = ugi.getShortUserName();
  }

  private boolean isSuperUser() { return groups_.contains(supergroup_); }

  private static List<AclEntryType> ACL_TYPE_PRIORITY =
      ImmutableList.of(AclEntryType.USER, AclEntryType.GROUP, AclEntryType.OTHER);

  /**
   * Allows checking different access permissions of a file without repeatedly accessing
   * the underlying filesystem by caching the results of a status call at construction.
   */
  public class Permissions {
    private final FileStatus fileStatus_;
    private final FsPermission permissions_;
    private final AclStatus aclStatus_;
    private Map<AclEntryType, List<AclEntry>> entriesByTypes_ = Maps.newHashMap();
    private AclEntry mask_;

    /**
     * If aclStatus is null, ACL permissions are not checked.
     */
    protected Permissions(FileStatus fileStatus, AclStatus aclStatus) {
      Preconditions.checkNotNull(fileStatus);
      fileStatus_ = fileStatus;
      permissions_ = fileStatus.getPermission();
      aclStatus_ = aclStatus;
      if (aclStatus_ == null) return;

      // Group the ACLs by type, so that we can apply them in correct priority order. Not
      // clear from documentation whether aclStatus_.getEntries() guarantees this
      // ordering, so this is defensive.
      for (AclEntryType t: ACL_TYPE_PRIORITY) {
        entriesByTypes_.put(t, Lists.<AclEntry>newArrayList());
      }

      List<AclEntry> fullAclList =
          getAclFromPermAndEntries(permissions_, aclStatus_.getEntries());
      for (AclEntry e: fullAclList) {
        if (e.getType() == AclEntryType.MASK && e.getScope() != AclEntryScope.DEFAULT) {
          mask_ = e;
        } else if (isApplicableAcl(e)) {
          entriesByTypes_.get(e.getType()).add(e);
        }
      }
    }

    /**
     * Returns true if the mask should apply. The mask ACL applies only to unnamed user
     * ACLs (e.g. user::r-x), and all group ACLs.
     */
    private boolean shouldApplyMask(AclEntry acl) {
      if (mask_ == null) return false;

      switch (acl.getType()) {
        case USER:
          return acl.getName() != null;
        case GROUP:
          return true;
      }
      return false;
    }

    /**
     * Returns true if this ACL applies to the current user and / or group
     */
    private boolean isApplicableAcl(AclEntry e) {
      // Default ACLs are not used for permission checking, but instead control the
      // permissions received by child directories
      if (e.getScope() == AclEntryScope.DEFAULT) return false;

      switch (e.getType()) {
        case USER:
          String aclUser = e.getName() == null ? aclStatus_.getOwner() : e.getName();
          return FsPermissionChecker.this.user_.equals(aclUser);
        case GROUP:
          String aclGroup = e.getName() == null ? aclStatus_.getGroup() : e.getName();
          return FsPermissionChecker.this.groups_.contains(aclGroup);
        case OTHER:
          return true;
        case MASK:
          return false;
        default:
          LOG.warn("Unknown Acl type: " + e.getType());
          return false;
      }
    }

    /**
     * Returns true if ACLs allow 'action', false if they explicitly disallow 'action',
     * and 'null' if no ACLs are available.
     * See http://users.suse.com/~agruen/acl/linux-acls/online for more details about
     * acl access check algorithm.
     */
    private Boolean checkAcls(FsAction action) {
      // ACLs may not be enabled, so we need this ternary logic. If no ACLs are available,
      // returning null causes us to fall back to standard ugo permissions.
      if (aclStatus_ == null) return null;

      // Remember if there is an applicable ACL entry, including owner user, named user,
      // owning group, named group.
      boolean foundMatch = false;
      for (AclEntryType t: ACL_TYPE_PRIORITY) {
        for (AclEntry e: entriesByTypes_.get(t)) {
          if (t == AclEntryType.OTHER) {
            // Processed all ACL entries except the OTHER entry.
            // If found applicable ACL entries but none of them contain requested
            // permission, deny access. Otherwise check OTHER entry.
            return foundMatch ? false : e.getPermission().implies(action);
          }
          // If there is an applicable mask, 'action' is allowed iff both the mask and
          // the underlying ACL permit it.
          if (e.getPermission().implies(action)) {
            if (shouldApplyMask(e)) {
              if (mask_.getPermission().implies(action)) return true;
            } else {
              return true;
            }
          }
          // User ACL entry has priority, no need to continue check.
          if (t == AclEntryType.USER) return false;

          foundMatch = true;
        }
      }
      return false;
    }

    /**
     * Returns true if the current user can perform the given action given these
     * permissions.
     */
    public boolean checkPermissions(FsAction action) {
      if (FsPermissionChecker.this.isSuperUser()) return true;
      Boolean aclPerms = checkAcls(action);
      if (aclPerms != null) return aclPerms;

      // Check user, group and then 'other' permissions in turn.
      if (FsPermissionChecker.this.user_.equals(fileStatus_.getOwner())) {
        // If the user matches, we must return their access rights whether or not the user
        // is allowed to access without checking the group. This is counter-intuitive if
        // the user cannot access the file, but the group permissions would allow it, but
        // is consistent with UNIX behaviour.
        return permissions_.getUserAction().implies(action);
      }

      if (FsPermissionChecker.this.groups_.contains(fileStatus_.getGroup())) {
        return permissions_.getGroupAction().implies(action);
      }
      return permissions_.getOtherAction().implies(action);
    }

    public boolean canRead() { return checkPermissions(FsAction.READ); }
    public boolean canWrite() { return checkPermissions(FsAction.WRITE); }
    public boolean canReadAndWrite() { return canRead() && canWrite(); }

    // This was originally lifted from Hadoop. Won't need it if HDFS-7177 is resolved.
    // getAclStatus() returns just extended ACL entries, the default file permissions
    // like "user::,group::,other::" are not included. We need to combine them together
    // to get full logic ACL list.
    private List<AclEntry> getAclFromPermAndEntries(FsPermission perm,
        List<AclEntry> entries) {
      // File permission always have 3 items.
      List<AclEntry> aclEntries = Lists.newArrayListWithCapacity(entries.size() + 3);

      // Owner entry implied by owner permission bits.
      aclEntries.add(new AclEntry.Builder()
          .setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.USER)
          .setPermission(perm.getUserAction())
          .build());

      // All extended access ACL entries add by "-setfacl" other than default file
      // permission.
      boolean hasAccessAcl = false;
      for (AclEntry entry: entries) {
        // AclEntry list should be ordered, all ACCESS one are in first half, DEFAULT one
        // are in second half, so no need to continue here.
        if (entry.getScope() == AclEntryScope.DEFAULT) break;
        hasAccessAcl = true;
        aclEntries.add(entry);
      }

      // Mask entry implied by group permission bits, or group entry if there is
      // no access ACL (only default ACL).
      aclEntries.add(new AclEntry.Builder()
          .setScope(AclEntryScope.ACCESS)
          .setType(hasAccessAcl ? AclEntryType.MASK : AclEntryType.GROUP)
          .setPermission(perm.getGroupAction())
          .build());

      // Other entry implied by other bits.
      aclEntries.add(new AclEntry.Builder()
          .setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.OTHER)
          .setPermission(perm.getOtherAction())
          .build());

      return aclEntries;
    }
  }

  /**
   * Returns a Permissions object that can answer all access permission queries for the
   * given path.
   */
  public Permissions getPermissions(FileSystem fs, Path path) throws IOException {
    Preconditions.checkNotNull(fs);
    Preconditions.checkNotNull(path);
    return getPermissions(fs, fs.getFileStatus(path));
  }

  /**
   * Returns a Permissions object for the given FileStatus object. In the common
   * case that ACLs are not in use, this does not require any additional round-trip
   * to the FileSystem. This allows batch construction using APIs like
   * FileSystem.listStatus(...).
   */
  public Permissions getPermissions(FileSystem fs, FileStatus fileStatus)
      throws IOException {
    AclStatus aclStatus = null;
    if (fileStatus.getPermission().getAclBit()) {
      try {
        aclStatus = fs.getAclStatus(fileStatus.getPath());
      } catch (AclException ex) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(
              "No ACLs retrieved, skipping ACLs check (HDFS will enforce ACLs)", ex);
        }
      } catch (UnsupportedOperationException ex) {
        if (LOG.isTraceEnabled()) LOG.trace("No ACLs retrieved, unsupported", ex);
      }
    }
    return new Permissions(fileStatus, aclStatus);
  }

  /**
   * Returns true if the current user can perform the given action given these
   * permissions.
   */
  public boolean checkAccess(Path path, FileSystem fs, FsAction action,
      boolean isAuthzEnabled) throws IOException {
    // When Ranger authz is enabled, we invoke method
    // FileSystem#access(Path path, FsAction mode) to check the actual access permission.
    if (isAuthzEnabled) {
      try{
        fs.access(path, action);
      } catch (AccessControlException e) {
        LOG.warn(e.getMessage());
        return false;
      }
      return true;
    }
    return this.getPermissions(fs, path).checkPermissions(action);
  }

  /**
   * Returns the FsPermissionChecker singleton.
   */
  public static FsPermissionChecker getInstance() { return instance_; }
}
