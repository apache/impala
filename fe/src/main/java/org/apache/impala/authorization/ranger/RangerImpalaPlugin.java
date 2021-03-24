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

package org.apache.impala.authorization.ranger;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.hadoop.constants.RangerHadoopConstants;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * An implementation of Ranger Impala plugin. Make this a singleton since each process
 * should have only one ranger plugin instance. Impalad and Catalogd already satisfy this
 * requirement. The main purpose is to avoid test classes that has embedded catalogd and
 * frontend create multiple ranger plugins.
 */
public class RangerImpalaPlugin extends RangerBasePlugin {
  private static volatile RangerImpalaPlugin INSTANCE = null;
  private static String SERVICE_TYPE = null;
  private static String APP_ID = null;
  private static boolean BLOCK_UPDATE_IF_TABLE_MASK_SPECIFIED = true;

  private RangerImpalaPlugin(String serviceType, String appId) {
    super(serviceType, appId);
  }

  @Override
  public void init() {
    super.init();
    RangerImpalaPlugin.BLOCK_UPDATE_IF_TABLE_MASK_SPECIFIED = getConfig().getBoolean(
        RangerHadoopConstants
            .HIVE_BLOCK_UPDATE_IF_ROWFILTER_COLUMNMASK_SPECIFIED_PROP,
        RangerHadoopConstants
            .HIVE_BLOCK_UPDATE_IF_ROWFILTER_COLUMNMASK_SPECIFIED_DEFAULT_VALUE);
  }

  public boolean blockUpdateIfTableMaskSpecified() {
    return BLOCK_UPDATE_IF_TABLE_MASK_SPECIFIED;
  }

  public static RangerImpalaPlugin getInstance(String serviceType, String appId) {
    if (INSTANCE == null) {
      synchronized(RangerImpalaPlugin.class) {
        if (INSTANCE == null) {
          SERVICE_TYPE = serviceType;
          APP_ID = appId;
          INSTANCE = new RangerImpalaPlugin(serviceType, appId);
          INSTANCE.init();
        }
      }
    }
    Preconditions.checkState(StringUtils.equals(SERVICE_TYPE, serviceType),
        String.format("%s != %s", SERVICE_TYPE, serviceType));
    Preconditions.checkState(StringUtils.equals(APP_ID, appId),
        String.format("%s != %s", APP_ID, appId));
    return INSTANCE;
  }

  /**
   * This method returns the names of the supported mask types excluding those specified
   * in maskNamesToFilter. The method is not static since getServiceDef() is not static.
   */
  public Set<String> getUnfilteredMaskNames(Collection<String> maskNamesToFilter) {
    Set<String> maskNames = new HashSet<>();
    List<RangerServiceDef.RangerDataMaskTypeDef> maskTypes =
        getServiceDef().getDataMaskDef().getMaskTypes();
    for (RangerServiceDef.RangerDataMaskTypeDef maskType : maskTypes) {
      maskNames.add(maskType.getName().toUpperCase());
    }
    maskNames.removeAll(maskNamesToFilter);
    return maskNames;
  }
}
