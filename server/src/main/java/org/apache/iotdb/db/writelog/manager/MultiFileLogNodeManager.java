/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.writelog.manager;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MultiFileLogNodeManager manages all ExclusiveWriteLogNodes, each manages WALs of a TsFile
 * (either seq or unseq).
 */
public class MultiFileLogNodeManager extends AbstractMultiLogNodeManager {

  private final Logger logger = LoggerFactory.getLogger(MultiFileLogNodeManager.class);

  private MultiFileLogNodeManager() {
    super();
    name = "MultiFileLogNodeManager";
  }

  public static MultiFileLogNodeManager getInstance() {
    return InstanceHolder.instance;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.WAL_SERVICE;
  }

  @Override
  protected boolean isEnable() {
    return config.isEnableWal();
  }

  @Override
  protected long getForceFlushPeriodInMs() {
    return config.getForceWalPeriodInMs();
  }

  @Override
  protected String getForceDaemonName() {
    return ThreadName.WAL_FORCE_DAEMON.getName();
  }

  @Override
  protected WriteLogNode newWriteLogNode(String identifier) {
    return new ExclusiveWriteLogNode<PhysicalPlan>(identifier);
  }

  private static class InstanceHolder {
    private InstanceHolder(){}

    private static MultiFileLogNodeManager instance = new MultiFileLogNodeManager();
  }

}
