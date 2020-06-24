/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.writelog.recover;

import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.crud.UpdatePlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * LogReplayer finds the logNode of the TsFile given by insertFilePath and logNodePrefix, reads the
 * WALs from the logNode and redoes them into a given MemTable and ModificationFile.
 */
public class LogReplayer {
  private Logger logger = LoggerFactory.getLogger(LogReplayer.class);
  private String logNodePrefix;
  private String insertFilePath;
  private ModificationFile modFile;
  private VersionController versionController;
  private TsFileResource currentTsFileResource;
  private IMemTable recoverMemTable;

  // unsequence file tolerates duplicated data
  private boolean acceptDuplication;

  private Map<String, Long> tempStartTimeMap = new HashMap<>();
  private Map<String, Long> tempEndTimeMap = new HashMap<>();

  public LogReplayer(String logNodePrefix, String insertFilePath, ModificationFile modFile,
      VersionController versionController, TsFileResource currentTsFileResource,
      IMemTable memTable, boolean acceptDuplication) {
    this.logNodePrefix = logNodePrefix;
    this.insertFilePath = insertFilePath;
    this.modFile = modFile;
    this.versionController = versionController;
    this.currentTsFileResource = currentTsFileResource;
    this.recoverMemTable = memTable;
    this.acceptDuplication = acceptDuplication;
  }

  /**
   * finds the logNode of the TsFile given by insertFilePath and logNodePrefix, reads the WALs from
   * the logNode and redoes them into a given MemTable and ModificationFile.
   */
  public void replayLogs() {
    WriteLogNode logNode = MultiFileLogNodeManager.getInstance().getNode(
        logNodePrefix + FSFactoryProducer.getFSFactory().getFile(insertFilePath).getName());

    ILogReader logReader = logNode.getLogReader();
    try {
      while (logReader.hasNext()) {
        try {
          PhysicalPlan plan = logReader.next();
          if (plan instanceof InsertPlan) {
            replayInsert((InsertPlan) plan);
          } else if (plan instanceof DeletePlan) {
            replayDelete((DeletePlan) plan);
          } else if (plan instanceof UpdatePlan) {
            replayUpdate((UpdatePlan) plan);
          } else if (plan instanceof InsertTabletPlan) {
            replayBatchInsert((InsertTabletPlan) plan);
          }
        } catch (Exception e) {
          logger.error("recover wal of {} failed", insertFilePath, e);
        }
      }
    } catch (IOException e) {
      logger.error("meet error when redo wal of {}", insertFilePath, e);
    } finally {
      logReader.close();
      try {
        modFile.close();
      } catch (IOException e) {
        logger.error("Canno close the modifications file {}", modFile.getFilePath(), e);
      }
    }
    tempStartTimeMap.forEach((k, v) -> currentTsFileResource.updateStartTime(k, v));
    tempEndTimeMap.forEach((k, v) -> currentTsFileResource.updateEndTime(k, v));
  }

  private void replayDelete(DeletePlan deletePlan) throws IOException {
    List<Path> paths = deletePlan.getPaths();
    for (Path path : paths) {
      recoverMemTable.delete(path.getDevice(), path.getMeasurement(), deletePlan.getDeleteTime());
      modFile
          .write(new Deletion(path, versionController.nextVersion(), deletePlan.getDeleteTime()));
    }
  }

  private void replayBatchInsert(InsertTabletPlan insertTabletPlan)
      throws WriteProcessException, QueryProcessException {
    if (currentTsFileResource != null) {
      // the last chunk group may contain the same data with the logs, ignore such logs in seq file
      long lastEndTime = currentTsFileResource.getEndTime(insertTabletPlan.getDeviceId());
      if (lastEndTime != Long.MIN_VALUE && lastEndTime >= insertTabletPlan.getMinTime() &&
          !acceptDuplication) {
        return;
      }
      Long startTime = tempStartTimeMap.get(insertTabletPlan.getDeviceId());
      if (startTime == null || startTime > insertTabletPlan.getMinTime()) {
        tempStartTimeMap.put(insertTabletPlan.getDeviceId(), insertTabletPlan.getMinTime());
      }
      Long endTime = tempEndTimeMap.get(insertTabletPlan.getDeviceId());
      if (endTime == null || endTime < insertTabletPlan.getMaxTime()) {
        tempEndTimeMap.put(insertTabletPlan.getDeviceId(), insertTabletPlan.getMaxTime());
      }
    }
    MeasurementSchema[] schemas;
    try {
      schemas = IoTDB.metaManager.getSchemas(insertTabletPlan.getDeviceId(), insertTabletPlan
          .getMeasurements());
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    insertTabletPlan.setSchemas(schemas);
    recoverMemTable.insertTablet(insertTabletPlan, 0, insertTabletPlan.getRowCount());
  }

  private void replayInsert(InsertPlan insertPlan) {
    if (currentTsFileResource != null) {
      // the last chunk group may contain the same data with the logs, ignore such logs in seq file
      long lastEndTime = currentTsFileResource.getEndTime(insertPlan.getDeviceId());
      if (lastEndTime != Long.MIN_VALUE && lastEndTime >= insertPlan.getTime() &&
          !acceptDuplication) {
        return;
      }
      Long startTime = tempStartTimeMap.get(insertPlan.getDeviceId());
      if (startTime == null || startTime > insertPlan.getTime()) {
        tempStartTimeMap.put(insertPlan.getDeviceId(), insertPlan.getTime());
      }
      Long endTime = tempEndTimeMap.get(insertPlan.getDeviceId());
      if (endTime == null || endTime < insertPlan.getTime()) {
        tempEndTimeMap.put(insertPlan.getDeviceId(), insertPlan.getTime());
      }
    }
    try {
      MeasurementSchema[] schemas =
          IoTDB.metaManager.getSchemas(insertPlan.getDeviceId(), insertPlan.getMeasurements());
      insertPlan.setSchemasAndTransferType(schemas);
      recoverMemTable.insert(insertPlan);
    } catch (Exception e) {
      logger.error(
          "occurs exception when replaying the record {} at timestamp {}: {}.(Will ignore the record)",
          insertPlan.getPaths(), insertPlan.getTime(), e.getMessage());
    }
  }

  @SuppressWarnings("unused")
  private void replayUpdate(UpdatePlan updatePlan) {
    // TODO: support update
    throw new UnsupportedOperationException("Update not supported");
  }
}
