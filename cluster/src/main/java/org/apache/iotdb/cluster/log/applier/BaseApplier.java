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

package org.apache.iotdb.cluster.log.applier;

import java.util.Collections;
import java.util.List;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.query.ClusterPlanExecutor;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MeasurementMeta;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BaseApplier use PlanExecutor to execute PhysicalPlans.
 */
abstract class BaseApplier implements LogApplier {

  private static final Logger logger = LoggerFactory.getLogger(BaseApplier.class);

  MetaGroupMember metaGroupMember;
  private PlanExecutor queryExecutor;

  BaseApplier(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  void applyPhysicalPlan(PhysicalPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    if (plan instanceof InsertPlan) {
      processPlanWithTolerance((InsertPlan) plan);
    } else if (!plan.isQuery()) {
      try {
        getQueryExecutor().processNonQuery(plan);
      } catch (QueryProcessException e) {
        if (e.getCause() instanceof StorageGroupNotSetException) {
          try {
            metaGroupMember.syncLeaderWithConsistencyCheck();
          } catch (CheckConsistencyException ce) {
            throw new QueryProcessException(ce.getMessage());
          }
          getQueryExecutor().processNonQuery(plan);
        } else {
          throw e;
        }
      }
    } else {
      logger.error("Unsupported physical plan: {}", plan);
    }
  }


  private void processPlanWithTolerance(InsertPlan plan)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    try {
      getQueryExecutor().processNonQuery(plan);
    } catch (QueryProcessException | StorageGroupNotSetException | StorageEngineException e) {
      // check if this is caused by metadata missing, if so, pull metadata and retry
      Throwable metaMissingException = SchemaUtils.findMetaMissingException(e);
      boolean causedByPathNotExist = metaMissingException instanceof PathNotExistException;
      boolean causedByStorageGroupNotSet = metaMissingException instanceof StorageGroupNotSetException;

      if (causedByPathNotExist) {
        if (logger.isDebugEnabled()) {
          logger.debug("Timeseries is not found locally[{}], try pulling it from another group: {}",
              metaGroupMember.getName(), e.getCause().getMessage());
        }
        try {
          String path = plan.getDeviceId();
          List<MeasurementSchema> schemas = metaGroupMember
              .pullTimeSeriesSchemas(Collections.singletonList(path));
          for (MeasurementSchema schema : schemas) {
            registerMeasurement(
                path + IoTDBConstant.PATH_SEPARATOR + schema.getMeasurementId(),
                schema);
          }
        } catch (MetadataException e1) {
          throw new QueryProcessException(e1);
        }
        getQueryExecutor().processNonQuery(plan);
      } else if (causedByStorageGroupNotSet) {
        try {
          metaGroupMember.syncLeaderWithConsistencyCheck();
        } catch (CheckConsistencyException ce) {
          throw new QueryProcessException(ce.getMessage());
        }
        getQueryExecutor().processNonQuery(plan);
      } else {
        throw e;
      }
    }
  }

  protected void registerMeasurement(String path, MeasurementSchema schema) {
    IoTDB.metaManager.cacheMeta(path, new MeasurementMeta(schema));
  }

  protected PlanExecutor getQueryExecutor() throws QueryProcessException {
    if (queryExecutor == null) {
      queryExecutor = new ClusterPlanExecutor(metaGroupMember);
    }
    return queryExecutor;
  }
}
