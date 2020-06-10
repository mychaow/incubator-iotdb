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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import junit.framework.TestCase;
import org.apache.iotdb.cluster.common.IoTDBTest;
import org.apache.iotdb.cluster.common.TestDataGroupMember;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MeasurementMeta;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DataLogApplierTest extends IoTDBTest {

  private TestMetaGroupMember testMetaGroupMember = new TestMetaGroupMember() {
    @Override
    public boolean syncLeader() {
      return true;
    }

    @Override
    public List<MeasurementMeta> pullTimeSeriesSchemas(List<String> prefixPaths, InsertPlan plan)
        throws StorageGroupNotSetException {
      List<MeasurementMeta> ret = new ArrayList<>();
      for (String prefixPath : prefixPaths) {
        if (prefixPath.startsWith(TestUtils.getTestSg(4))) {
          for (int i = 0; i < 10; i++) {
            MeasurementMeta meta = new MeasurementMeta();
            meta.setSchema(TestUtils.getTestMeasurementSchema(i));
            ret.add(meta);
          }
        } else if (!prefixPath.startsWith(TestUtils.getTestSg(5))) {
          throw new StorageGroupNotSetException(prefixPath);
        }
      }
      return ret;
    }
  };

  private TestDataGroupMember testDataGroupMember = new TestDataGroupMember();

  private LogApplier applier = new DataLogApplier(testMetaGroupMember, testDataGroupMember);

  @Override
  @Before
  public void setUp() throws org.apache.iotdb.db.exception.StartupException, QueryProcessException {
    super.setUp();
    testMetaGroupMember.setLeader(testMetaGroupMember.getThisNode());
    testDataGroupMember.setLeader(testDataGroupMember.getThisNode());
    testDataGroupMember.setCharacter(NodeCharacter.LEADER);
    testMetaGroupMember.setCharacter(NodeCharacter.LEADER);
  }

  @Override
  @After
  public void tearDown() throws IOException, StorageEngineException {
    testDataGroupMember.closeLogManager();
    testMetaGroupMember.closeLogManager();
    super.tearDown();
  }

  @Test
  public void testApplyInsert()
      throws QueryProcessException, IOException, QueryFilterOptimizationException, StorageEngineException, MetadataException, TException, InterruptedException {
    InsertPlan insertPlan = new InsertPlan();
    PhysicalPlanLog log = new PhysicalPlanLog();
    log.setPlan(insertPlan);

    // this series is already created
    insertPlan.setDeviceId(TestUtils.getTestSg(1));
    insertPlan.setTime(1);
    insertPlan.setInferType(true);
    insertPlan.setMeasurements(new String[] {TestUtils.getTestMeasurement(0)});
    insertPlan.setTypes(new TSDataType[insertPlan.getMeasurements().length]);
    insertPlan.setValues(new Object[] {"1.0"});
    insertPlan.setInferType(true);
    insertPlan.setSchemasAndTransferType(new MeasurementSchema[] {TestUtils.getTestMeasurementSchema(0)});

    applier.apply(log);
    QueryDataSet dataSet = query(Collections.singletonList(TestUtils.getTestSeries(1, 0)), null);
    assertTrue(dataSet.hasNext());
    RowRecord record = dataSet.next();
    assertEquals(1, record.getTimestamp());
    assertEquals(1, record.getFields().size());
    assertEquals(1.0, record.getFields().get(0).getDoubleV(), 0.00001);
    assertFalse(dataSet.hasNext());

    // this series is not created but can be fetched
    insertPlan.setDeviceId(TestUtils.getTestSg(4));
    applier.apply(log);
    dataSet = query(Collections.singletonList(TestUtils.getTestSeries(4, 0)), null);
    assertTrue(dataSet.hasNext());
    record = dataSet.next();
    assertEquals(1, record.getTimestamp());
    assertEquals(1, record.getFields().size());
    assertEquals(1.0, record.getFields().get(0).getDoubleV(), 0.00001);
    assertFalse(dataSet.hasNext());

    // this series does not exists any where
    insertPlan.setDeviceId(TestUtils.getTestSg(5));
    try {
      applier.apply(log);
      fail("exception should be thrown");
    } catch (QueryProcessException e) {
      assertEquals("org.apache.iotdb.db.exception.metadata.PathNotExistException: Path [root.test5.s0] does not exist", e.getMessage());
    }

    // this storage group is not even set
    insertPlan.setDeviceId(TestUtils.getTestSg(6));
    try {
      applier.apply(log);
      fail("exception should be thrown");
    } catch (QueryProcessException e) {
      assertEquals("org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException: Storage group is not set for current seriesPath: [root.test6.s0]", e.getMessage());
    }
  }

  @Test
  public void testApplyDeletion()
      throws QueryProcessException, MetadataException, QueryFilterOptimizationException, StorageEngineException, IOException, TException, InterruptedException {
    DeletePlan deletePlan = new DeletePlan();
    deletePlan.setPaths(Collections.singletonList(new Path(TestUtils.getTestSeries(0, 0))));
    deletePlan.setDeleteTime(50);
    applier.apply(new PhysicalPlanLog(deletePlan));
    QueryDataSet dataSet = query(Collections.singletonList(TestUtils.getTestSeries(0, 0)), null);
    int cnt = 0;
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      assertEquals(cnt + 51L , record.getTimestamp());
      assertEquals((cnt + 51) * 1.0, record.getFields().get(0).getDoubleV(), 0.00001);
      cnt++;
    }
    assertEquals(49, cnt);
  }

  @Test
  public void testApplyCloseFile()
      throws StorageEngineException, QueryProcessException, StorageGroupNotSetException {
    StorageGroupProcessor storageGroupProcessor =
        StorageEngine.getInstance().getProcessor(TestUtils.getTestSg(0));
    TestCase.assertFalse(storageGroupProcessor.getWorkSequenceTsFileProcessors().isEmpty());

    CloseFileLog closeFileLog = new CloseFileLog(TestUtils.getTestSg(0), 0, true);
    applier.apply(closeFileLog);
    TestCase.assertTrue(storageGroupProcessor.getWorkSequenceTsFileProcessors().isEmpty());
  }
}