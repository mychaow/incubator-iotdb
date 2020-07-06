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

package org.apache.iotdb.cluster.server.member;

import static org.apache.iotdb.cluster.server.NodeCharacter.ELECTOR;
import static org.apache.iotdb.cluster.server.NodeCharacter.FOLLOWER;
import static org.apache.iotdb.cluster.server.NodeCharacter.LEADER;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.common.TestAsyncDataClient;
import org.apache.iotdb.cluster.common.TestAsyncMetaClient;
import org.apache.iotdb.cluster.common.TestPartitionedLogManager;
import org.apache.iotdb.cluster.common.TestSnapshot;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.PartitionTableUnavailableException;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.snapshot.MetaSimpleSnapshot;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.CheckStatusResponse;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.StartUpStatus;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.DataClusterServer;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TCompactProtocol.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetaGroupMemberTest extends MemberTest {

  private DataClusterServer dataClusterServer;

  private boolean mockDataClusterServer;
  private Node exiledNode;

  private int prevReplicaNum;
  private List<String> prevSeedNodes;

  @Override
  @After
  public void tearDown() throws Exception {
    dataClusterServer.stop();
    super.tearDown();
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(prevReplicaNum);
    ClusterDescriptor.getInstance().getConfig().setSeedNodeUrls(prevSeedNodes);
  }

  @Before
  public void setUp() throws Exception {
    prevSeedNodes = ClusterDescriptor.getInstance().getConfig().getSeedNodeUrls();
    ClusterDescriptor.getInstance().getConfig().setSeedNodeUrls(Collections.emptyList());
    prevReplicaNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    ClusterDescriptor.getInstance().getConfig().setReplicationNum(2);
    super.setUp();
    dummyResponse.set(Response.RESPONSE_AGREE);
    testMetaMember.setAllNodes(allNodes);

    dataClusterServer = new DataClusterServer(TestUtils.getNode(0),
        new DataGroupMember.Factory(null, testMetaMember) {
          @Override
          public DataGroupMember create(PartitionGroup partitionGroup, Node thisNode) {
            return getDataGroupMember(partitionGroup, thisNode);
          }
        }, testMetaMember);
    buildDataGroups(dataClusterServer);
    testMetaMember.getThisNode().setNodeIdentifier(0);
    mockDataClusterServer = false;
    QueryCoordinator.getINSTANCE().setMetaGroupMember(testMetaMember);
    exiledNode = null;
    System.out.println("Init term of metaGroupMember: " + testMetaMember.getTerm().get());
  }

  private DataGroupMember getDataGroupMember(PartitionGroup group, Node node) {
    DataGroupMember dataGroupMember = new DataGroupMember(null, group, node,
        testMetaMember) {
      @Override
      public boolean syncLeader() {
        return true;
      }

      @Override
      TSStatus executeNonQuery(PhysicalPlan plan) {
        try {
          planExecutor.processNonQuery(plan);
          return StatusUtils.OK;
        } catch (QueryProcessException | StorageGroupNotSetException | StorageEngineException e) {
          TSStatus status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
          status.setMessage(e.getMessage());
          return status;
        }
      }

      @Override
      TSStatus forwardPlan(PhysicalPlan plan, Node node, Node header) {
        return executeNonQuery(plan);
      }

      @Override
      public AsyncClient connectNode(Node node) {
        return null;
      }

      @Override
      public void pullTimeSeriesSchema(PullSchemaRequest request,
          AsyncMethodCallback<PullSchemaResp> resultHandler) {
        mockedPullTimeSeriesSchema(request, resultHandler);
      }
    };
    dataGroupMember.setLogManager(new TestPartitionedLogManager(null,
        partitionTable, group.getHeader(), TestSnapshot::new));
    dataGroupMember.setLeader(node);
    dataGroupMember.setCharacter(NodeCharacter.LEADER);
    return dataGroupMember;
  }

  private void mockedPullTimeSeriesSchema(PullSchemaRequest request,
      AsyncMethodCallback<PullSchemaResp> resultHandler) {
    new Thread(() -> {
      try {
        List<MeasurementSchema> schemas = new ArrayList<>();
        List<String> prefixPaths = request.getPrefixPaths();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        for (String prefixPath : prefixPaths) {
          if (!prefixPath.equals(TestUtils.getTestSeries(10, 0))) {
            IoTDB.metaManager.collectSeries(prefixPath, schemas);
            dataOutputStream.writeInt(schemas.size());
            for (MeasurementSchema schema : schemas) {
              schema.serializeTo(dataOutputStream);
            }
          } else {
            dataOutputStream.writeInt(1);
            TestUtils.getTestMeasurementSchema( 0).serializeTo(dataOutputStream);
          }
        }
        PullSchemaResp resp = new PullSchemaResp();
        resp.setSchemaBytes(byteArrayOutputStream.toByteArray());
        resultHandler.onComplete(resp);
      } catch (IOException e) {
        resultHandler.onError(e);
      }
    }).start();
  }


  protected MetaGroupMember getMetaGroupMember(Node node) throws QueryProcessException {
    MetaGroupMember metaGroupMember = new MetaGroupMember(new Factory(), node) {

      @Override
      public DataClusterServer getDataClusterServer() {
        return mockDataClusterServer ? MetaGroupMemberTest.this.dataClusterServer
            : super.getDataClusterServer();
      }

      @Override
      public AsyncDataClient getDataClient(Node node) throws IOException {
        return new TestAsyncDataClient(node, dataGroupMemberMap);
      }

      @Override
      public DataGroupMember getLocalDataMember(Node header, AsyncMethodCallback resultHandler,
                                                Object request) {
        return getDataGroupMember(header);
      }

      @Override
      public DataGroupMember getLocalDataMember(Node header) {
        return getDataGroupMember(header);
      }

      @Override
      public void updateHardState(long currentTerm, Node leader) {
      }

      @Override
      protected void addSeedNodes() {
        List<String> seedUrls = config.getSeedNodeUrls();
        // initialize allNodes
        for (String seedUrl : seedUrls) {
          Node node = generateNode(seedUrl);
          if ((!node.getIp().equals(thisNode.ip) || node.getMetaPort() != thisNode.getMetaPort())
              && !allNodes.contains(node)) {
            // do not add the local node since it is added in `setThisNode()`
            allNodes.add(node);
          }
        }
      }

      @Override
      public AsyncClient connectNode(Node node) {
        try {
          return new TestAsyncMetaClient(null, null, node, null) {
            @Override
            public void startElection(ElectionRequest request,
                AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                long resp = dummyResponse.get();
                // MIN_VALUE means let the request time out
                if (resp != Long.MIN_VALUE) {
                  resultHandler.onComplete(resp);
                }
              }).start();
            }

            @Override
            public void sendHeartbeat(HeartBeatRequest request,
                AsyncMethodCallback<HeartBeatResponse> resultHandler) {
              new Thread(() -> {
                HeartBeatResponse response = new HeartBeatResponse();
                response.setTerm(Response.RESPONSE_AGREE);
                resultHandler.onComplete(response);
              }).start();
            }

            @Override
            public void appendEntry(AppendEntryRequest request,
                AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                long resp = dummyResponse.get();
                // MIN_VALUE means let the request time out
                if (resp != Long.MIN_VALUE) {
                  resultHandler.onComplete(dummyResponse.get());
                }
              }).start();
            }

            @Override
            public void addNode(Node node, StartUpStatus startUpStatus,
                AsyncMethodCallback<AddNodeResponse> resultHandler) {
              new Thread(() -> {
                if (node.getNodeIdentifier() == 10) {
                  resultHandler.onComplete(new AddNodeResponse(
                      (int) Response.RESPONSE_IDENTIFIER_CONFLICT));
                } else {
                  partitionTable.addNode(node);
                  AddNodeResponse resp = new AddNodeResponse((int) dummyResponse.get());
                  resp.setPartitionTableBytes(partitionTable.serialize());
                  resultHandler.onComplete(resp);
                }
              }).start();
            }

            @Override
            public void executeNonQueryPlan(ExecutNonQueryReq request,
                AsyncMethodCallback<TSStatus> resultHandler) {
              new Thread(() -> {
                try {
                  PhysicalPlan plan = PhysicalPlan.Factory.create(request.planBytes);
                  planExecutor.processNonQuery(plan);
                  resultHandler.onComplete(StatusUtils.OK);
                } catch (IOException | QueryProcessException | StorageGroupNotSetException | StorageEngineException e) {
                  resultHandler.onError(e);
                }
              }).start();
            }

            @Override
            public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
              new Thread(() -> resultHandler.onComplete(new TNodeStatus())).start();
            }

            @Override
            public void exile(AsyncMethodCallback<Void> resultHandler) {
              System.out.printf("%s was exiled%n", node);
              exiledNode = node;
            }

            @Override
            public void removeNode(Node node, AsyncMethodCallback<Long> resultHandler) {
              new Thread(() -> {
                testMetaMember.applyRemoveNode(node);
                resultHandler.onComplete(Response.RESPONSE_AGREE);
              }).start();
            }

            @Override
            public void checkStatus(StartUpStatus startUpStatus,
                AsyncMethodCallback<CheckStatusResponse> resultHandler) {
              new Thread(() -> {
                CheckStatusResponse response = new CheckStatusResponse();
                response.setHashSaltEquals(true);
                response.setPartitionalIntervalEquals(true);
                response.setReplicationNumEquals(true);
                response.setSeedNodeEquals(true);
                resultHandler.onComplete(response);
              }).start();
            }
          };
        } catch (IOException e) {
          return null;
        }
      }
    };
    metaGroupMember.setLeader(node);
    metaGroupMember.setAllNodes(allNodes);
    metaGroupMember.setCharacter(NodeCharacter.LEADER);
    metaGroupMember.setAppendLogThreadPool(testThreadPool);
    return metaGroupMember;
  }

  private void buildDataGroups(DataClusterServer dataClusterServer) {
    List<PartitionGroup> partitionGroups = partitionTable.getLocalGroups();

    dataClusterServer.setPartitionTable(partitionTable);
    for (PartitionGroup partitionGroup : partitionGroups) {
      DataGroupMember dataGroupMember = getDataGroupMember(partitionGroup, TestUtils.getNode(0));
      dataGroupMember.start();
      dataClusterServer.addDataGroupMember(dataGroupMember);
    }
  }

  @Test
  public void testClosePartition()
      throws QueryProcessException, StorageEngineException, StorageGroupNotSetException {
    System.out.println("Start testClosePartition()");
    // the operation is accepted
    dummyResponse.set(Response.RESPONSE_AGREE);
    InsertRowPlan insertPlan = new InsertRowPlan();
    insertPlan.setDeviceId(TestUtils.getTestSg(0));
    insertPlan.setNeedInferType(true);
    insertPlan.setMeasurements(new String[]{TestUtils.getTestMeasurement(0)});
    insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);
    for (int i = 0; i < 10; i++) {
      insertPlan.setTime(i);
      insertPlan.setValues(new Object[]{String.valueOf(i)});
      insertPlan.setSchemasAndTransferType(new MeasurementSchema[]{TestUtils.getTestMeasurementSchema(0)});
      PlanExecutor planExecutor = new PlanExecutor();
      planExecutor.processNonQuery(insertPlan);
    }
    ExecutorService testThreadPool = Executors.newFixedThreadPool(4);
    testMetaMember.setAppendLogThreadPool(testThreadPool);
    testMetaMember.closePartition(TestUtils.getTestSg(0), 0, true);

    StorageGroupProcessor processor =
        StorageEngine.getInstance().getProcessor(TestUtils.getTestSg(0));
    assertTrue(processor.getWorkSequenceTsFileProcessors().isEmpty());

    int prevTimeout = RaftServer.getConnectionTimeoutInMS();
    RaftServer.setConnectionTimeoutInMS(100);
    try {
      System.out.println("Create the first file");
      for (int i = 20; i < 30; i++) {
        insertPlan.setTime(i);
        insertPlan.setValues(new Object[]{String.valueOf(i)});
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.processNonQuery(insertPlan);
      }
      // the net work is down
      dummyResponse.set(Long.MIN_VALUE);

      // network resume in 100ms
      new Thread(() -> {
        await().atLeast(200, TimeUnit.MILLISECONDS);
        dummyResponse.set(Response.RESPONSE_AGREE);
      }).start();

      System.out.println("Close the first file");
      testMetaMember.closePartition(TestUtils.getTestSg(0), 0, true);
      assertTrue(processor.getWorkSequenceTsFileProcessors().isEmpty());

      System.out.println("Create the second file");
      for (int i = 30; i < 40; i++) {
        insertPlan.setTime(i);
        insertPlan.setValues(new Object[]{String.valueOf(i)});
        PlanExecutor planExecutor = new PlanExecutor();
        planExecutor.processNonQuery(insertPlan);
      }
      // indicating the leader is stale
      System.out.println("Close the second file");
      dummyResponse.set(100);
      testMetaMember.closePartition(TestUtils.getTestSg(0), 0, true);
      assertFalse(processor.getWorkSequenceTsFileProcessors().isEmpty());
    } finally {
      RaftServer.setConnectionTimeoutInMS(prevTimeout);
    }
    testThreadPool.shutdownNow();
  }

  @Test
  public void testAddNode() {
    System.out.println("Start testAddNode()");
    Node newNode = TestUtils.getNode(10);
    testMetaMember.onElectionWins();
    testMetaMember.applyAddNode(newNode);
    assertTrue(partitionTable.getAllNodes().contains(newNode));
  }

  @Test
  public void testBuildCluster() {
    System.out.println("Start testBuildCluster()");
    testMetaMember.start();
    try {
      testMetaMember.buildCluster();
      long startTime = System.currentTimeMillis();
      long timeConsumption = 0;
      while (timeConsumption < 5000 && testMetaMember.getCharacter() != LEADER) {
        timeConsumption = System.currentTimeMillis() - startTime;
      }
      if (timeConsumption >= 5000) {
        fail("The member takes too long to be the leader");
      }
      assertEquals(LEADER, testMetaMember.getCharacter());
    } finally {
      testMetaMember.stop();
    }
  }

  @Test
  public void testJoinCluster() throws QueryProcessException {
    System.out.println("Start testJoinCluster()");
    MetaGroupMember newMember = getMetaGroupMember(TestUtils.getNode(10));
    newMember.start();
    try {
      assertTrue(newMember.joinCluster());
      newMember.setCharacter(ELECTOR);
      while (!LEADER.equals(newMember.getCharacter())) {

      }
    } finally {
      newMember.stop();
    }
  }

  @Test
  public void testJoinClusterFailed() throws QueryProcessException {
    System.out.println("Start testJoinClusterFailed()");
    long prevInterval = RaftServer.getHeartBeatIntervalMs();
    RaftServer.setHeartBeatIntervalMs(10);
    try {
      dummyResponse.set(Response.RESPONSE_NO_CONNECTION);
      MetaGroupMember newMember = getMetaGroupMember(TestUtils.getNode(10));
      assertFalse(newMember.joinCluster());
      newMember.closeLogManager();
    } finally {
      RaftServer.setHeartBeatIntervalMs(prevInterval);
    }
  }

  @Test
  public void testSendSnapshot() {
    System.out.println("Start testSendSnapshot()");
    SendSnapshotRequest request = new SendSnapshotRequest();

    // 1. prepare storage group and its tll
    Map<String, Long> storageGroupTTL = new HashMap<>();
    long baseTTL = 3600;
    for (int i = 0; i <= 10; i++) {
      storageGroupTTL.put(TestUtils.getTestSg(i), baseTTL + i * 100);
      if (i >= 5) {
        storageGroupTTL.put(TestUtils.getTestSg(i), Long.MAX_VALUE);
      }
    }

    HashMap<String, User> userMap = new HashMap<>();
    HashMap<String, Role> roleMap = new HashMap<>();

    try {

      IAuthorizer authorizer = LocalFileAuthorizer.getInstance();

      // 2. prepare the role info
      authorizer.createRole("role_1");
      authorizer.createRole("role_2");
      authorizer.createRole("role_3");
      authorizer.createRole("role_4");

      authorizer.grantPrivilegeToRole("role_1", TestUtils.getTestSg(3), 1);
      authorizer.grantPrivilegeToRole("role_2", TestUtils.getTestSg(4), 1);

      roleMap.put("role_1", authorizer.getRole("role_1"));
      roleMap.put("role_2", authorizer.getRole("role_2"));
      roleMap.put("role_3", authorizer.getRole("role_3"));
      roleMap.put("role_4", authorizer.getRole("role_4"));

      // 3. prepare the user info
      authorizer.createUser("user_1", "password_1");
      authorizer.createUser("user_2", "password_2");
      authorizer.createUser("user_3", "password_3");
      authorizer.createUser("user_4", "password_4");

      authorizer.grantPrivilegeToUser("user_1", TestUtils.getTestSg(1), 1);
      authorizer.setUserUseWaterMark("user_2", true);

      authorizer.grantRoleToUser("role_1", "user_1");

      userMap.put("user_1", authorizer.getUser("user_1"));
      userMap.put("user_2", authorizer.getUser("user_2"));
      userMap.put("user_3", authorizer.getUser("user_3"));
      userMap.put("user_4", authorizer.getUser("user_4"));


    } catch (AuthException e) {
      assertEquals("why failed?", e.getMessage());
    }

    // 4. prepare the partition table
    PartitionTable partitionTable = TestUtils.getPartitionTable(3);
    ByteBuffer beforePartitionTableBuffer = partitionTable.serialize();
    // 5. serialize
    MetaSimpleSnapshot snapshot = new MetaSimpleSnapshot(storageGroupTTL, userMap, roleMap,
        beforePartitionTableBuffer);
    request.setSnapshotBytes(snapshot.serialize());
    AtomicReference<Void> reference = new AtomicReference<>();
    testMetaMember.sendSnapshot(request, new GenericHandler(TestUtils.getNode(0), reference));

    // 6. check whether the snapshot applied or not
    Map<String, Long> localStorageGroupTTL = IoTDB.metaManager.getStorageGroupsTTL();
    assertNotNull(localStorageGroupTTL);
    assertEquals(storageGroupTTL, localStorageGroupTTL);

    try {
      IAuthorizer authorizer = LocalFileAuthorizer.getInstance();

      assertTrue(authorizer.checkUserPrivileges("user_1", TestUtils.getTestSg(1), 1));
      assertTrue(authorizer.checkUserPrivileges("user_1", TestUtils.getTestSg(3), 1));
      assertFalse(authorizer.checkUserPrivileges("user_3", TestUtils.getTestSg(1), 1));

      assertTrue(authorizer.isUserUseWaterMark("user_2"));
      assertFalse(authorizer.isUserUseWaterMark("user_4"));

      Map<String, Role> localRoleMap = authorizer.getAllRoles();
      assertEquals(roleMap, localRoleMap);

      PartitionTable localPartitionTable = this.testMetaMember.getPartitionTable();
      assertEquals(localPartitionTable, partitionTable);

    } catch (AuthException e) {
      assertEquals("why failed?", e.getMessage());
    }
  }

  @Test
  public void testProcessNonQuery() {
    System.out.println("Start testProcessNonQuery()");
    mockDataClusterServer = true;
    // as a leader
    testMetaMember.setCharacter(LEADER);
    testMetaMember.setAppendLogThreadPool(testThreadPool);
    for (int i = 10; i < 20; i++) {
      // process a non partitioned plan
      SetStorageGroupPlan setStorageGroupPlan =
          new SetStorageGroupPlan(new Path(TestUtils.getTestSg(i)));
      TSStatus status = testMetaMember.executeNonQuery(setStorageGroupPlan);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.code);
      assertTrue(IoTDB.metaManager.isPathExist(TestUtils.getTestSg(i)));

      // process a partitioned plan
      TimeseriesSchema schema = TestUtils.getTestTimeSeriesSchema(i, 0);
      CreateTimeSeriesPlan createTimeSeriesPlan = new CreateTimeSeriesPlan(
          new Path(schema.getFullPath()), schema.getType(),
          schema.getEncodingType(), schema.getCompressor(), schema.getProps(),
          Collections.emptyMap(), Collections.emptyMap(), null);
      status = testMetaMember.executeNonQuery(createTimeSeriesPlan);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.code);
      assertTrue(IoTDB.metaManager.isPathExist(TestUtils.getTestSeries(i, 0)));
    }
    testThreadPool.shutdownNow();
  }

  @Test
  public void testPullTimeseriesSchema() throws MetadataException {
    System.out.println("Start testPullTimeseriesSchema()");
    for (int i = 0; i < 10; i++) {
      List<MeasurementSchema> schemas =
          testMetaMember.pullTimeSeriesSchemas(Collections.singletonList(TestUtils.getTestSg(i)));
      assertEquals(20, schemas.size());
      for (int j = 0; j < 10; j++) {
        assertEquals(TestUtils.getTestMeasurementSchema(j), schemas.get(j));
      }
    }
  }

  @Test
  public void testGetSeriesType() throws MetadataException {
    System.out.println("Start testGetSeriesType()");
    // a local series
    assertEquals(Collections.singletonList(TSDataType.DOUBLE),
        testMetaMember
            .getSeriesTypesByString(Collections.singletonList(TestUtils.getTestSeries(0, 0)),
                null).left);
    // a remote series that can be fetched
    assertEquals(Collections.singletonList(TSDataType.DOUBLE),
        testMetaMember
            .getSeriesTypesByString(Collections.singletonList(TestUtils.getTestSeries(9, 0)),
                null).left);
    // a non-existent series
    IoTDB.metaManager.setStorageGroup(TestUtils.getTestSg(10));
    try {
      testMetaMember.getSeriesTypesByString(Collections.singletonList(TestUtils.getTestSeries(10
          , 100)), null);
    } catch (PathNotExistException e) {
      assertEquals("Path [root.test10.s100] does not exist", e.getMessage());
    }
    // a non-existent group
    try {
      testMetaMember.getSeriesTypesByString(Collections.singletonList(TestUtils.getTestSeries(11
          , 100)), null);
    } catch (StorageGroupNotSetException e) {
      assertEquals("Storage group is not set for current seriesPath: [root.test11.s100]",
          e.getMessage());
    }
  }


  @Test
  public void testGetReaderByTimestamp()
      throws QueryProcessException, StorageEngineException, IOException, StorageGroupNotSetException {
    System.out.println("Start testGetReaderByTimestamp()");
    mockDataClusterServer = true;
    InsertRowPlan insertPlan = new InsertRowPlan();
    insertPlan.setNeedInferType(true);
    insertPlan.setMeasurements(new String[]{TestUtils.getTestMeasurement(0)});
    insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);
    for (int i = 0; i < 10; i++) {
      insertPlan.setDeviceId(TestUtils.getTestSg(i));
      MeasurementSchema schema = TestUtils.getTestMeasurementSchema(0);
      try {
        IoTDB.metaManager.createTimeseries(schema.getMeasurementId(), schema.getType()
            , schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      } catch (MetadataException e) {
        // ignore
      }
      for (int j = 0; j < 10; j++) {
        insertPlan.setTime(j);
        insertPlan.setValues(new Object[]{String.valueOf(j)});
        insertPlan.setSchemasAndTransferType(new MeasurementSchema[]{TestUtils.getTestMeasurementSchema( 0)});
        planExecutor.processNonQuery(insertPlan);
      }
    }

    QueryContext context = new RemoteQueryContext(
        QueryResourceManager.getInstance().assignQueryId(true));

    try {
      for (int i = 0; i < 10; i++) {
        IReaderByTimestamp readerByTimestamp = testMetaMember
            .getReaderByTimestamp(new Path(TestUtils.getTestSeries(i, 0)),
                Collections.singleton(TestUtils.getTestMeasurement(0)), TSDataType.DOUBLE,
                context);
        for (int j = 0; j < 10; j++) {
          assertEquals(j * 1.0, (double) readerByTimestamp.getValueInTimestamp(j), 0.00001);
        }
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testGetReader()
      throws QueryProcessException, StorageEngineException, IOException, StorageGroupNotSetException {
    System.out.println("Start testGetReader()");
    mockDataClusterServer = true;
    InsertRowPlan insertPlan = new InsertRowPlan();
    insertPlan.setNeedInferType(true);
    insertPlan.setMeasurements(new String[]{TestUtils.getTestMeasurement(0)});
    insertPlan.setDataTypes(new TSDataType[insertPlan.getMeasurements().length]);

    for (int i = 0; i < 10; i++) {
      insertPlan.setDeviceId(TestUtils.getTestSg(i));
      MeasurementSchema schema = TestUtils.getTestMeasurementSchema(0);
      try {
        IoTDB.metaManager.createTimeseries(schema.getMeasurementId(), schema.getType()
            , schema.getEncodingType(), schema.getCompressor(), schema.getProps());
      } catch (MetadataException e) {
        // ignore
      }
      for (int j = 0; j < 10; j++) {
        insertPlan.setTime(j);
        insertPlan.setValues(new Object[]{String.valueOf(j)});
        insertPlan.setSchemasAndTransferType(new MeasurementSchema[]{TestUtils.getTestMeasurementSchema(0)});
        planExecutor.processNonQuery(insertPlan);
      }
    }

    QueryContext context = new RemoteQueryContext(
        QueryResourceManager.getInstance().assignQueryId(true));

    try {
      for (int i = 0; i < 10; i++) {
        ManagedSeriesReader reader = testMetaMember
            .getSeriesReader(new Path(TestUtils.getTestSeries(i, 0)),
                Collections.singleton(TestUtils.getTestMeasurement(0)), TSDataType.DOUBLE,
                TimeFilter.gtEq(5),
                ValueFilter.ltEq(8.0), context);
        assertTrue(reader.hasNextBatch());
        BatchData batchData = reader.nextBatch();
        for (int j = 5; j < 9; j++) {
          assertTrue(batchData.hasCurrent());
          assertEquals(j, batchData.currentTime());
          assertEquals(j * 1.0, batchData.getDouble(), 0.00001);
          batchData.next();
        }
        assertFalse(batchData.hasCurrent());
        assertFalse(reader.hasNextBatch());
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testGetMatchedPaths() throws MetadataException {
    System.out.println("Start testGetMatchedPaths()");
    List<String> matchedPaths = testMetaMember
        .getMatchedPaths(TestUtils.getTestSg(0) + ".*");
    assertEquals(20, matchedPaths.size());
    for (int j = 0; j < 10; j++) {
      assertEquals(TestUtils.getTestSeries(0, j), matchedPaths.get(j));
    }
    matchedPaths = testMetaMember
        .getMatchedPaths(TestUtils.getTestSg(10) + ".*");
    assertTrue(matchedPaths.isEmpty());
  }

  @Test
  public void testProcessValidHeartbeatReq() throws QueryProcessException {
    System.out.println("Start testProcessValidHeartbeatReq()");
    MetaGroupMember testMetaMember = getMetaGroupMember(TestUtils.getNode(10));
    try {
      HeartBeatRequest request = new HeartBeatRequest();
      request.setRequireIdentifier(true);
      HeartBeatResponse response = new HeartBeatResponse();
      testMetaMember.processValidHeartbeatReq(request, response);
      assertEquals(10, response.getFollowerIdentifier());

      request.setRegenerateIdentifier(true);
      testMetaMember.processValidHeartbeatReq(request, response);
      assertNotEquals(10, response.getFollowerIdentifier());
      assertTrue(response.isRequirePartitionTable());

      request.setPartitionTableBytes(partitionTable.serialize());
      testMetaMember.processValidHeartbeatReq(request, response);
      assertEquals(partitionTable, testMetaMember.getPartitionTable());
    } finally {
      testMetaMember.stop();
    }
  }

  @Test
  public void testProcessValidHeartbeatResp()
      throws QueryProcessException {
    System.out.println("Start testProcessValidHeartbeatResp()");
    MetaGroupMember metaGroupMember = getMetaGroupMember(TestUtils.getNode(10));
    metaGroupMember.start();
    metaGroupMember.onElectionWins();
    try {
      for (int i = 0; i < 10; i++) {
        HeartBeatResponse response = new HeartBeatResponse();
        response.setFollowerIdentifier(i);
        response.setRequirePartitionTable(true);
        metaGroupMember.processValidHeartbeatResp(response, TestUtils.getNode(i));
        metaGroupMember.removeBlindNode(TestUtils.getNode(i));
      }
      assertNotNull(metaGroupMember.getPartitionTable());
    } finally {
      metaGroupMember.stop();
    }
  }

  @Test
  public void testAppendEntry() {
    System.out.println("Start testAppendEntry()");
    System.out.println("Term before append: " + testMetaMember.getTerm().get());

    testMetaMember.setPartitionTable(null);
    CloseFileLog log = new CloseFileLog(TestUtils.getTestSg(0), 0, true);
    log.setCurrLogIndex(0);
    log.setCurrLogTerm(0);
    AppendEntryRequest request = new AppendEntryRequest();
    request.setEntry(log.serialize());
    request.setTerm(0);
    request.setLeaderCommit(0);
    request.setPrevLogIndex(-1);
    request.setPrevLogTerm(-1);
    request.setLeader(new Node("127.0.0.1", 30000, 0, 40000));
    AtomicReference<Long> result = new AtomicReference<>();
    GenericHandler<Long> handler = new GenericHandler<>(TestUtils.getNode(0), result);
    testMetaMember.appendEntry(request, handler);
    assertEquals(Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE, (long) result.get());
    System.out.println("Term after first append: " + testMetaMember.getTerm().get());

    testMetaMember.setPartitionTable(partitionTable);
    testMetaMember.appendEntry(request, handler);
    System.out.println("Term after second append: " + testMetaMember.getTerm().get());
    assertEquals(Response.RESPONSE_AGREE, (long) result.get());
  }

  @Test
  public void testRemoteAddNode() {
    System.out.println("Start testRemoteAddNode()");
    int prevTimeout = RaftServer.getConnectionTimeoutInMS();
    RaftServer.setConnectionTimeoutInMS(100);
    try {
      // cannot add node when partition table is not built
      testMetaMember.setPartitionTable(null);
      AtomicReference<AddNodeResponse> result = new AtomicReference<>();
      GenericHandler<AddNodeResponse> handler = new GenericHandler<>(TestUtils.getNode(0), result);
      testMetaMember.addNode(TestUtils.getNode(10), TestUtils.getStartUpStatus(), handler);
      AddNodeResponse response = result.get();
      assertEquals(Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE, response.getRespNum());

      // cannot add itself
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      testMetaMember.addNode(TestUtils.getNode(0), TestUtils.getStartUpStatus(), handler);
      assertNull(result.get());

      // process the request as a leader
      testMetaMember.setCharacter(LEADER);
      testMetaMember.onElectionWins();
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      testMetaMember.addNode(TestUtils.getNode(10), TestUtils.getStartUpStatus(), handler);
      response = result.get();
      assertEquals(Response.RESPONSE_AGREE, response.getRespNum());
      assertEquals(partitionTable.serialize(), response.partitionTableBytes);

      // adding an existing node is ok
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      testMetaMember.addNode(TestUtils.getNode(10), TestUtils.getStartUpStatus(), handler);
      response = result.get();
      assertEquals(Response.RESPONSE_AGREE, response.getRespNum());
      assertEquals(partitionTable.serialize(), response.partitionTableBytes);

      // process the request as a follower
      testMetaMember.setCharacter(FOLLOWER);
      testMetaMember.setLeader(TestUtils.getNode(1));
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      testMetaMember.addNode(TestUtils.getNode(11), TestUtils.getStartUpStatus(), handler);
      while (result.get() == null) {

      }
      response = result.get();
      assertEquals(Response.RESPONSE_AGREE, response.getRespNum());
      assertEquals(partitionTable.serialize(), response.partitionTableBytes);

      // cannot add a node with conflict id
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      Node node = TestUtils.getNode(12).setNodeIdentifier(10);
      testMetaMember.addNode(node, TestUtils.getStartUpStatus(), handler);
      response = result.get();
      assertEquals(Response.RESPONSE_IDENTIFIER_CONFLICT, response.getRespNum());

      //  cannot add a node due to configuration conflict, partition interval
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      node = TestUtils.getNode(13);
      StartUpStatus startUpStatus = TestUtils.getStartUpStatus();
      startUpStatus.setPartitionInterval(-1);
      testMetaMember.addNode(node, startUpStatus, handler);
      response = result.get();
      assertEquals(Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT, response.getRespNum());
      assertFalse(response.getCheckStatusResponse().isPartitionalIntervalEquals());
      assertTrue(response.getCheckStatusResponse().isHashSaltEquals());
      assertTrue(response.getCheckStatusResponse().isReplicationNumEquals());

      // cannot add a node due to configuration conflict, hash salt
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      node = TestUtils.getNode(12);
      startUpStatus = TestUtils.getStartUpStatus();
      startUpStatus.setHashSalt(0);
      testMetaMember.addNode(node, startUpStatus, handler);
      response = result.get();
      assertEquals(Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT, response.getRespNum());
      assertTrue(response.getCheckStatusResponse().isPartitionalIntervalEquals());
      assertFalse(response.getCheckStatusResponse().isHashSaltEquals());
      assertTrue(response.getCheckStatusResponse().isReplicationNumEquals());

      // cannot add a node due to configuration conflict, replication number
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      node = TestUtils.getNode(12);
      startUpStatus = TestUtils.getStartUpStatus();
      startUpStatus.setReplicationNumber(0);
      testMetaMember.addNode(node, startUpStatus, handler);
      response = result.get();
      assertEquals(Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT, response.getRespNum());
      assertTrue(response.getCheckStatusResponse().isPartitionalIntervalEquals());
      assertTrue(response.getCheckStatusResponse().isHashSaltEquals());
      assertFalse(response.getCheckStatusResponse().isReplicationNumEquals());

      // cannot add a node due to network failure
      dummyResponse.set(Response.RESPONSE_NO_CONNECTION);
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      new Thread(() -> {
        await().atLeast(200, TimeUnit.MILLISECONDS);
        dummyResponse.set(Response.RESPONSE_AGREE);
      }).start();
      testMetaMember.addNode(TestUtils.getNode(12), TestUtils.getStartUpStatus(), handler);
      response = result.get();
      assertEquals(Response.RESPONSE_AGREE, response.getRespNum());

      // cannot add a node due to leadership lost
      dummyResponse.set(100);
      testMetaMember.setCharacter(LEADER);
      result.set(null);
      testMetaMember.setPartitionTable(partitionTable);
      testMetaMember.addNode(TestUtils.getNode(13), TestUtils.getStartUpStatus(), handler);
      response = result.get();
      assertNull(response);

    } finally {
      testMetaMember.stop();
      RaftServer.setConnectionTimeoutInMS(prevTimeout);
    }
  }

  @Test
  public void testLoadIdentifier() throws IOException, QueryProcessException {
    System.out.println("Start testLoadIdentifier()");
    try (RandomAccessFile raf = new RandomAccessFile(MetaGroupMember.NODE_IDENTIFIER_FILE_NAME,
        "rw")) {
      raf.writeBytes("100");
    }
    MetaGroupMember metaGroupMember = getMetaGroupMember(new Node());
    assertEquals(100, metaGroupMember.getThisNode().getNodeIdentifier());
    metaGroupMember.closeLogManager();
  }

  @Test
  public void testRemoveNodeWithoutPartitionTable() {
    System.out.println("Start testRemoveNodeWithoutPartitionTable()");
    testMetaMember.setPartitionTable(null);
    AtomicBoolean passed = new AtomicBoolean(false);
    testMetaMember.removeNode(TestUtils.getNode(0), new AsyncMethodCallback<Long>() {
      @Override
      public void onComplete(Long aLong) {

      }

      @Override
      public void onError(Exception e) {
        passed.set(e instanceof PartitionTableUnavailableException);
      }
    });

    assertTrue(passed.get());
  }

  @Test
  public void testRemoveThisNode() {
    System.out.println("Start testRemoveThisNode()");
    AtomicReference<Long> resultRef = new AtomicReference<>();
    testMetaMember.setLeader(testMetaMember.getThisNode());
    testMetaMember.setCharacter(LEADER);
    doRemoveNode(resultRef, testMetaMember.getThisNode());
    assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
    assertFalse(testMetaMember.getAllNodes().contains(testMetaMember.getThisNode()));
  }

  @Test
  public void testRemoveLeader() {
    System.out.println("Start testRemoveLeader()");
    AtomicReference<Long> resultRef = new AtomicReference<>();
    testMetaMember.setLeader(TestUtils.getNode(40));
    testMetaMember.setCharacter(FOLLOWER);
    doRemoveNode(resultRef, TestUtils.getNode(40));
    assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
    assertFalse(testMetaMember.getAllNodes().contains(TestUtils.getNode(40)));
    assertEquals(ELECTOR, testMetaMember.getCharacter());
    assertEquals(Long.MIN_VALUE, testMetaMember.getLastHeartbeatReceivedTime());
  }

  @Test
  public void testRemoveNonLeader() {
    System.out.println("Start testRemoveNonLeader()");
    AtomicReference<Long> resultRef = new AtomicReference<>();
    testMetaMember.setLeader(TestUtils.getNode(40));
    testMetaMember.setCharacter(FOLLOWER);
    doRemoveNode(resultRef, TestUtils.getNode(20));
    assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
    assertFalse(testMetaMember.getAllNodes().contains(TestUtils.getNode(20)));
    assertEquals(0, testMetaMember.getLastHeartbeatReceivedTime());
  }

  @Test
  public void testRemoveNodeAsLeader() {
    System.out.println("Start testRemoveNodeAsLeader()");
    AtomicReference<Long> resultRef = new AtomicReference<>();
    testMetaMember.setLeader(testMetaMember.getThisNode());
    testMetaMember.setCharacter(LEADER);
    doRemoveNode(resultRef, TestUtils.getNode(20));
    assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
    assertFalse(testMetaMember.getAllNodes().contains(TestUtils.getNode(20)));
    System.out.println("Checking exiled node in testRemoveNodeAsLeader()");
    assertEquals(TestUtils.getNode(20), exiledNode);
  }

  @Test
  public void testRemoveNonExistNode() {
    System.out.println("Start testRemoveNonExistNode()");
    AtomicBoolean passed = new AtomicBoolean(false);
    testMetaMember.setCharacter(LEADER);
    testMetaMember.setLeader(testMetaMember.getThisNode());
    testMetaMember.removeNode(TestUtils.getNode(120), new AsyncMethodCallback<Long>() {
      @Override
      public void onComplete(Long aLong) {
        passed.set(aLong.equals(Response.RESPONSE_REJECT));
      }

      @Override
      public void onError(Exception e) {
        e.printStackTrace();
      }
    });

    assertTrue(passed.get());
  }

  @Test
  public void testRemoveTooManyNodes() {
    System.out.println("Start testRemoveTooManyNodes()");
    for (int i = 0; i < 8; i++) {
      AtomicReference<Long> resultRef = new AtomicReference<>();
      testMetaMember.setCharacter(LEADER);
      testMetaMember.setLeader(testMetaMember.getThisNode());
      doRemoveNode(resultRef, TestUtils.getNode(90 - i * 10));
      assertEquals(Response.RESPONSE_AGREE, (long) resultRef.get());
      assertFalse(testMetaMember.getAllNodes().contains(TestUtils.getNode(90 - i * 10)));
    }
    AtomicReference<Long> resultRef = new AtomicReference<>();
    testMetaMember.setCharacter(LEADER);
    doRemoveNode(resultRef, TestUtils.getNode(10));
    assertEquals(Response.RESPONSE_CLUSTER_TOO_SMALL, (long) resultRef.get());
    assertTrue(testMetaMember.getAllNodes().contains(TestUtils.getNode(10)));
  }

  private void doRemoveNode(AtomicReference<Long> resultRef, Node nodeToRemove) {
    testMetaMember.removeNode(nodeToRemove, new AsyncMethodCallback<Long>() {
      @Override
      public void onComplete(Long o) {
        resultRef.set(o);
      }

      @Override
      public void onError(Exception e) {
        e.printStackTrace();
      }
    });
    while (resultRef.get() == null) {

    }
  }
}