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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.client.async.DataClient;
import org.apache.iotdb.cluster.common.EnvironmentUtils;
import org.apache.iotdb.cluster.common.TestDataClient;
import org.apache.iotdb.cluster.common.TestDataGroupMember;
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestMetaClient;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestPartitionedLogManager;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.manage.RaftLogManager;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.After;
import org.junit.Before;

public class MemberTest {

  public static AtomicLong dummyResponse = new AtomicLong(Response.RESPONSE_AGREE);

  protected Map<Node, DataGroupMember> dataGroupMemberMap;
  protected Map<Node, MetaGroupMember> metaGroupMemberMap;
  protected PartitionGroup allNodes;
  protected MetaGroupMember testMetaMember;
  RaftLogManager metaLogManager;
  PartitionTable partitionTable;
  PlanExecutor planExecutor;

  private List<String> prevUrls;
  private long prevLeaderWait;

  @Before
  public void setUp() throws Exception {
    prevLeaderWait = RaftMember.getWaitLeaderTimeMs();
    RaftMember.setWaitLeaderTimeMs(10);
    EnvironmentUtils.envSetUp();
    prevUrls = ClusterDescriptor.getInstance().getConfig().getSeedNodeUrls();
    List<String> testUrls = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Node node = TestUtils.getNode(i);
      testUrls.add(node.getIp() + ":" + node.getMetaPort() + ":" + node.getDataPort());
    }
    ClusterDescriptor.getInstance().getConfig().setSeedNodeUrls(testUrls);

    allNodes = new PartitionGroup();
    for (int i = 0; i < 100; i += 10) {
      allNodes.add(TestUtils.getNode(i));
    }

    partitionTable = new SlotPartitionTable(allNodes, TestUtils.getNode(0));

    dataGroupMemberMap = new HashMap<>();
    metaGroupMemberMap = new HashMap<>();
    metaLogManager = new TestLogManager(1);
    testMetaMember = getMetaGroupMember(TestUtils.getNode(0));
    for (Node node : allNodes) {
      // pre-create data members
      getDataGroupMember(node);
    }

    for (int i = 0; i < 10; i++) {
      try {
        IoTDB.getMManager().setStorageGroup(TestUtils.getTestSg(i));
        for (int j = 0; j < 20; j++) {
          SchemaUtils.registerTimeseries(TestUtils.getTestTimeSeriesSchema(i, j));
        }
      } catch (MetadataException e) {
        // ignore
      }
    }
    planExecutor = new PlanExecutor();
    testMetaMember.setPartitionTable(partitionTable);
  }

  @After
  public void tearDown() throws Exception {
    testMetaMember.stop();
    metaLogManager.close();
    for (DataGroupMember member : dataGroupMemberMap.values()) {
      member.stop();
    }
    for (MetaGroupMember member : metaGroupMemberMap.values()) {
      member.stop();
    }
    EnvironmentUtils.cleanEnv();
    ClusterDescriptor.getInstance().getConfig().setSeedNodeUrls(prevUrls);
    new File(MetaGroupMember.PARTITION_FILE_NAME).delete();
    new File(MetaGroupMember.NODE_IDENTIFIER_FILE_NAME).delete();
    RaftMember.setWaitLeaderTimeMs(prevLeaderWait);
  }

  DataGroupMember getDataGroupMember(Node node) {
    return dataGroupMemberMap.computeIfAbsent(node, this::newDataGroupMember);
  }

  private DataGroupMember newDataGroupMember(Node node) {
    DataGroupMember newMember = new TestDataGroupMember(node, partitionTable.getHeaderGroup(node)) {
      @Override
      public boolean syncLeader() {
        return true;
      }

      @Override
      public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {
        new Thread(() -> resultHandler.onComplete(Response.RESPONSE_AGREE)).start();
      }

      @Override
      public AsyncClient connectNode(Node node) {
        try {
          return new TestDataClient(node, dataGroupMemberMap);
        } catch (IOException e) {
          return null;
        }
      }
    };
    newMember.setThisNode(node);
    newMember.setMetaGroupMember(testMetaMember);
    newMember.setLeader(node);
    newMember.setCharacter(NodeCharacter.LEADER);
    newMember.setLogManager(new TestPartitionedLogManager());
    return newMember;
  }

  protected MetaGroupMember getMetaGroupMember(Node node) throws QueryProcessException {
    return metaGroupMemberMap.computeIfAbsent(node, this::newMetaGroupMember);
  }

  private MetaGroupMember newMetaGroupMember(Node node) {
    MetaGroupMember ret = new TestMetaGroupMember() {
      @Override
      public List<TSDataType> getSeriesTypesByPath(List<Path> paths, List<String> aggregations)
          throws MetadataException {
        return SchemaUtils.getSeriesTypesByPath(paths, aggregations);
      }

      @Override
      public List<TSDataType> getSeriesTypesByString(List<String> pathStrs, String aggregation)
          throws MetadataException {
        return SchemaUtils.getSeriesTypesByString(pathStrs, aggregation);
      }

      @Override
      public List<String> getMatchedPaths(String pathPattern) throws MetadataException {
        return IoTDB.getMManager().getAllTimeseriesName(pathPattern);
      }

      @Override
      protected DataGroupMember getLocalDataMember(Node header, AsyncMethodCallback resultHandler,
          Object request) {
        return getDataGroupMember(header);
      }

      @Override
      public DataGroupMember getLocalDataMember(Node header) {
        return getDataGroupMember(header);
      }

      @Override
      public AsyncClient connectNode(Node node) {
        try {
          return new TestMetaClient(null, null, node, null) {
            @Override
            public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
              new Thread(() -> resultHandler.onComplete(new TNodeStatus())).start();
            }
          };
        } catch (IOException e) {
          return null;
        }
      }

      @Override
      public DataClient getDataClient(Node node) throws IOException {
        return new TestDataClient(node, dataGroupMemberMap);
      }
    };
    ret.setThisNode(node);
    ret.setPartitionTable(partitionTable);
    ret.setAllNodes(allNodes);
    ret.setLogManager(metaLogManager);
    ret.setLeader(node);
    ret.setCharacter(NodeCharacter.LEADER);
    return ret;
  }
}