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

package org.apache.iotdb.cluster.server.handlers.caller;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.db.metadata.MeasurementMeta;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullTimeseriesSchemaHandler implements AsyncMethodCallback<PullSchemaResp> {

  private static final Logger logger = LoggerFactory.getLogger(PullTimeseriesSchemaHandler.class);

  private Node owner;
  private List<String> prefixPaths;
  private AtomicReference<List<MeasurementMeta>> timeseriesMetas;

  public PullTimeseriesSchemaHandler(Node owner, List<String> prefixPaths,
      AtomicReference<List<MeasurementMeta>> timeseriesMetas) {
    this.owner = owner;
    this.prefixPaths = prefixPaths;
    this.timeseriesMetas = timeseriesMetas;
  }

  @Override
  public void onComplete(PullSchemaResp response) {
    ByteBuffer buffer = response.schemaBytes;
    int size = buffer.getInt();
    List<MeasurementMeta> metas = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      metas.add(MeasurementMeta.deserializeFrom(buffer));
    }

    synchronized (timeseriesMetas) {
      timeseriesMetas.set(metas);
      timeseriesMetas.notifyAll();
    }
  }

  @Override
  public void onError(Exception exception) {
    logger.error("Cannot pull time series schema of {} from {}", prefixPaths, owner, exception);
    synchronized (timeseriesMetas) {
      timeseriesMetas.notifyAll();
    }
  }
}
