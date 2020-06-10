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
package org.apache.iotdb.db.metadata.mnode;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.Collections;
import java.util.Map;

public class LeafMNode extends MNode {

  private static final long serialVersionUID = -1199657856921206435L;

  /**
   * measurement's Schema for one timeseries represented by current leaf node
   */
  private MeasurementSchema schema;
  private String alias;
  // tag/attribute's start offset in tag file
  private long offset = -1;

  private TimeValuePair cachedLastValuePair = null;

  /**
   * @param alias alias of measurementName
   */
  public LeafMNode(MNode parent, String measurementName, String alias, TSDataType dataType,
      TSEncoding encoding, CompressionType type, Map<String, String> props) {
    super(parent, measurementName);
    this.schema = new MeasurementSchema(measurementName, dataType, encoding, type, props);
    this.alias = alias;
  }

  @Override
  public boolean hasChild(String name) {
    return false;
  }

  @Override
  public void addChild(String name, MNode child) {
    // Do nothing
  }

  @Override
  public void deleteChild(String name) {
    // Do nothing
  }

  @Override
  public void deleteAliasChild(String alias) {
    // Do nothing
  }

  @Override
  public MNode getChild(String name) {
    return null;
  }

  @Override
  public int getLeafCount() {
    return 1;
  }

  @Override
  public void addAlias(String alias, MNode child) {
    // Do nothing
  }

  @Override
  public Map<String, MNode> getChildren() {
    return Collections.emptyMap();
  }

  public MeasurementSchema getSchema() {
    return schema;
  }

  public TimeValuePair getCachedLast() {
    return cachedLastValuePair;
  }

  public synchronized void updateCachedLast(
      TimeValuePair timeValuePair, boolean highPriorityUpdate, Long latestFlushedTime) {
    TimeValuePair ret = LeafMNode.updateLastValue(cachedLastValuePair, timeValuePair,
      highPriorityUpdate, latestFlushedTime);

    if (ret != null) {
      cachedLastValuePair = ret;
    }
  }

  public String getFullPath() {
    return concatFullPath();
  }

  public void resetCache() {
    cachedLastValuePair = null;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public static TimeValuePair updateLastValue(TimeValuePair oldPair, TimeValuePair timeValuePair,
                                          boolean highPriorityUpdate, Long latestFlushedTime) {
    if (timeValuePair == null || timeValuePair.getValue() == null) return null;

    TimeValuePair retPair = null;
    if (oldPair == null) {
      // If no cached last, (1) a last query (2) an unseq insertion or (3) a seq insertion will update cache.
      if (!highPriorityUpdate || latestFlushedTime <= timeValuePair.getTimestamp()) {
        retPair =
          new TimeValuePair(timeValuePair.getTimestamp(), timeValuePair.getValue());
      }
    } else if (timeValuePair.getTimestamp() > oldPair.getTimestamp()
      || (timeValuePair.getTimestamp() == oldPair.getTimestamp()
      && highPriorityUpdate)) {
      retPair =
        new TimeValuePair(timeValuePair.getTimestamp(), timeValuePair.getValue());
    }
    return retPair;
  }
}