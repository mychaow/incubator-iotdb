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

package org.apache.iotdb.db.writelog.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchedLogReader reads logs from a binary batch of log in the format of ByteBuffer. The
 * ByteBuffer must be readable.
 */
public class BatchLogReader<T> implements ILogReader<T> {

  private static Logger logger = LoggerFactory.getLogger(BatchLogReader.class);

  private Iterator<T> logIterator;

  private boolean fileCorrupted = false;

  BatchLogReader(ByteBuffer buffer) {
    List<T> logs = readLogs(buffer);
    this.logIterator = logs.iterator();
  }

  private List<T> readLogs(ByteBuffer buffer) {
    List<T> logs = new ArrayList<>();
    while (buffer.position() != buffer.limit()) {
      try {
        logs.add(Factory.create(buffer));
      } catch (IOException e) {
        logger.error("Cannot deserialize PhysicalPlans from ByteBuffer, ignore remaining logs", e);
        fileCorrupted = true;
        break;
      }
    }
    return logs;
  }


  @Override
  public void close() {
    // nothing to be closed
  }

  @Override
  public boolean hasNext() {
    return logIterator.hasNext();
  }

  @Override
  public T next() {
    return logIterator.next();
  }

  public boolean isFileCorrupted() {
    return fileCorrupted;
  }
}
