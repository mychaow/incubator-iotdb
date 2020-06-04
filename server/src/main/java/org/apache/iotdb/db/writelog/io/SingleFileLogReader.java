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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SingleFileLogReader reads binarized WAL logs from a file through a DataInputStream by scanning
 * the file from head to tail.
 */
public class SingleFileLogReader extends AbstractSingleFileLogReader<PhysicalPlan> {

  private final Logger logger = LoggerFactory.getLogger(SingleFileLogReader.class);

  public SingleFileLogReader(File logFile) throws FileNotFoundException {
    super(logFile);
  }

  @Override
  protected ILogReader<PhysicalPlan> newLogReader(ByteBuffer buf) {
    return new BatchLogReader(buf);
  }
}
