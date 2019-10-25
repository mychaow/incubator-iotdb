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

package org.apache.iotdb.db.engine.merge.inplace.recover;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * MergeLogger records the progress of a merge in file "merge.log" as text lines.
 */
public class MergeLogger {

  public static final String MERGE_LOG_NAME = "merge.log";

  static final String STR_SEQ_FILES = "seqFiles";
  static final String STR_UNSEQ_FILES = "unseqFiles";
  static final String STR_TIMESERIES = "timeseries";
  static final String STR_START = "start";
  static final String STR_END = "end";
  static final String STR_ALL_TS_END = "all ts end";
  static final String STR_MERGE_START = "merge start";
  static final String STR_MERGE_END = "merge end";

  private BufferedWriter logStream;

  public MergeLogger(String storageGroupDir) throws IOException {
    logStream = new BufferedWriter(new FileWriter(SystemFileFactory.INSTANCE.getFile(storageGroupDir,
      MERGE_LOG_NAME), true));
  }

  public void close() throws IOException {
    logStream.close();
  }

  public void logTSStart(List<Path> paths) throws IOException {
    logStream.write(STR_START);
    for (Path path : paths) {
      logStream.write(" " + path.getFullPath());
    }
    logStream.newLine();
    logStream.flush();
  }

  public void logFilePosition(File file) throws IOException {
    logStream.write(String.format("%s %d", file.getAbsolutePath(), file.length()));
    logStream.newLine();
    logStream.flush();
  }

  public void logTSEnd() throws IOException {
    logStream.write(STR_END);
    logStream.newLine();
    logStream.flush();
  }

  public void logAllTsEnd() throws IOException {
    logStream.write(STR_ALL_TS_END);
    logStream.newLine();
    logStream.flush();
  }

  public void logFileMergeStart(File file, long position) throws IOException {
    logStream.write(String.format("%s %d", file.getAbsolutePath(), position));
    logStream.newLine();
    logStream.flush();
  }

  public void logFileMergeEnd() throws IOException {
    logStream.write(STR_END);
    logStream.newLine();
    logStream.flush();
  }

  public void logMergeEnd() throws IOException {
    logStream.write(STR_MERGE_END);
    logStream.newLine();
    logStream.flush();
  }

  public void logFiles(MergeResource resource) throws IOException {
    logSeqFiles(resource.getSeqFiles());
    logUnseqFiles(resource.getUnseqFiles());
  }

  private void logSeqFiles(List<TsFileResource> seqFiles) throws IOException {
    logStream.write(STR_SEQ_FILES);
    logStream.newLine();
    for (TsFileResource tsFileResource : seqFiles) {
      logStream.write(tsFileResource.getFile().getAbsolutePath());
      logStream.newLine();
    }
    logStream.flush();
  }

  private void logUnseqFiles(List<TsFileResource> unseqFiles) throws IOException {
    logStream.write(STR_UNSEQ_FILES);
    logStream.newLine();
    for (TsFileResource tsFileResource : unseqFiles) {
      logStream.write(tsFileResource.getFile().getAbsolutePath());
      logStream.newLine();
    }
    logStream.flush();
  }

  public void logMergeStart() throws IOException {
    logStream.write(STR_MERGE_START);
    logStream.newLine();
    logStream.flush();
  }
}
