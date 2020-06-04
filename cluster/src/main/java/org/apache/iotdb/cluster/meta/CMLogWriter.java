package org.apache.iotdb.cluster.meta;

import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.metadata.MeasurementMeta;
import org.apache.iotdb.db.metadata.MetadataOperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

public class CMLogWriter {
  private static final Logger logger = LoggerFactory.getLogger(CMLogWriter.class);
  private BufferedWriter writer;

  public CMLogWriter(String schemaDir, String logFileName) throws IOException {
    File metadataDir = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!metadataDir.exists()) {
      if (metadataDir.mkdirs()) {
        logger.info("create schema folder {}.", metadataDir);
      } else {
        logger.info("create schema folder {} failed.", metadataDir);
      }
    }

    File logFile = SystemFileFactory.INSTANCE.getFile(schemaDir + File.separator + logFileName);

    FileWriter fileWriter;
    fileWriter = new FileWriter(logFile, true);
    writer = new BufferedWriter(fileWriter);
  }

  public void close() throws IOException {
    writer.close();
  }

  public void cacheTimeSeries(String key, MeasurementMeta meta) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    meta.getSchema().serializeTo(dataOutputStream);
    // measurementId, schema, alias
    writer.write(String.format("%s,%s,%s", MetadataOperationType.CREATE_TIMESERIES, key, byteArrayOutputStream.toString()));

    writer.write(",");

    if (meta.getAlias() != null) {
      writer.write(meta.getAlias());
    }
    writer.newLine();
    writer.flush();
  }

  public void deleteTimeseries(String path) throws IOException {
    writer.write(MetadataOperationType.DELETE_TIMESERIES + "," + path);
    writer.newLine();
    writer.flush();
  }
}
