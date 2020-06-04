package org.apache.iotdb.db.metadata;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * used to accelerate the insert when meta is not owned by self
 * only store MeasurementSchema & alias meta
 *    we used alias to check whether cached the schema
 */
public class MeasurementMeta {
  private MeasurementSchema schema = null;
  private TimeValuePair lastCache = null;
  private String alias = null;
  // maybe we will use tags&attributeds in future

  public MeasurementMeta() {
  }

  public MeasurementSchema getSchema() {
    return schema;
  }

  public void setSchema(MeasurementSchema schema) {
    this.schema = schema;
  }

  public TimeValuePair getLastCache() {
    return lastCache;
  }

  public void setLastCache(TimeValuePair lastCache) {
    this.lastCache = lastCache;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  /**
   * function for deserializing data from byte buffer.
   */
  public static MeasurementMeta deserializeFrom(ByteBuffer buffer) {
    MeasurementMeta measurementMeta = new MeasurementMeta();

    measurementMeta.schema = MeasurementSchema.deserializeFrom(buffer);
    int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      measurementMeta.alias = ReadWriteIOUtils.readString(buffer);
    }

    return measurementMeta;
  }

  /**
   * function for serializing data to output stream.
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    byteLen += schema.serializeTo(outputStream);

    if (alias == null) {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    } else {
      byteLen += ReadWriteIOUtils.write(alias, outputStream);
    }

    return byteLen;
  }
}
