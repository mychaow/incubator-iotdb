package org.apache.iotdb.db.writelog.io;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SerializeFactory<T> {
  public static T create(ByteBuffer buffer) throws IOException {
      if (T instanceof PhysicalPlan) {
        return PhysicalPlan.Factory.create(buffer);
      }
  }
}
