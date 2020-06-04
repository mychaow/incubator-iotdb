package org.apache.iotdb.db.writelog.node;

import java.nio.ByteBuffer;

public interface ISerialize {

  void serialize(ByteBuffer buffer);

  void deserialize(ByteBuffer buffer);

  ISerialize createFrom(ByteBuffer buffer);
}
