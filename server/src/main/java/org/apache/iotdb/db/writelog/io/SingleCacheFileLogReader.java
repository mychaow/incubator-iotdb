package org.apache.iotdb.db.writelog.io;

import org.apache.iotdb.db.writelog.node.CacheLog;

import java.nio.ByteBuffer;

public class SingleCacheFileLogReader extends AbstractSingleFileLogReader<CacheLog> {

  @Override
  protected ILogReader<CacheLog> newLogReader(ByteBuffer buf) {
    return ;
  }
}
