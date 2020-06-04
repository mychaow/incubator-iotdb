package org.apache.iotdb.db.writelog.node;

public class CacheWriteLogNode<CacheLog> extends AbstractWriteLogNode<CacheLog> {

  public static final String RCACHE_FILE_NAME = "rcache";
  
  /**
   * constructor of CacheWriteLogNode.
   *
   * @param identifier CacheWriteLogNode identifier
   */
  public CacheWriteLogNode(String identifier) {
    super(identifier);
  }

  @Override
  protected String getPrefix() {
    return RCACHE_FILE_NAME;
  }

  @Override
  protected String getFolder() {
    return config.getRCacheFolder();
  }
}
