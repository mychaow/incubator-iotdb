package org.apache.iotdb.db.writelog.manager;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.writelog.node.CacheLog;
import org.apache.iotdb.db.writelog.node.CacheWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;

public class MultiCacheLogNodeManager extends AbstractMultiLogNodeManager {

  private MultiCacheLogNodeManager() {
    super();
    name = "MultiCacheLogNodeManager";
  }

  public static MultiCacheLogNodeManager getInstance() {
    return MultiCacheLogNodeManager.InstanceHolder.instance;
  }

  @Override
  protected boolean isEnable() {
    return config.isEnableRCache();
  }

  @Override
  protected long getForceFlushPeriodInMs() {
    return config.getForceRCachePeriodInMs();
  }

  @Override
  protected String getForceDaemonName() {
    return ThreadName.RCACHE_FORCE_DAEMON.getName();
  }

  @Override
  protected WriteLogNode newWriteLogNode(String identifier) {
    return new CacheWriteLogNode<CacheLog>(identifier);
  }

  @Override
  public ServiceType getID() {
    return ServiceType.RCACHE_SERVICE;
  }

  private static class InstanceHolder {
    private InstanceHolder(){}

    private static MultiCacheLogNodeManager instance = new MultiCacheLogNodeManager();
  }
}
