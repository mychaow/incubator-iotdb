package org.apache.iotdb.db.writelog.manager;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractMultiLogNodeManager implements WriteLogNodeManager, IService {
  protected Logger logger = LoggerFactory.getLogger(AbstractMultiLogNodeManager.class);
  private Map<String, WriteLogNode> nodeMap;

  private Thread forceThread;
  
  protected String name = "AbstractMultiLogNodeManager";
  protected IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Runnable forceTask = () -> {
    while (true) {
      if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
        logger.warn("system mode is read-only, the force flush task is stopped");
        return;
      }
      if (Thread.interrupted()) {
        logger.info("WAL force thread exits.");
        return;
      }

      for (WriteLogNode node : nodeMap.values()) {
        try {
          node.forceSync();
        } catch (IOException e) {
          logger.error("Cannot force {}, because ", node, e);
        }
      }
      try {
        Thread.sleep(getForceFlushPeriodInMs());
      } catch (InterruptedException e) {
        logger.info("WAL force thread exits.");
        Thread.currentThread().interrupt();
        break;
      }
    }
  };

  protected AbstractMultiLogNodeManager() {
    nodeMap = new ConcurrentHashMap<>();
  }

  @Override
  public WriteLogNode getNode(String identifier) {
    WriteLogNode node = nodeMap.get(identifier);
    if (node == null) {
      node = newWriteLogNode(identifier);
      WriteLogNode oldNode = nodeMap.putIfAbsent(identifier, node);
      if (oldNode != null) {
        return oldNode;
      }
    }
    return node;
  }

  @Override
  public void deleteNode(String identifier) throws IOException {
    WriteLogNode node = nodeMap.remove(identifier);
    if (node != null) {
      node.delete();
    }
  }

  @Override
  public void close() {
    if (!isActivated(forceThread)) {
      logger.debug("MultiFileLogNodeManager has not yet started");
      return;
    }
    logger.info("LogNodeManager starts closing..");
    if (isActivated(forceThread)) {
      forceThread.interrupt();
      logger.info("Waiting for force thread to stop");
      while (forceThread.isAlive()) {
        // wait for forceThread
      }
    }
    logger.info("{} nodes to be closed", nodeMap.size());
    for (WriteLogNode node : nodeMap.values()) {
      try {
        node.close();
      } catch (IOException e) {
        logger.error("failed to close {} for {}", node, e);
      }
    }
    nodeMap.clear();
    logger.info("LogNodeManager {} closed.", name);
  }

  @Override
  public void start() throws StartupException {
    try {
      if (!isEnable()) {
        return;
      }
      if (!isActivated(forceThread)) {
        if (getForceFlushPeriodInMs() > 0) {
          forceThread = new Thread(forceTask, getForceDaemonName());
          forceThread.start();
        }
      } else {
        logger.debug("MultiLogNodeManager {} has already started", name);
      }
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    if (!isEnable()) {
      return;
    }
    close();
  }
  
  private boolean isActivated(Thread thread) {
    return thread != null && thread.isAlive();
  }

  protected abstract boolean isEnable();

  protected abstract long getForceFlushPeriodInMs();
  
  protected abstract String getForceDaemonName();

  protected abstract WriteLogNode newWriteLogNode(String identifier);
}
