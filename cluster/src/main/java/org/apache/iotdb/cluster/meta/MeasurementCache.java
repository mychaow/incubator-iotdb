package org.apache.iotdb.cluster.meta;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.*;
import org.apache.iotdb.db.metadata.mnode.LeafMNode;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * manage the measurementSchema & last value cache
 * maybe support tag/attribute cache later
 * <p>
 * only cache the measurement which has data in local
 */
public class MeasurementCache {

  private static final Logger logger = LoggerFactory.getLogger(MeasurementCache.class);
  private static final String TIME_SERIES_CACHE_HEADER = "c44901bb";

  // the lock for read/insert
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private Map<String, SGCache> caches;
  private boolean initialized;
  private IoTDBConfig config;

  public MeasurementCache() {
    config = IoTDBDescriptor.getInstance().getConfig();
    String schemaDir = config.getSchemaDir();
    File schemaFolder = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!schemaFolder.exists()) {
      if (schemaFolder.mkdirs()) {
        logger.info("create system folder {}", schemaFolder.getAbsolutePath());
      } else {
        logger.info("create system folder {} failed.", schemaFolder.getAbsolutePath());
      }
    }
  }

  private static class MeasurementCacheHolder {

    private MeasurementCacheHolder() {
      // allowed to do nothing
    }

    private static final MeasurementCache INSTANCE = new MeasurementCache();
  }

  public static MeasurementCache getInstance() {
    return MeasurementCacheHolder.INSTANCE;
  }

  public void cacheMeta(String key, MeasurementMeta meta) throws StorageGroupNotSetException {
    caches.get(IoTDB.getMManager().getStorageGroupName(key)).cacheMeta(key, meta);
  }

  public MeasurementMeta getMeta(String key) throws IOException, StorageGroupNotSetException {
    return caches.get(IoTDB.getMManager().getStorageGroupName(key)).getMeta(key);
  }

  public void removeSeries(String key) {
    for (Map.Entry<String, SGCache> entry : caches.entrySet()) {
      entry.getValue().removeSeries(key);
    }
  }

  public void updateCachedLast(String sg, String key, TimeValuePair timeValuePair,
                               boolean highPriorityUpdate, Long latestFlushedTime) {
    if (timeValuePair == null || timeValuePair.getValue() == null) return;

    try {
      MeasurementMeta meta = caches.get(sg).getMeta(key);
      TimeValuePair pair = meta.getLastCache();
      pair = LeafMNode.updateLastValue(pair, timeValuePair, highPriorityUpdate, latestFlushedTime);
      meta.setLastCache(pair);
    } catch (IOException e) {
      // not in the cache
    }
  }

  public TimeValuePair getCachedLast(String sg, String key) {
    try {
      MeasurementMeta meta = caches.get(sg).getMeta(key);
      return meta.getLastCache();
    } catch (IOException e) {
      // not in the cache
    }
    return null;
  }

  public void removeSg(String sg) {
    SGCache cache = caches.get(sg);
    if (cache == null) {
      return;
    }
    cache.clear();
  }

  public void addSg(String sg) {
    SGCache cache = caches.get(sg);
    if (cache != null) {
      return;
    }
    cache = new SGCache(sg);
    cache.init();
    caches.put(sg, cache);
  }

  public synchronized void init() {
    if (initialized) {
      return;
    }

    List<String> sgs = IoTDB.getMManager().getAllStorageGroupNames();
    for (String sg : sgs) {
      SGCache cache = new SGCache(sg);
      cache.init();
      caches.put(sg, cache);
    }

    initialized = true;
  }

  public void clear() {
    for (Map.Entry<String, SGCache> entry : caches.entrySet()) {
      entry.getValue().clear();
    }
    caches.clear();
    initialized = false;
  }

  public void Serialize() {

  }

  public void Deserialize() {

  }
}
