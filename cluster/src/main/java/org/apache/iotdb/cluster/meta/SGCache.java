package org.apache.iotdb.cluster.meta;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MeasurementMeta;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.MetadataOperationType;
import org.apache.iotdb.db.metadata.mnode.LeafMNode;
import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * manage Measurement schema & last value for one storage group
 */
public class SGCache {
  private static final Logger logger = LoggerFactory.getLogger(SGCache.class);
  private static final String TIME_SERIES_CACHE_HEADER = "c44901bb";

  private LRUCache<String, MeasurementMeta> mRemoteSchemaCache;
  private boolean initialized;
  private String sgName;
  private IoTDBConfig config;

  // the lock for read/insert
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  // the log file seriesPath
  private String cacheFilePath;
  private List<String> oldCacheFiles;

  // persistence
  CMLogWriter logWriter;
  private boolean writeToLog;

  public SGCache(String storageGroupName) {
    config = IoTDBDescriptor.getInstance().getConfig();
    sgName = storageGroupName;

    String schemaDir = config.getSchemaDir();
    File schemaFolder = SystemFileFactory.INSTANCE.getFile(schemaDir);
    if (!schemaFolder.exists()) {
      if (schemaFolder.mkdirs()) {
        logger.info("create system folder {}", schemaFolder.getAbsolutePath());
      } else {
        logger.info("create system folder {} failed.", schemaFolder.getAbsolutePath());
      }
    }
    cacheFilePath = schemaDir + File.separator + MetadataConstant.REMOTE_CACHE_FILE_PREFIX + "-" + sgName + ".txt";

    // do not write log when recover
    writeToLog = false;
    oldCacheFiles = new ArrayList<>();

    mRemoteSchemaCache = new LRUCache<String, MeasurementMeta>(config.getmRemoteSchemaCacheSize()) {
      @Override
      protected MeasurementMeta loadObjectByKey(String key) throws IOException {
        throw new IOException();
      }
    };
  }

  public void cacheMeta(String key, MeasurementMeta meta) {
    try {
      if (mRemoteSchemaCache.get(key) != null) {
        return;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (writeToLog) {
      try {
        logWriter.cacheTimeSeries(key, meta);
      } catch (IOException e) {
        logger.error("failed to cache {}", key);
      }
    }
    mRemoteSchemaCache.put(key, meta);
  }

  public MeasurementMeta getMeta(String key) throws IOException {
    return mRemoteSchemaCache.get(key);
  }

  public void removeSeries(String key) {
    if (writeToLog) {
      try {
        logWriter.deleteTimeseries(key);
      } catch (IOException e) {
        logger.error("failed to delete {}", key);
      }
    }
    mRemoteSchemaCache.removeItem(key);
  }

  public void updateCachedLast(String key, TimeValuePair timeValuePair,
                               boolean highPriorityUpdate, Long latestFlushedTime) {
    if (timeValuePair == null || timeValuePair.getValue() == null) return;

    try {
      MeasurementMeta meta = mRemoteSchemaCache.get(key);
      TimeValuePair pair = meta.getLastCache();
      pair = LeafMNode.updateLastValue(pair, timeValuePair, highPriorityUpdate, latestFlushedTime);
      meta.setLastCache(pair);
    } catch (IOException e) {
      // not in the cache
    }
  }

  public TimeValuePair getCachedLast(String key) {
    try {
      MeasurementMeta meta = mRemoteSchemaCache.get(key);
      return meta.getLastCache();
    } catch (IOException e) {
      // not in the cache
    }
    return null;
  }

  public synchronized void init() {
    if (initialized) {
      return;
    }
    File cacheFile = SystemFileFactory.INSTANCE.getFile(cacheFilePath);

    try {
      initFromFile(cacheFile);

      logWriter = new CMLogWriter(config.getSchemaDir(),
        MetadataConstant.REMOTE_CACHE_FILE_PREFIX + "-" + sgName + ".txt");
      writeToLog = true;
    } catch (IOException e) {
      logger.error("Cannot read schema from file, using an empty new one", e);
    }
    initialized = true;
  }

  public void initFromFile(File cacheFile) throws IOException {
    // init the metadata from the operation log
    if (cacheFile.exists()) {
      try (FileReader fr = new FileReader(cacheFile);
           BufferedReader br = new BufferedReader(fr)) {
        String cmd;
        while ((cmd = br.readLine()) != null) {
          try {
            operation(cmd);
          } catch (Exception e) {
            logger.error("Can not operate cmd {}", cmd, e);
          }
        }
      }
    }
  }

  public void operation(String cmd) throws IOException, MetadataException {
    String[] args = cmd.trim().split(",", -1);
    switch (args[0]) {
      case MetadataOperationType.CREATE_TIMESERIES:
        MeasurementMeta newMeta = new MeasurementMeta();
        InputStream inputStream = new ByteArrayInputStream(args[2].getBytes());
        newMeta.setSchema(MeasurementSchema.deserializeFrom(inputStream));

        String alias = null;
        if (!args[3].isEmpty()) {
          alias = args[3];
        }
        newMeta.setAlias(alias);

        cacheMeta(args[1], newMeta);
        break;
      case MetadataOperationType.DELETE_TIMESERIES:
        removeSeries(args[1]);
        break;
      default:
        logger.error("Unrecognizable command {}", cmd);
    }
  }

  public void close() {
    try {
      logWriter.close();
    } catch (IOException e) {
      logger.error("failed to close sg {} cache file, msg:{}", sgName, e.getMessage());
    }
  }

  public void changeToNewCacheFile() {
    close();
    try {
      oldCacheFiles.add(cacheFilePath);
      String fileName = newCacheFileName();
      logWriter = new CMLogWriter(config.getSchemaDir(), fileName);
      cacheFilePath = config.getSchemaDir() + File.separator + fileName;

      // need to serialize old item to new cache file
      // for we may use the old item, but will not store in new cache file
      Map<String, MeasurementMeta> items = mRemoteSchemaCache.getAllItems();
      for (Map.Entry<String, MeasurementMeta> item : items.entrySet()){
        logWriter.cacheTimeSeries(item.getKey(), item.getValue());
      }
    } catch (Exception e) {
      logger.error("failed to change to new cache file sg {} cache file, msg:{}", sgName, e.getMessage());
    }
  }

  private String newCacheFileName() {
    return MetadataConstant.REMOTE_CACHE_FILE_PREFIX + "-" + sgName + "-" + System.currentTimeMillis() + ".txt";
  }
}
