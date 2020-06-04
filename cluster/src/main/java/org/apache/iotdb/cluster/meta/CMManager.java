package org.apache.iotdb.cluster.meta;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MeasurementMeta;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CMManager extends MManager {
  private static final Logger logger = LoggerFactory.getLogger(CMManager.class);

  // the lock for read/insert
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private MeasurementCache mCache;

  CMManager() {
    super();
    mCache = MeasurementCache.getInstance();
  }

  @Override
  public Pair<Set<String>, String> deleteTimeseries(String prefixPath) throws MetadataException {
    lock.writeLock().lock();
    try {
      // clear cached schema
      mCache.removeSeries(prefixPath);
      return super.deleteTimeseries(prefixPath);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void deleteStorageGroups(List<String> storageGroups) throws MetadataException {
    lock.writeLock().lock();
    try {
      // clear cached schema
      for (String sg : storageGroups) {
        mCache.removeSg(sg);
      }
      super.deleteStorageGroups(storageGroups);
    } finally {
      lock.writeLock().unlock();
    }

  }

  @Override
  public TSDataType getSeriesType(String path) throws MetadataException {
    lock.readLock().lock();
    try {
      if (path.equals(SQLConstant.RESERVED_TIME)) {
        return TSDataType.INT64;
      }

      try {
        MeasurementSchema schema = mCache.getMeta(path).getSchema();
        return schema.getType();
      } catch (IOException e) {
        // ignore
      }

      return super.getSeriesType(path);
    } finally {
      lock.readLock().unlock();
    }

  }

  @Override
  public MeasurementSchema getSeriesSchema(String device, String measurement) throws MetadataException {
    lock.readLock().lock();
    try {
      try {
        return mCache.getMeta(device+measurement).getSchema();
      } catch (IOException e) {
        // ignore
      }
      return super.getSeriesSchema(device, measurement);
    } finally {
      lock.readLock().unlock();
    }

  }

  private static class CMManagerHolder {

    private CMManagerHolder() {
      // allowed to do nothing
    }

    private static final CMManager INSTANCE = new CMManager();
  }

  public static CMManager getInstance() {
    return CMManagerHolder.INSTANCE;
  }

  public void cacheMeta(String path, MeasurementMeta meta) {
    try {
      mCache.cacheMeta(path, meta);
    } catch (StorageGroupNotSetException e) {
      logger.error("failed to cache TimeSeries {} for {}", path, e.getMessage());
    }
  }
}
