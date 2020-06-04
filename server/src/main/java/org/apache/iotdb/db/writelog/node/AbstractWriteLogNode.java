package org.apache.iotdb.db.writelog.node;

import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.io.ILogWriter;
import org.apache.iotdb.db.writelog.io.LogWriter;
import org.apache.iotdb.db.writelog.io.MultiFileLogReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractWriteLogNode<T> implements WriteLogNode<T>, Comparable<AbstractWriteLogNode>  {

  protected Logger logger = LoggerFactory.getLogger(AbstractWriteLogNode.class);

  private String identifier;

  private String logDirectory;

  private ILogWriter currentFileWriter;

  protected IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private ByteBuffer logBuffer = ByteBuffer
    .allocate(IoTDBDescriptor.getInstance().getConfig().getWalBufferSize());

  private ReadWriteLock lock = new ReentrantReadWriteLock();

  private long fileId = 0;
  private long lastFlushedId = 0;

  private int bufferedLogNum = 0;

  /**
   * constructor of AbstractWriteLogNode.
   *
   * @param identifier AbstractWriteLogNode identifier
   */
  public AbstractWriteLogNode(String identifier) {
    this.identifier = identifier;
    this.logDirectory =
      getFolder() + File.separator + this.identifier;
    if (SystemFileFactory.INSTANCE.getFile(logDirectory).mkdirs()) {
      logger.info("create the log folder {}." + logDirectory);
    }
  }

  @Override
  public void write(T log) throws IOException {
    lock.writeLock().lock();
    try {
      putLog(log);
      if (bufferedLogNum >= config.getFlushWalThreshold()) {
        sync();
      }
    } catch (BufferOverflowException e) {
      throw new IOException(
        "Log cannot fit into buffer, if you don't enable Dynamic Parameter Adapter, please increase wal_buffer_size;"
          + "otherwise, please increase the JVM memory", e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  protected void putLog(T log) {
    logBuffer.mark();
    try {
      ((org.apache.iotdb.db.writelog.node.ISerialize)log).serialize(logBuffer);
    } catch (BufferOverflowException e) {
      logger.info("WAL BufferOverflow !");
      logBuffer.reset();
      sync();
      ((org.apache.iotdb.db.writelog.node.ISerialize)log).serialize(logBuffer);
    }
    bufferedLogNum ++;
  }

  @Override
  public void close() {
    sync();
    forceWal();
    lock.writeLock().lock();
    try {
      if (this.currentFileWriter != null) {
        this.currentFileWriter.close();
        this.currentFileWriter = null;
      }
      logger.debug("Log node {} closed successfully", identifier);
    } catch (IOException e) {
      logger.error("Cannot close log node {} because:", identifier, e);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void forceSync() {
    sync();
    forceWal();
  }


  @Override
  public void notifyStartFlush() {
    lock.writeLock().lock();
    try {
      close();
      nextFileWriter();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void notifyEndFlush() {
    lock.writeLock().lock();
    try {
      File logFile = SystemFileFactory.INSTANCE.getFile(logDirectory, getPrefix() + ++lastFlushedId);
      discard(logFile);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public String getLogDirectory() {
    return logDirectory;
  }

  @Override
  public void delete() throws IOException {
    lock.writeLock().lock();
    try {
      logBuffer.clear();
      close();
      FileUtils.deleteDirectory(SystemFileFactory.INSTANCE.getFile(logDirectory));
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public ILogReader getLogReader() {
    File[] logFiles = SystemFileFactory.INSTANCE.getFile(logDirectory).listFiles();
    Arrays.sort(logFiles,
      Comparator.comparingInt(f -> Integer.parseInt(f.getName().replace(getPrefix(), ""))));
    return new MultiFileLogReader(logFiles);
  }

  private void discard(File logFile) {
    if (!logFile.exists()) {
      logger.info("Log file does not exist");
    } else {
      try {
        FileUtils.forceDelete(logFile);
        logger.info("Log node {} cleaned old file", identifier);
      } catch (IOException e) {
        logger.error("Old log file {} of {} cannot be deleted", logFile.getName(), identifier, e);
      }
    }
  }

  private void forceWal() {
    lock.writeLock().lock();
    try {
      try {
        if (currentFileWriter != null) {
          currentFileWriter.force();
        }
      } catch (IOException e) {
        logger.error("Log node {} force failed.", identifier, e);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void sync() {
    lock.writeLock().lock();
    try {
      if (bufferedLogNum == 0) {
        return;
      }
      try {
        getCurrentFileWriter().write(logBuffer);
      } catch (IOException e) {
        logger.error("Log node {} sync failed, change system mode to read-only", identifier, e);
        IoTDBDescriptor.getInstance().getConfig().setReadOnly(true);
        return;
      }
      logBuffer.clear();
      bufferedLogNum = 0;
      logger.debug("Log node {} ends sync.", identifier);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private ILogWriter getCurrentFileWriter() {
    if (currentFileWriter == null) {
      nextFileWriter();
    }
    return currentFileWriter;
  }

  private void nextFileWriter() {
    fileId++;
    File newFile = SystemFileFactory.INSTANCE.getFile(logDirectory, getPrefix() + fileId);
    if (newFile.getParentFile().mkdirs()) {
      logger.info("create log parent folder {}.", newFile.getParent());
    }
    currentFileWriter = new LogWriter(newFile);
  }

  protected abstract String getPrefix();

  @Override
  public int hashCode() {
    return identifier.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    return compareTo((AbstractWriteLogNode) obj) == 0;
  }

  @Override
  public String toString() {
    return "Log node " + identifier;
  }

  @Override
  public int compareTo(AbstractWriteLogNode o) {
    return this.identifier.compareTo(o.identifier);
  }

  protected abstract String getFolder();
}
