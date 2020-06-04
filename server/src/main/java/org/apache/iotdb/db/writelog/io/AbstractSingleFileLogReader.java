package org.apache.iotdb.db.writelog.io;

import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;

public abstract class AbstractSingleFileLogReader<T> implements ILogReader<T>  {
  protected Logger logger = LoggerFactory.getLogger(AbstractSingleFileLogReader.class);
  public static final int LEAST_LOG_SIZE = 12; // size + checksum

  private DataInputStream logStream;
  private String filepath;

  private byte[] buffer;
  private CRC32 checkSummer = new CRC32();

  // used to indicate the position of the broken log
  private int idx;

  private ILogReader<T> batchLogReader;

  private boolean fileCorrupted = false;

  public AbstractSingleFileLogReader(File logFile) throws FileNotFoundException {
    open(logFile);
  }

  @Override
  public boolean hasNext() {
    try {
      if (batchLogReader != null && batchLogReader.hasNext()) {
        return true;
      }

      if (logStream.available() < LEAST_LOG_SIZE) {
        return false;
      }

      int logSize = logStream.readInt();
      if (logSize <= 0) {
        return false;
      }
      buffer = new byte[logSize];

      int readLen = logStream.read(buffer, 0, logSize);
      if (readLen < logSize) {
        throw new IOException("Reach eof");
      }

      final long checkSum = logStream.readLong();
      checkSummer.reset();
      checkSummer.update(buffer, 0, logSize);
      if (checkSummer.getValue() != checkSum) {
        throw new IOException(String.format("The check sum of the No.%d log batch is incorrect! In "
          + "file: "
          + "%d Calculated: %d.", idx, checkSum, checkSummer.getValue()));
      }

      batchLogReader = newLogReader(ByteBuffer.wrap(buffer));
      fileCorrupted = fileCorrupted || batchLogReader.isFileCorrupted();
    } catch (Exception e) {
      logger.error("Cannot read more PhysicalPlans from {} because", filepath, e);
      fileCorrupted = true;
      return false;
    }
    return true;
  }

  @Override
  public T next() throws FileNotFoundException {
    if (!hasNext()){
      throw new NoSuchElementException();
    }

    idx ++;
    return batchLogReader.next();
  }

  @Override
  public void close() {
    if (logStream != null) {
      try {
        logStream.close();
      } catch (IOException e) {
        logger.error("Cannot close log file {}", filepath, e);
      }
    }
  }

  public void open(File logFile) throws FileNotFoundException {
    close();
    logStream = new DataInputStream(new BufferedInputStream(new FileInputStream(logFile)));
    this.filepath = logFile.getPath();
    idx = 0;
  }

  public boolean isFileCorrupted() {
    return fileCorrupted;
  }

  protected ILogReader<T> newLogReader(ByteBuffer buf) {
    return new IBatchReader<T>(buf);
  };
}
