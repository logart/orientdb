package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WriteAheadLogTest;

public class WoWCacheTest {
  private int                    systemOffset = 2 * (OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);

  private OReadWriteCache        cache;
  private OLocalPaginatedStorage storageLocal;
  private ODirectMemory          directMemory;
  private String                 fileName;
  private byte                   seed;
  private OWriteAheadLog         writeAheadLog;
  private OWoWCache              writeCache;

  @BeforeClass
  public void beforeClass() throws IOException {

    OGlobalConfiguration.FILE_LOCK.setValue(Boolean.FALSE);
    directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    storageLocal = (OLocalPaginatedStorage) Orient.instance().loadStorage("plocal:" + buildDirectory + "/ReadWriteCacheTest");

    fileName = "o2QCacheTest.tst";

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, WriteAheadLogTest.TestRecord.class);
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    closeBufferAndDeleteFile();

    initBuffer();

    Random random = new Random();
    seed = (byte) (random.nextInt() & 0xFF);
  }

  private void closeBufferAndDeleteFile() throws IOException {
    if (cache != null) {
      cache.close();
      cache = null;
    }

    if (writeAheadLog != null) {
      writeAheadLog.delete();
      writeAheadLog = null;
    }

    File file = new File(storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.tst");
    if (file.exists()) {
      boolean delete = file.delete();
      Assert.assertTrue(delete);
    }
  }

  @AfterClass
  public void afterClass() throws IOException {
    if (cache != null) {
      cache.close();
      cache = null;
    }

    if (writeAheadLog != null) {
      writeAheadLog.delete();
      writeAheadLog = null;
    }

    storageLocal.delete();

    File file = new File(storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.tst");
    if (file.exists()) {
      Assert.assertTrue(file.delete());
      file.getParentFile().delete();
    }

  }

  private void initBuffer() throws IOException {
    cache = new OReadWriteCache(4 * (8 + systemOffset), 15000, directMemory, null, 8 + systemOffset, storageLocal, true);
    writeCache = cache.getWriteCache();

    final OStorageSegmentConfiguration segmentConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(),
        "oRWCacheTest", 0);
    segmentConfiguration.fileType = OFileFactory.CLASSIC;
  }

  @Test
  public void testCacheShouldContainsRecordsAfterLoadMethod() throws Exception {
    long fileId = cache.openFile(fileName);

    OCacheEntry cacheEntry = writeCache.load(fileId, 0);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry.fileId, fileId);
    Assert.assertEquals(cacheEntry.pageIndex, 0);
    Assert.assertFalse(ODirectMemory.NULL_POINTER == cacheEntry.dataPointer);
    Assert.assertTrue(cacheEntry.inWriteCache);
    Assert.assertTrue(cacheEntry.recentlyChanged);
    Assert.assertEquals(cacheEntry.usageCounter, 1);

  }
}
