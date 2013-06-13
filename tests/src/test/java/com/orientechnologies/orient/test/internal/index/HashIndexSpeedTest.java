package com.orientechnologies.orient.test.internal.index;

import java.lang.reflect.Field;

import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionLong;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.index.hashindex.local.OHashIndexBucket;
import com.orientechnologies.orient.core.index.hashindex.local.OUniqueHashIndex;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OReadWriteCache;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * @author Andrey Lomakin
 * @since 30.01.13
 */
public class HashIndexSpeedTest extends SpeedTestMonoThread {
  private ODatabaseDocumentTx databaseDocumentTx;
  private OUniqueHashIndex    hashIndex;
  private MersenneTwisterFast random = new MersenneTwisterFast();
  private OReadWriteCache     buffer;

  public HashIndexSpeedTest() {
    super(5000000);
  }

  @Override
  @Test(enabled = true)
  public void init() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    databaseDocumentTx = new ODatabaseDocumentTx("local:" + buildDirectory + "/uniqueHashIndexTest");
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    long maxMemory = 2L * 1024 * 1024 * 1024;
    System.out.println("Max memory :" + maxMemory);
    buffer = new OReadWriteCache(maxMemory, 15000, ODirectMemoryFactory.INSTANCE.directMemory(), null,
        OHashIndexBucket.MAX_BUCKET_SIZE_BYTES, (OStorageLocal) databaseDocumentTx.getStorage(), false);

    hashIndex = new OUniqueHashIndex();

    OStorageLocal storage = (OStorageLocal) databaseDocumentTx.getStorage();
    Field diskCache = storage.getClass().getDeclaredField("diskCache");
    diskCache.setAccessible(true);
    try {
      diskCache.set(storage, buffer);
    } finally {
      diskCache.setAccessible(false);
    }
    hashIndex.create("uhashIndexTest", new OSimpleKeyIndexDefinition(OType.STRING), databaseDocumentTx,
        OMetadata.CLUSTER_INDEX_NAME, new int[0], null);
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    String key = "bsadfasfas" + random.nextInt();
    hashIndex.put(key, new ORecordId(0, new OClusterPositionLong(0)));
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {
    hashIndex.delete();
    databaseDocumentTx.drop();
  }
}
