package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Andrey Lomakin
 * @since 26.02.13
 */
@Test
public class LRUListTest {
  public void testSingleAdd() {
    LRUList lruList = new LRUList();

    lruList.putToMRU(1, 10, 100, false, new OLogSequenceNumber(0, 0));

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertCacheEntry(entryIterator.next(), 1, 10, 100);
  }

  public void testAddTwo() {
    LRUList lruList = new LRUList();

    lruList.putToMRU(1, 10, 100, false, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(1, 20, 200, false, new OLogSequenceNumber(0, 0));

    Assert.assertEquals(lruList.size(), 2);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertCacheEntry(entryIterator.next(), 1, 20, 200);
    assertCacheEntry(entryIterator.next(), 1, 10, 100);

  }

  public void testAddThree() {
    LRUList lruList = new LRUList();

    lruList.putToMRU(1, 10, 100, false, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(1, 20, 200, false, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(3, 30, 300, false, new OLogSequenceNumber(0, 0));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertCacheEntry(entryIterator.next(), 3, 30, 300);
    assertCacheEntry(entryIterator.next(), 1, 20, 200);
    assertCacheEntry(entryIterator.next(), 1, 10, 100);
  }

  public void testAddThreePutMiddleToTop() {
    LRUList lruList = new LRUList();

    lruList.putToMRU(1, 10, 100, false, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(1, 20, 200, false, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(3, 30, 300, false, new OLogSequenceNumber(0, 0));

    lruList.putToMRU(1, 20, 200, false, new OLogSequenceNumber(0, 0));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertCacheEntry(entryIterator.next(), 1, 20, 200);
    assertCacheEntry(entryIterator.next(), 3, 30, 300);
    assertCacheEntry(entryIterator.next(), 1, 10, 100);
  }

  public void testAddThreePutMiddleToTopChangePointer() {
    LRUList lruList = new LRUList();

    lruList.putToMRU(1, 10, 100, false, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(1, 20, 200, false, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(3, 30, 300, false, new OLogSequenceNumber(0, 0));

    lruList.putToMRU(1, 20, 400, false, new OLogSequenceNumber(0, 0));

    Assert.assertEquals(lruList.size(), 3);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertTrue(entryIterator.hasNext());

    assertCacheEntry(entryIterator.next(), 1, 20, 400);
    assertCacheEntry(entryIterator.next(), 3, 30, 300);
    assertCacheEntry(entryIterator.next(), 1, 10, 100);
  }

  public void testAddElevenPutMiddleToTopChangePointer() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 11; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false, new OLogSequenceNumber(0, 0));
    }

    lruList.putToMRU(1, 50, 500, false, new OLogSequenceNumber(0, 0));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    assertCacheEntry(entryIterator.next(), 1, 50, 500);

    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      assertCacheEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAddOneRemoveLRU() {
    LRUList lruList = new LRUList();

    lruList.putToMRU(1, 10, 100, false, new OLogSequenceNumber(0, 0));
    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 0);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    Assert.assertFalse(entryIterator.hasNext());
  }

  public void testRemoveLRUShouldReturnNullIfAllRecordsAreUsed() {
    LRUList lruList = new LRUList();

    OCacheEntry lruEntry = lruList.putToMRU(1, 10, 100, false, new OLogSequenceNumber(0, 0));
    lruEntry.usageCounter++;
    OCacheEntry removedLRU = lruList.removeLRU();

    Assert.assertNull(removedLRU);
  }

  public void testAddElevenRemoveLRU() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 11; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false, new OLogSequenceNumber(0, 0));
    }

    lruList.removeLRU();

    Assert.assertEquals(lruList.size(), 10);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    for (int i = 10; i > 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertCacheEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAddElevenRemoveMiddle() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 11; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false, new OLogSequenceNumber(0, 0));
    }

    assertCacheEntry(lruList.remove(1, 50), 1, 50, 500);
    Assert.assertNull(lruList.remove(1, 500));

    Assert.assertEquals(lruList.size(), 10);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      if (i == 5)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      assertCacheEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAddElevenGetMiddle() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 11; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false, new OLogSequenceNumber(0, 0));
    }

    Assert.assertTrue(lruList.contains(1, 50));
    assertCacheEntry(lruList.get(1, 50), 1, 50, 500);

    Assert.assertFalse(lruList.contains(2, 50));

    Assert.assertEquals(lruList.size(), 11);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 10; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertCacheEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAdd9128() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 9128; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false, new OLogSequenceNumber(0, 0));
    }

    Assert.assertEquals(lruList.size(), 9128);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertCacheEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAdd9128Get() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 9128; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false, new OLogSequenceNumber(0, 0));
    }

    Assert.assertEquals(lruList.size(), 9128);

    for (int i = 0; i < 9128; i++)
      assertCacheEntry(lruList.get(1, i * 10), 1, i * 10, i * 100);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 9127; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertCacheEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAdd9128Remove4564() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 9128; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false, new OLogSequenceNumber(0, 0));
    }

    for (int i = 4564; i < 9128; i++)
      assertCacheEntry(lruList.remove(1, i * 10), 1, i * 10, i * 100);

    Assert.assertEquals(lruList.size(), 4564);

    Iterator<OCacheEntry> entryIterator = lruList.iterator();
    for (int i = 4563; i >= 0; i--) {
      Assert.assertTrue(entryIterator.hasNext());
      assertCacheEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  public void testAdd9128PutLastAndMiddleToTop() {
    LRUList lruList = new LRUList();

    for (int i = 0; i < 9128; i++) {
      lruList.putToMRU(1, i * 10, i * 100, false, new OLogSequenceNumber(0, 0));
    }

    lruList.putToMRU(1, 0, 0, false, new OLogSequenceNumber(0, 0));
    lruList.putToMRU(1, 4500 * 10, 4500 * 100, false, new OLogSequenceNumber(0, 0));

    Assert.assertEquals(lruList.size(), 9128);
    Iterator<OCacheEntry> entryIterator = lruList.iterator();

    Assert.assertTrue(entryIterator.hasNext());
    assertCacheEntry(entryIterator.next(), 1, 4500 * 10, 4500 * 100);
    assertCacheEntry(entryIterator.next(), 1, 0, 0);

    for (int i = 9127; i >= 1; i--) {
      if (i == 4500)
        continue;

      Assert.assertTrue(entryIterator.hasNext());
      assertCacheEntry(entryIterator.next(), 1, i * 10, i * 100);
    }
  }

  private void assertCacheEntry(OCacheEntry lruEntry, long fileId, long filePosition, long dataPointer) {
    Assert.assertEquals(lruEntry.fileId, fileId);
    Assert.assertEquals(lruEntry.pageIndex, filePosition);
    Assert.assertEquals(lruEntry.dataPointer, dataPointer);
  }

}
