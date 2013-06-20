package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

class OWoWCache {
  /**
   * Keys is a file id. Values is a sorted set of dirty pages.
   */
  private final Map<Long, SortedMap<Long, OLogSequenceNumber>>  dirtyPages;

  private final boolean                                         syncOnPageFlush;

  private final OWriteAheadLog                                  writeAheadLog;
  /**
   * List of pages which were flushed out of the buffer but were not written to the disk.
   */
  private final Map<FileLockKey, Long>                          evictedPages;

  private final ODirectMemory                                   directMemory;

  private final int                                             pageSize;
  public final int                                              writeQueueLength;

  private final Map<Long, OFileClassic>                         files;

  private static final int                                      DATA_SIZE = 32 * 1024;
  private static final int                                      PRODUCERS = 200;

  private final int                                             maxSize;
  private final ConcurrentSkipListMap<FileLockKey, OCacheEntry> cache;

  OWoWCache(int maxSize, boolean syncOnPageFlush, OWriteAheadLog writeAheadLog, ODirectMemory directMemory, int pageSize,
      int writeQueueLength, Map<Long, OFileClassic> files) {

    this.maxSize = maxSize;
    this.syncOnPageFlush = syncOnPageFlush;
    this.writeAheadLog = writeAheadLog;
    this.evictedPages = new HashMap<FileLockKey, Long>();
    this.directMemory = directMemory;
    this.pageSize = pageSize;
    this.writeQueueLength = writeQueueLength;
    this.files = files;

    this.cache = new ConcurrentSkipListMap<FileLockKey, OCacheEntry>();
    this.dirtyPages = new HashMap<Long, SortedMap<Long, OLogSequenceNumber>>();
  }

  public OCacheEntry load(long fileId, long pageIndex) throws IOException {
    OCacheEntry cacheEntry = updateCache(fileId, pageIndex);
    doMarkDirty(cacheEntry);
    return cacheEntry;
  }

  public void load(OCacheEntry cacheEntry) {
    // todo lock on entry
    if (cacheEntry == null) {
      throw new IllegalStateException("Requested page number " + cacheEntry.pageIndex + " for file "
          + files.get(cacheEntry.fileId).getName() + " is not in cache");
    }
    doMarkDirty(cacheEntry);
  }

  private OCacheEntry updateCache(long fileId, long pageIndex) throws IOException {
    FileLockKey fileLockKey = new FileLockKey(fileId, pageIndex);
    OCacheEntry cacheEntry = cache.get(fileLockKey);
    if (cacheEntry != null) {
      // update cache info
      cacheEntry.recentlyChanged = true;
      cacheEntry.inWriteCache = true;
    } else {
      // create new cache entry
      CacheResult cacheResult = cacheFileContent(fileId, pageIndex);

      final OLogSequenceNumber lsn;
      if (cacheResult.isDirty)
        lsn = dirtyPages.get(fileId).get(pageIndex);
      else
        lsn = OLSNHelper.getLogSequenceNumberFromPage(cacheResult.dataPointer, directMemory);

      cacheEntry = new OCacheEntry(fileId, pageIndex, lsn);
      cacheEntry.inWriteCache = true;
      cache.put(fileLockKey, cacheEntry);
    }
    return cacheEntry;
  }

  private CacheResult cacheFileContent(long fileId, long pageIndex) throws IOException {
    FileLockKey key = new FileLockKey(fileId, pageIndex);
    if (evictedPages.containsKey(key))
      return new CacheResult(true, evictedPages.remove(key));

    final OFileClassic fileClassic = files.get(fileId);
    final long startPosition = pageIndex * pageSize;
    final long endPosition = startPosition + pageSize;

    byte[] content = new byte[pageSize];
    long dataPointer;
    if (fileClassic.getFilledUpTo() >= endPosition) {
      fileClassic.read(startPosition, content, content.length);
      dataPointer = directMemory.allocate(content);
    } else {
      fileClassic.allocateSpace((int) (endPosition - fileClassic.getFilledUpTo()));
      dataPointer = directMemory.allocate(content);
    }

    return new CacheResult(false, dataPointer);
  }

  private void doMarkDirty(OCacheEntry cacheEntry) {
    if (cacheEntry.inWriteCache || cacheEntry.isDirty)
      return;

    dirtyPages.get(cacheEntry.fileId).put(cacheEntry.pageIndex, cacheEntry.loadedLSN);

    cacheEntry.recentlyChanged = true;
    cacheEntry.inWriteCache = true;
    cacheEntry.isDirty = true;
  }

  public void clear() {
    cache.clear();
  }

  public OCacheEntry get(long fileId, long pageIndex) {
    return cache.get(new FileLockKey(fileId, pageIndex));
  }

  // TODO!
  public void remove(long fileId, long pageIndex) {
    OCacheEntry lruEntry = get(fileId, pageIndex);
    if (lruEntry != null) {
      if (lruEntry.usageCounter == 0) {
        // lruEntry = remove(fileId, pageIndex);
        if (lruEntry.dataPointer != ODirectMemory.NULL_POINTER)
          directMemory.free(lruEntry.dataPointer);
      }
    } else {
      Long dataPointer = evictedPages.remove(new FileLockKey(fileId, pageIndex));
      if (dataPointer != null)
        directMemory.free(dataPointer);
    }
  }

  public void deleteFile(long fileId) {
    dirtyPages.remove(fileId);
  }

  public void fillDirtyPages(long fileId) {
    dirtyPages.put(fileId, new TreeMap<Long, OLogSequenceNumber>());
  }

  public void flushFile(long fileId) throws IOException {
    final OFileClassic fileClassic = files.get(fileId);
    if (fileClassic == null || !fileClassic.isOpen())
      return;

    final SortedMap<Long, OLogSequenceNumber> dirtyPages = this.dirtyPages.get(fileId);

    for (Iterator<Long> iterator = dirtyPages.keySet().iterator(); iterator.hasNext();) {
      Long pageIndex = iterator.next();
      OCacheEntry lruEntry = get(fileId, pageIndex);

      if (lruEntry == null) {
        final Long dataPointer = evictedPages.remove(new FileLockKey(fileId, pageIndex));
        if (dataPointer != null) {
          flushData(fileId, pageIndex, dataPointer);
          iterator.remove();
        }
      } else {
        if (lruEntry.usageCounter == 0) {
          flushData(fileId, lruEntry.pageIndex, lruEntry.dataPointer);
          iterator.remove();
          lruEntry.isDirty = false;
        } else {
          throw new OBlockedPageException("Unable to perform flush file because some pages is in use.");
        }
      }
    }

    fileClassic.synch();
  }

  private void flushData(final long fileId, final long pageIndex, final long dataPointer) throws IOException {
    if (writeAheadLog != null) {
      OLogSequenceNumber lsn = OLSNHelper.getLogSequenceNumberFromPage(dataPointer, directMemory);
      OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLSN();
      if (flushedLSN == null || flushedLSN.compareTo(lsn) < 0)
        writeAheadLog.flush();
    }

    final byte[] content = directMemory.get(dataPointer, pageSize);
    OLongSerializer.INSTANCE.serializeNative(OReadWriteCache.MAGIC_NUMBER, content, 0);

    final int crc32 = OCRCCalculator.calculatePageCrc(content);
    OIntegerSerializer.INSTANCE.serializeNative(crc32, content, OLongSerializer.LONG_SIZE);

    final OFileClassic fileClassic = files.get(fileId);

    fileClassic.write(pageIndex * pageSize, content);

    if (syncOnPageFlush)
      fileClassic.synch();
  }

  public Set<ODirtyPage> logDirtyPagesTable() throws IOException {
    if (writeAheadLog == null)
      return Collections.emptySet();

    Set<ODirtyPage> logDirtyPages = new HashSet<ODirtyPage>(dirtyPages.size());
    for (long fileId : dirtyPages.keySet()) {
      SortedMap<Long, OLogSequenceNumber> pages = dirtyPages.get(fileId);
      for (Map.Entry<Long, OLogSequenceNumber> pageEntry : pages.entrySet()) {
        final ODirtyPage logDirtyPage = new ODirtyPage(files.get(fileId).getName(), pageEntry.getKey(), pageEntry.getValue());
        logDirtyPages.add(logDirtyPage);
      }
    }

    writeAheadLog.logDirtyPages(logDirtyPages);
    return logDirtyPages;
  }

  public void closeFile(long fileId, Map<Long, Set<Long>> filePages, boolean flush) throws IOException {

    final Set<Long> pageIndexes = filePages.get(fileId);
    Long[] sortedPageIndexes = new Long[pageIndexes.size()];
    sortedPageIndexes = pageIndexes.toArray(sortedPageIndexes);
    Arrays.sort(sortedPageIndexes);

    final SortedMap<Long, OLogSequenceNumber> fileDirtyPages = dirtyPages.get(fileId);

    for (Long pageIndex : sortedPageIndexes) {
      OCacheEntry lruEntry = get(fileId, pageIndex);
      if (lruEntry != null) {
        if (lruEntry.usageCounter == 0) {
          // lruEntry = remove(fileId, pageIndex);

          flushData(fileId, pageIndex, lruEntry.dataPointer);
          fileDirtyPages.remove(pageIndex);

          directMemory.free(lruEntry.dataPointer);
        }
      } else {
        Long dataPointer = evictedPages.remove(new FileLockKey(fileId, pageIndex));
        if (dataPointer != null) {
          flushData(fileId, pageIndex, dataPointer);
          fileDirtyPages.remove(pageIndex);
        }
      }
    }
    // TODO why?!
    pageIndexes.clear();
  }

  public void clearDirtyPages(long fileId) {
    dirtyPages.get(fileId).clear();
  }

  private void flushEvictedPages() throws IOException {
    @SuppressWarnings("unchecked")
    Map.Entry<FileLockKey, Long>[] sortedPages = evictedPages.entrySet().toArray(new Map.Entry[evictedPages.size()]);
    Arrays.sort(sortedPages, new Comparator<Map.Entry>() {
      @Override
      public int compare(Map.Entry entryOne, Map.Entry entryTwo) {
        FileLockKey fileLockKeyOne = (FileLockKey) entryOne.getKey();
        FileLockKey fileLockKeyTwo = (FileLockKey) entryTwo.getKey();
        return fileLockKeyOne.compareTo(fileLockKeyTwo);
      }
    });

    for (Map.Entry<FileLockKey, Long> entry : sortedPages) {
      long evictedDataPointer = entry.getValue();
      FileLockKey fileLockKey = entry.getKey();

      flushData(fileLockKey.fileId, fileLockKey.pageIndex, evictedDataPointer);
      dirtyPages.get(fileLockKey.fileId).remove(fileLockKey.pageIndex);

      directMemory.free(evictedDataPointer);
    }

    evictedPages.clear();
  }

  private void evictFileContent(long fileId, long pageIndex, long dataPointer, boolean isDirty) throws IOException {
    if (isDirty) {
      if (evictedPages.size() >= writeQueueLength)
        flushEvictedPages();

      evictedPages.put(new FileLockKey(fileId, pageIndex), dataPointer);
    } else {
      directMemory.free(dataPointer);
    }
  }

  private static final class FileLockKey implements Comparable<FileLockKey> {
    private final long fileId;
    private final long pageIndex;

    private FileLockKey(long fileId, long pageIndex) {
      this.fileId = fileId;
      this.pageIndex = pageIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      FileLockKey that = (FileLockKey) o;

      if (fileId != that.fileId)
        return false;
      if (pageIndex != that.pageIndex)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (fileId ^ (fileId >>> 32));
      result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
      return result;
    }

    @Override
    public int compareTo(FileLockKey otherKey) {
      if (fileId > otherKey.fileId)
        return 1;
      if (fileId < otherKey.fileId)
        return -1;

      if (pageIndex > otherKey.pageIndex)
        return 1;
      if (pageIndex < otherKey.pageIndex)
        return -1;

      return 0;
    }
  }

  private static class CacheResult {
    private final boolean isDirty;
    private final long    dataPointer;

    private CacheResult(boolean dirty, long dataPointer) {
      isDirty = dirty;
      this.dataPointer = dataPointer;
    }
  }

  // public static void main(String[] args) throws InterruptedException {
  //
  // for (int t = 0; t < 5; t++) {
  // // locks
  //
  // final ConcurrentHashMap<Integer, Lock> locks = new ConcurrentHashMap<Integer, Lock>();
  //
  // // cache
  //
  // final AtomicInteger dataGenerator = new AtomicInteger();
  // final CountDownLatch latch = new CountDownLatch(1);
  //
  // final AtomicInteger producers = new AtomicInteger(PRODUCERS);
  // final List<Thread> threads = new ArrayList<Thread>();
  //
  // for (int i = 0; i < PRODUCERS; i++) {
  // threads.add(new Thread(new Runnable() {
  // @Override
  // public void run() {
  // try {
  // final Random r = new Random();
  // latch.await();
  // for (int i = 0; i < 1000; i++) {
  // for (int j = 0; j < 100; j++) {
  // final int pointer = r.nextInt(DATA_SIZE);
  // final int data = dataGenerator.incrementAndGet();
  // final long time = System.nanoTime();
  // final OCacheEntry entry = new OCacheEntry(-1, -1, null);// new OCacheEntry(pointer, time);
  //
  // final Lock newLock = new ReentrantLock();
  // final Lock putLock = locks.putIfAbsent(pointer, newLock);
  // final Lock theLock = putLock != null ? putLock : newLock;
  // theLock.lock();
  // cache.put(pointer, entry);
  // theLock.unlock();
  // }
  // TimeUnit.MILLISECONDS.sleep(1);
  // }
  // } catch (InterruptedException e) {
  // throw new RuntimeException(e);
  // } finally {
  // producers.decrementAndGet();
  // }
  // }
  // }));
  // }
  //
  // final AtomicInteger putOperations = new AtomicInteger();
  //
  // threads.add(new Thread(new Runnable() {
  // @Override
  // public void run() {
  // try {
  // latch.await();
  // while (producers.get() > 0 || !cache.isEmpty()) {
  // System.out.println("Flushing started, cache size is " + cache.size());
  // long time1 = System.currentTimeMillis();
  // for (Map.Entry<Integer, OCacheEntry> entry : cache.entrySet()) {
  // final int pointer = entry.getKey();
  // final OCacheEntry cacheEntry = entry.getValue();
  // if (!cacheEntry.recentlyChanged/* System.nanoTime() - 100000000L > cacheEntry.time */) {
  // cache.remove(pointer, cacheEntry);
  //
  // // do flush
  // putOperations.incrementAndGet();
  // } else {
  // cacheEntry.recentlyChanged = false;
  // }
  // }
  // long time2 = System.currentTimeMillis();
  // System.out.println("Flushing finished, cache size is " + cache.size());
  // System.out.println("Time: " + (time2 - time1));
  // TimeUnit.MILLISECONDS.sleep(1);
  // }
  // } catch (InterruptedException e) {
  // throw new RuntimeException(e);
  // }
  // }
  // }));
  //
  // TimeUnit.SECONDS.sleep(1);
  //
  // for (Thread thread : threads) {
  // thread.start();
  // }
  //
  // latch.countDown();
  //
  // for (Thread thread : threads) {
  // thread.join();
  // }
  //
  // System.out.println();
  // System.out.println("Put from cache operations: " + putOperations.get());
  // System.out.println();
  //
  // int errors = 0;
  //
  // System.out.println("Errors: " + errors);
  //
  // }
  // }
}
