package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
  private final Map<Long, SortedMap<Long, OLogSequenceNumber>>                  dirtyPages;

  private final boolean                                                         syncOnPageFlush;

  private final OWriteAheadLog                                                  writeAheadLog;
  /**
   * List of pages which were flushed out of the buffer but were not written to the disk.
   */

  private final ODirectMemory                                                   directMemory;

  private final int                                                             pageSize;
  public final int                                                              writeQueueLength;

  private final Map<Long, OFileClassic>                                         files;

  private final int                                                             maxSize;
  private final ConcurrentSkipListMap<OReadWriteCache.FileLockKey, OCacheEntry> cache;
  private static final double                                                   THRESHOLD1       = 0.5;
  private static final double                                                   THRESHOLD2       = 0.9;
  private Map<Long, Long>                                                       currentFlushPointer;

  private static final long                                                     WRITE_GROUP_SIZE = 4;
  private final ConcurrentMap<OReadWriteCache.FileLockKey, ReadWriteLock>       entriesLocks;
  private int                                                                   commitDelay      = 1;
  private ScheduledExecutorService                                              commitExecutor   = Executors
                                                                                                     .newSingleThreadScheduledExecutor(new ThreadFactory() {
                                                                                                       @Override
                                                                                                       public Thread newThread(
                                                                                                           Runnable r) {
                                                                                                         Thread thread = new Thread(
                                                                                                             r);
                                                                                                         thread.setDaemon(true);
                                                                                                         thread
                                                                                                             .setName("Disk-cache Flush Task");
                                                                                                         return thread;
                                                                                                       }
                                                                                                     });

  OWoWCache(int maxSize, boolean syncOnPageFlush, OWriteAheadLog writeAheadLog, ODirectMemory directMemory, int pageSize,
      int writeQueueLength, Map<Long, OFileClassic> files, ConcurrentMap<OReadWriteCache.FileLockKey, ReadWriteLock> entriesLocks) {

    this.maxSize = maxSize;
    this.syncOnPageFlush = syncOnPageFlush;
    this.writeAheadLog = writeAheadLog;
    this.directMemory = directMemory;
    this.pageSize = pageSize;
    this.writeQueueLength = writeQueueLength;
    this.files = files;
    this.entriesLocks = entriesLocks;

    this.cache = new ConcurrentSkipListMap<OReadWriteCache.FileLockKey, OCacheEntry>();
    this.dirtyPages = new HashMap<Long, SortedMap<Long, OLogSequenceNumber>>();
    this.currentFlushPointer = new HashMap<Long, Long>();

  }

  public OCacheEntry markDirty(long fileId, long pageIndex) throws IOException {
    OCacheEntry cacheEntry = putToCache(fileId, pageIndex);
    doMarkDirty(cacheEntry);
    return cacheEntry;
  }

  public void markDirty(OCacheEntry cacheEntry) throws IOException {
    if (cacheEntry == null) {
      throw new IllegalStateException("Requested page is not in cache");
    }

    OReadWriteCache.FileLockKey fileLock = new OReadWriteCache.FileLockKey(cacheEntry.fileId, cacheEntry.pageIndex);
    ReadWriteLock lock = entriesLocks.get(fileLock);
    if (lock == null) {
      lock = new ReentrantReadWriteLock();
      entriesLocks.putIfAbsent(fileLock, lock);
    }

    lock.writeLock().lock();
    try {
      cacheEntry = putToCache(cacheEntry);
      doMarkDirty(cacheEntry);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private OCacheEntry putToCache(long fileId, long pageIndex) throws IOException {
    // absent in 2Q
    OReadWriteCache.FileLockKey fileLockKey = new OReadWriteCache.FileLockKey(fileId, pageIndex);
    OCacheEntry cacheEntry = cache.get(fileLockKey);
    if (cacheEntry == null) {
      long dataPointer = cacheFileContent(fileId, pageIndex);

      final OLogSequenceNumber lsn = OLSNHelper.getLogSequenceNumberFromPage(dataPointer, directMemory);

      OCacheEntry newCacheEntry = new OCacheEntry(fileId, pageIndex, lsn);
      newCacheEntry.dataPointer = dataPointer;
      cache.put(fileLockKey, newCacheEntry);
      cacheEntry = newCacheEntry;
    }
    return cacheEntry;
  }

  private OCacheEntry putToCache(OCacheEntry cachedEntry) throws IOException {
    OReadWriteCache.FileLockKey fileLockKey = new OReadWriteCache.FileLockKey(cachedEntry.fileId, cachedEntry.pageIndex);
    OCacheEntry result = cache.get(fileLockKey);
    if (result == null) {
      cache.put(fileLockKey, cachedEntry);
      result = cachedEntry;
    }
    return result;
  }

  private long cacheFileContent(long fileId, long pageIndex) throws IOException {
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

    return dataPointer;
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
    return cache.get(new OReadWriteCache.FileLockKey(fileId, pageIndex));
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
    }
  }

  public void deleteFile(long fileId) {
    dirtyPages.remove(fileId);
  }

  public void fillDirtyPages(long fileId) {
    dirtyPages.put(fileId, new TreeMap<Long, OLogSequenceNumber>());
  }

  public void flushFile(long fileId, boolean forceFlush) throws IOException {
    final OFileClassic fileClassic = files.get(fileId);
    if (fileClassic == null || !fileClassic.isOpen())
      return;

    final long initialFlushPointer;
    Long flushPointerCandidate = currentFlushPointer.get(fileId);
    if (flushPointerCandidate == null) {
      initialFlushPointer = (long) 0;
    } else {
      initialFlushPointer = ((flushPointerCandidate >>> WRITE_GROUP_SIZE) << WRITE_GROUP_SIZE);
    }

    long flushPointer = initialFlushPointer;

    List<OReadWriteCache.FileLockKey> keysToFlush;
    final long writeGroupsCount;
    if (cache.isEmpty()) {
      writeGroupsCount = 0;
    } else {
      writeGroupsCount = cache.lastKey().pageIndex >>> WRITE_GROUP_SIZE;
    }
    Map<OReadWriteCache.FileLockKey, OCacheEntry> writeGroupEntries = Collections.emptyMap();
    while (writeGroupEntries.isEmpty() && (flushPointer - initialFlushPointer) <= writeGroupsCount) {
      writeGroupEntries = getWriteGroup(fileId, initialFlushPointer);
      flushPointer += 16;
    }

    if (!writeGroupEntries.isEmpty()) {
      boolean recordsIsFresh = false;
      for (OCacheEntry cacheEntry : writeGroupEntries.values()) {
        if (cacheEntry.recentlyChanged) {
          recordsIsFresh = true;
        }
        cacheEntry.recentlyChanged = false;
      }
      if (forceFlush || !recordsIsFresh) {
        keysToFlush = lockWriteGroup(new ArrayList<OReadWriteCache.FileLockKey>(writeGroupEntries.keySet()));
        // flush
        try {
          for (OReadWriteCache.FileLockKey fileLockKey : keysToFlush) {
            OCacheEntry cacheEntry = cache.get(fileLockKey);
            flushData(cacheEntry);
            cacheEntry.isDirty = false;
            cacheEntry.inWriteCache = false;
            cache.remove(fileLockKey);
          }
        } finally {
          unlockWriteGroup(keysToFlush);
        }
      }
    }

    fileClassic.synch();
  }

  private List<OReadWriteCache.FileLockKey> lockWriteGroup(List<OReadWriteCache.FileLockKey> writeGroupEntries) {
    for (int i = 0; i < writeGroupEntries.size(); i++) {
      OReadWriteCache.FileLockKey fileLockKey = writeGroupEntries.get(i);
      ReadWriteLock newLock = new ReentrantReadWriteLock();
      assert null != fileLockKey;
      entriesLocks.putIfAbsent(fileLockKey, newLock);
      ReadWriteLock lock = entriesLocks.get(fileLockKey);
      if (!lock.writeLock().tryLock()) {
        rollbackRecordsLock(writeGroupEntries, i);
        return Collections.emptyList();
      }
    }
    return writeGroupEntries;
  }

  private Map<OReadWriteCache.FileLockKey, OCacheEntry> getWriteGroup(long fileId, long writeGroupFirstPage) {
    Map<OReadWriteCache.FileLockKey, OCacheEntry> entriesToFlush = new HashMap<OReadWriteCache.FileLockKey, OCacheEntry>(16);
    Map.Entry<OReadWriteCache.FileLockKey, OCacheEntry> entryToFlush = cache.ceilingEntry(new OReadWriteCache.FileLockKey(fileId,
        writeGroupFirstPage));
    while (entryToFlush != null && (entryToFlush.getKey().pageIndex <= (writeGroupFirstPage + 16 - 1))) {
      entriesToFlush.put(entryToFlush.getKey(), entryToFlush.getValue());
      entryToFlush = cache.higherEntry(entryToFlush.getKey());
    }
    return entriesToFlush;
  }

  private void flushData(OCacheEntry cacheEntry) throws IOException {
    if (writeAheadLog != null) {
      OLogSequenceNumber lsn = OLSNHelper.getLogSequenceNumberFromPage(cacheEntry.dataPointer, directMemory);
      OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLSN();
      if (flushedLSN == null || flushedLSN.compareTo(lsn) < 0)
        writeAheadLog.flush();
    }

    final byte[] content = directMemory.get(cacheEntry.dataPointer, pageSize);
    OLongSerializer.INSTANCE.serializeNative(OReadWriteCache.MAGIC_NUMBER, content, 0);

    final int crc32 = OCRCCalculator.calculatePageCrc(content);
    OIntegerSerializer.INSTANCE.serializeNative(crc32, content, OLongSerializer.LONG_SIZE);

    final OFileClassic fileClassic = files.get(cacheEntry.fileId);

    fileClassic.write(cacheEntry.pageIndex * pageSize, content);

    if (syncOnPageFlush)
      fileClassic.synch();
  }

  private void unlockWriteGroup(List<OReadWriteCache.FileLockKey> keysToUnlock) {
    for (OReadWriteCache.FileLockKey fileLockKey : keysToUnlock) {
      entriesLocks.get(fileLockKey).writeLock().unlock();
    }
  }

  private void rollbackRecordsLock(List<OReadWriteCache.FileLockKey> lockedEntries, int lastLockedEntry) {
    for (int j = lastLockedEntry - 1; j > 0; j--) {
      OReadWriteCache.FileLockKey lockKey = lockedEntries.get(j);
      entriesLocks.get(lockKey).writeLock().unlock();
    }
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

  public void closeFile(long fileId, Set<Long> pageIndexes, boolean flush) throws IOException {
    Long[] sortedPageIndexes = new Long[pageIndexes.size()];
    sortedPageIndexes = pageIndexes.toArray(sortedPageIndexes);
    Arrays.sort(sortedPageIndexes);

    final SortedMap<Long, OLogSequenceNumber> fileDirtyPages = dirtyPages.get(fileId);

    for (Long pageIndex : sortedPageIndexes) {
      OCacheEntry lruEntry = get(fileId, pageIndex);
      if (lruEntry != null) {
        if (lruEntry.usageCounter == 0) {
          // lruEntry = remove(fileId, pageIndex);

          flushData(lruEntry);
          fileDirtyPages.remove(pageIndex);

          directMemory.free(lruEntry.dataPointer);
        }
      }
    }
    // TODO why?!
    pageIndexes.clear();
  }

  public void clearDirtyPages(long fileId) {
    dirtyPages.get(fileId).clear();
  }

  ConcurrentSkipListMap<OReadWriteCache.FileLockKey, OCacheEntry> getCache() {
    return cache;
  }

  public void startFlush() {
    if (commitDelay > 0)
      commitExecutor.scheduleAtFixedRate(new FlushTask(), commitDelay, commitDelay, TimeUnit.MILLISECONDS);
  }

  private final class FlushTask implements Runnable {

    @Override
    public void run() {
      double threshold = ((double) cache.size()) / maxSize;
      if (threshold > THRESHOLD1) {
        flushOne();
      } else if (threshold > THRESHOLD2) {
        flushOne();
      } else {
        forceFlush();
      }
    }
  }
}
