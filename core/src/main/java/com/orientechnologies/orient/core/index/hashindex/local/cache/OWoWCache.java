package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
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
  private static final double                                                   THRESHOLD3       = 0.7;
  private OReadWriteCache.FileLockKey                                           currentFlushPointer;

  private static final long                                                     WRITE_GROUP_SIZE = 4;
  private final ConcurrentMap<OReadWriteCache.FileLockKey, ReadWriteLock>       entriesLocks;
  private int                                                                   commitDelay      = 1000;
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
  private volatile long                                                         oldChanges;
  private volatile long                                                         currentChanges;

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
    this.currentFlushPointer = new OReadWriteCache.FileLockKey(0, 0);
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
      if (cache.size() >= maxSize) {
        flushOne(true);
      }
      long dataPointer = cacheFileContent(fileId, pageIndex);

      System.out.println("7");
      final OLogSequenceNumber lsn = OLSNHelper.getLogSequenceNumberFromPage(dataPointer, directMemory);

      OCacheEntry newCacheEntry = new OCacheEntry(fileId, pageIndex, lsn);
      newCacheEntry.dataPointer = dataPointer;
      cache.put(fileLockKey, newCacheEntry);
      cacheEntry = newCacheEntry;
    }
    return cacheEntry;
  }

  private OCacheEntry putToCache(OCacheEntry cachedEntry) throws IOException {
    assert cachedEntry != null;
    OReadWriteCache.FileLockKey fileLockKey = new OReadWriteCache.FileLockKey(cachedEntry.fileId, cachedEntry.pageIndex);
    OCacheEntry result = cache.get(fileLockKey);
    if (result == null) {
      if (cache.size() >= maxSize) {
        flushOne(true);
      }
      cache.put(fileLockKey, cachedEntry);
      result = cachedEntry;
    }
    return result;
  }

  private long cacheFileContent(long fileId, long pageIndex) throws IOException {
    final OFileClassic fileClassic = files.get(fileId);
    final long startPosition = pageIndex * pageSize;
    final long endPosition = startPosition + pageSize;

    assert endPosition > startPosition;

    byte[] content = new byte[pageSize];
    long dataPointer;
    if (fileClassic.getFilledUpTo() >= endPosition) {
      fileClassic.read(startPosition, content, content.length);
      System.out.println("1");
      dataPointer = directMemory.allocate(content);
    } else {
      fileClassic.allocateSpace((int) (endPosition - fileClassic.getFilledUpTo()));
      System.out.println("2");
      dataPointer = directMemory.allocate(content);
    }

    return dataPointer;
  }

  private void doMarkDirty(OCacheEntry cacheEntry) {
    if (!cacheEntry.inWriteCache) {
      dirtyPages.get(cacheEntry.fileId).put(cacheEntry.pageIndex, cacheEntry.loadedLSN);
      cacheEntry.inWriteCache = true;
    }

    cacheEntry.recentlyChanged = true;
  }

  public void clear() {
    cache.clear();
  }

  public OCacheEntry get(long fileId, long pageIndex) {
    return cache.get(new OReadWriteCache.FileLockKey(fileId, pageIndex));
  }

  public void remove(long fileId, long pageIndex) {
    OCacheEntry lruEntry = get(fileId, pageIndex);
    if (lruEntry != null) {
      OReadWriteCache.FileLockKey key = new OReadWriteCache.FileLockKey(fileId, pageIndex);
      entriesLocks.putIfAbsent(key, new ReentrantReadWriteLock());
      ReadWriteLock lock = entriesLocks.get(key);
      lock.writeLock().lock();
      try {
        if (lruEntry.inWriteCache && lruEntry.usageCounter == 0) {
          cache.remove(key);
          if (lruEntry.dataPointer != ODirectMemory.NULL_POINTER && !lruEntry.inReadCache) {

            System.out.println("8 " + lruEntry.dataPointer);
            directMemory.free(lruEntry.dataPointer);
          }
          lruEntry.inWriteCache = false;
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  public void deleteFile(long fileId) {
    dirtyPages.remove(fileId);
  }

  public void fillDirtyPages(long fileId) {
    dirtyPages.put(fileId, new TreeMap<Long, OLogSequenceNumber>());
  }

  private void flushOne(boolean forceFlush) throws IOException {
    Map<OReadWriteCache.FileLockKey, OCacheEntry> writeGroupEntries;
    final OReadWriteCache.FileLockKey initialFlushPointer = getInitialFlushPointer();
    OReadWriteCache.FileLockKey flushPointer = initialFlushPointer;
    OReadWriteCache.FileLockKey prevFlushPointer;
    boolean unsuccessfulFlush;
    do {
      unsuccessfulFlush = false;
      do {
        writeGroupEntries = getWriteGroup(flushPointer);
        prevFlushPointer = flushPointer;
        flushPointer = getNextFlushPointer(flushPointer);
      } while (writeGroupEntries.isEmpty() && ringIsNotPassed(initialFlushPointer, prevFlushPointer, flushPointer));

      if (!writeGroupEntries.isEmpty()) {
        boolean recordsIsFresh = false;
        if (!forceFlush) {
          for (OCacheEntry cacheEntry : writeGroupEntries.values()) {
            if (cacheEntry.recentlyChanged) {
              recordsIsFresh = true;
            }
            cacheEntry.recentlyChanged = false;
          }
        }
        if (forceFlush || !recordsIsFresh) {
          List<OReadWriteCache.FileLockKey> keysToFlush = lockWriteGroup(new ArrayList<OReadWriteCache.FileLockKey>(
              writeGroupEntries.keySet()));
          // flush
          try {
            for (OReadWriteCache.FileLockKey fileLockKey : keysToFlush) {
              OCacheEntry cacheEntry = cache.get(fileLockKey);
              entriesLocks.putIfAbsent(fileLockKey, new ReentrantReadWriteLock());
              ReadWriteLock lock = entriesLocks.get(fileLockKey);
              lock.writeLock().lock();
              try {
                if (cacheEntry.usageCounter != 0) {
                  unsuccessfulFlush = true;
                }
                flushData(cacheEntry);
                cacheEntry.inWriteCache = false;
                cache.remove(fileLockKey);
              } finally {
                lock.writeLock().unlock();
              }
            }
            if (!keysToFlush.isEmpty()) {
              OFileClassic fileClassic = files.get(keysToFlush.get(0).fileId);
              fileClassic.synch();
            }
          } finally {
            unlockWriteGroup(keysToFlush);
          }
        }

      }
    } while (unsuccessfulFlush);
  }

  private boolean ringIsNotPassed(OReadWriteCache.FileLockKey initialFlushPointer, OReadWriteCache.FileLockKey flushPointer,
      OReadWriteCache.FileLockKey nextFlushPointer) {
    return (flushPointer.compareTo(initialFlushPointer) <= 0 && nextFlushPointer.compareTo(initialFlushPointer) >= 0)
        || (nextFlushPointer.compareTo(flushPointer) <= 0);
  }

  private OReadWriteCache.FileLockKey getInitialFlushPointer() {
    final long initialFlushPointer;
    final Long flushPointerCandidate = currentFlushPointer.pageIndex;
    if (flushPointerCandidate == null) {
      initialFlushPointer = (long) 0;
    } else {
      initialFlushPointer = ((flushPointerCandidate >>> WRITE_GROUP_SIZE) << WRITE_GROUP_SIZE);
    }
    return new OReadWriteCache.FileLockKey(currentFlushPointer.fileId, initialFlushPointer);
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

  private Map<OReadWriteCache.FileLockKey, OCacheEntry> getWriteGroup(final OReadWriteCache.FileLockKey writeGroupFirstPage) {
    Map<OReadWriteCache.FileLockKey, OCacheEntry> entriesToFlush = new LinkedHashMap<OReadWriteCache.FileLockKey, OCacheEntry>(16);
    Map.Entry<OReadWriteCache.FileLockKey, OCacheEntry> entryToFlush = cache.ceilingEntry(writeGroupFirstPage);
    while (isNextPageInGroupExists(writeGroupFirstPage, entryToFlush)) {
      entriesToFlush.put(entryToFlush.getKey(), entryToFlush.getValue());
      entryToFlush = cache.higherEntry(entryToFlush.getKey());
    }
    return entriesToFlush;
  }

  private boolean isNextPageInGroupExists(OReadWriteCache.FileLockKey writeGroupFirstPage,
      Map.Entry<OReadWriteCache.FileLockKey, OCacheEntry> entryToFlush) {
    return entryToFlush != null && (entryToFlush.getKey().pageIndex <= (writeGroupFirstPage.pageIndex + 16 - 1))
        && writeGroupFirstPage.fileId == entryToFlush.getKey().fileId;
  }

  private void flushData(OCacheEntry cacheEntry) throws IOException {
    if (writeAheadLog != null) {
      OLogSequenceNumber lsn = OLSNHelper.getLogSequenceNumberFromPage(cacheEntry.dataPointer, directMemory);
      OLogSequenceNumber flushedLSN = writeAheadLog.getFlushedLSN();
      if (flushedLSN == null || flushedLSN.compareTo(lsn) < 0)
        writeAheadLog.flush();
    }
    long dataPointer = cacheEntry.dataPointer;
    System.out.println("6a " + dataPointer);
    final byte[] content = directMemory.get(dataPointer, pageSize);
    System.out.println("6b " + dataPointer);
    OLongSerializer.INSTANCE.serializeNative(OReadWriteCache.MAGIC_NUMBER, content, 0);

    final int crc32 = OCRCCalculator.calculatePageCrc(content);
    OIntegerSerializer.INSTANCE.serializeNative(crc32, content, OLongSerializer.LONG_SIZE);

    final OFileClassic fileClassic = files.get(cacheEntry.fileId);

    fileClassic.write(cacheEntry.pageIndex * pageSize, content);

    if (syncOnPageFlush)
      fileClassic.synch();

    dirtyPages.get(cacheEntry.fileId).remove(cacheEntry.pageIndex);
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
          cache.remove(new OReadWriteCache.FileLockKey(fileId, pageIndex));
          lruEntry.inWriteCache = false;
          if (flush) {
            flushData(lruEntry);
          }
          fileDirtyPages.remove(pageIndex);

          System.out.println("3");
          directMemory.free(lruEntry.dataPointer);
        }
      }
    }

    pageIndexes.clear();
  }

  public void clearDirtyPages(long fileId) {
    dirtyPages.get(fileId).clear();
  }

  ConcurrentSkipListMap<OReadWriteCache.FileLockKey, OCacheEntry> getCache() {
    return cache;
  }

  public void startFlush() {
    if (commitDelay > 0 && commitExecutor.isShutdown())
      commitExecutor.scheduleAtFixedRate(new FlushTask(), commitDelay, commitDelay, TimeUnit.MILLISECONDS);
  }

  public void stopFlush() {
    commitExecutor.shutdown();
  }

  public OReadWriteCache.FileLockKey getNextFlushPointer(OReadWriteCache.FileLockKey flushPointer) {
    OReadWriteCache.FileLockKey potentialNextPointer = new OReadWriteCache.FileLockKey(flushPointer.fileId,
        flushPointer.pageIndex + 16);
    OReadWriteCache.FileLockKey key = cache.higherKey(potentialNextPointer);
    if (key == null || key.fileId == flushPointer.fileId) {
      return potentialNextPointer;
    } else {
      return key;
    }
  }

  public void flushFile(long fileId) throws IOException {
    final OFileClassic fileClassic = files.get(fileId);
    if (fileClassic == null || !fileClassic.isOpen())
      return;
    List<OReadWriteCache.FileLockKey> keysToFlush = lockFilePages(fileId);
    // flush
    try {
      for (OReadWriteCache.FileLockKey fileLockKey : keysToFlush) {
        OCacheEntry cacheEntry = cache.get(fileLockKey);
        if (cacheEntry.usageCounter != 0) {
          throw new OBlockedPageException("Unable to perform flush file because page [" + cacheEntry.fileId + ", "
              + cacheEntry.pageIndex + "] is in use.");
        }
        flushData(cacheEntry);
        cacheEntry.inWriteCache = false;
        cache.remove(fileLockKey);
      }
    } finally {
      unlockWriteGroup(keysToFlush);
    }
    fileClassic.synch();
  }

  private List<OReadWriteCache.FileLockKey> lockFilePages(final long fileId) {
    OReadWriteCache.FileLockKey firstKey = new OReadWriteCache.FileLockKey(fileId, 0);
    OReadWriteCache.FileLockKey fileLockKey = cache.ceilingKey(firstKey);
    if (fileLockKey != null) {
      System.out.println(fileLockKey + " rly! " + cache.get(fileLockKey));
    }
    List<OReadWriteCache.FileLockKey> result = new ArrayList<OReadWriteCache.FileLockKey>(cache.size());
    while (fileLockKey != null && fileLockKey.fileId == fileId) {
      entriesLocks.putIfAbsent(fileLockKey, new ReentrantReadWriteLock());
      ReadWriteLock lock = entriesLocks.get(fileLockKey);
      lock.writeLock().lock();
      System.out.println(fileLockKey + " locked! " + cache.get(fileLockKey));
      result.add(fileLockKey);
      fileLockKey = cache.higherKey(fileLockKey);
    }
    return result;
  }

  public int getSize() {
    return cache.size();
  }

  private final class FlushTask implements Runnable {
    @Override
    public void run() {
      double threshold = ((double) cache.size()) / maxSize;
      if (threshold > THRESHOLD1) {
        flushUntilThreshold(THRESHOLD2, false);
      } else if (threshold > THRESHOLD2) {
        if (((oldChanges + currentChanges) / cache.size()) > THRESHOLD3) {
          flushUntilThreshold(THRESHOLD2, true);
        } else {
          flushUntilThreshold(THRESHOLD2, false);
        }
      } else if (cache.size() > 0) {
        try {
          flushOne(false);
        } catch (IOException e) {
          // todo change to normal exception
          e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
        }
      }
      oldChanges = currentChanges;
      currentChanges = 0;
    }

    private void flushUntilThreshold(final double threshold, final boolean forceFlush) {
      while (((double) cache.size()) / maxSize > threshold) {
        try {
          flushOne(forceFlush);
        } catch (IOException e) {
          // todo change to normal exception
          e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
        }
      }
    }
  }
}
