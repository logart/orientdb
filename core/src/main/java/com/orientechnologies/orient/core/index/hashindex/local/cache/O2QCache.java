/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OAllLRUListEntriesAreUsedException;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Artem Loginov
 * @since 14.03.13
 */
class O2QCache {

  private int                                                             K_IN;
  private int                                                             K_OUT;

  private LRUList                                                         am;
  private LRUList                                                         a1out;
  private LRUList                                                         a1in;

  private int                                                             maxSize;

  private final int                                                       pageSize;

  private final Map<Long, OFileClassic>                                   files;

  /**
   * Contains all pages in cache for given file, not only dirty onces.
   */
  private final Map<Long, Set<Long>>                                      filePages;

  private ODirectMemory                                                   directMemory;
  private final ConcurrentMap<OReadWriteCache.FileLockKey, ReadWriteLock> entriesLocks;

  O2QCache(int maxSize, Map<Long, OFileClassic> files, Map<Long, Set<Long>> filePages, int pageSize, ODirectMemory directMemory,
      ConcurrentMap<OReadWriteCache.FileLockKey, ReadWriteLock> entriesLocks) {
    am = new LRUList();
    a1out = new LRUList();
    a1in = new LRUList();

    this.filePages = filePages;
    this.files = files;

    this.pageSize = pageSize;

    this.directMemory = directMemory;

    this.maxSize = maxSize;

    this.entriesLocks = entriesLocks;

    K_IN = maxSize >> 2;
    K_OUT = maxSize >> 1;
  }

  public void clear() {
    am.clear();
    a1in.clear();
    a1out.clear();
    for (Set<Long> fileEntries : filePages.values())
      fileEntries.clear();
  }

  public OCacheEntry load(OCacheEntry entry) throws IOException {
    OReadWriteCache.FileLockKey key = new OReadWriteCache.FileLockKey(entry.fileId, entry.pageIndex);
    entriesLocks.putIfAbsent(key, new ReentrantReadWriteLock());
    ReadWriteLock lock = entriesLocks.get(key);
    lock.readLock().lock();
    try {
      OCacheEntry cacheEntry = updateCache(entry.fileId, entry.pageIndex, entry.dataPointer);
      entry.inReadCache = true;
      return cacheEntry;
    } finally {
      lock.readLock().unlock();
    }
  }

  public OCacheEntry load(long fileId, long pageIndex) throws IOException {
    final OCacheEntry entry = updateCache(fileId, pageIndex, ODirectMemory.NULL_POINTER);
    entry.inReadCache = true;
    return entry;
  }

  private OCacheEntry updateCache(long fileId, long pageIndex, long dataPointer) throws IOException {
    OCacheEntry lruEntry = am.get(fileId, pageIndex);
    if (lruEntry != null) {
      lruEntry = am.putToMRU(fileId, pageIndex, lruEntry.dataPointer, lruEntry.loadedLSN);

      return lruEntry;
    }

    lruEntry = a1out.remove(fileId, pageIndex);
    if (lruEntry != null) {
      return putEntryToLongLeaveCache(lruEntry);
    }

    lruEntry = a1in.get(fileId, pageIndex);
    if (lruEntry != null)
      return lruEntry;

    lruEntry = putRecordToShortLeaveCache(fileId, pageIndex, dataPointer);

    filePages.get(fileId).add(pageIndex);

    return lruEntry;
  }

  private OCacheEntry putEntryToLongLeaveCache(OCacheEntry cacheEntry) throws IOException {
    removeColdestPageIfNeeded();

    cacheEntry.dataPointer = cacheFileContent(cacheEntry.fileId, cacheEntry.pageIndex);

    OLogSequenceNumber lsn;
    if (cacheEntry.inWriteCache)
      lsn = cacheEntry.loadedLSN;
    else {
      System.out.println("q3");
      lsn = OLSNHelper.getLogSequenceNumberFromPage(cacheEntry.dataPointer, directMemory);
    }

    cacheEntry = am.putToMRU(cacheEntry.fileId, cacheEntry.pageIndex, cacheEntry.dataPointer, lsn);
    return cacheEntry;

  }

  private OCacheEntry putRecordToShortLeaveCache(long fileId, long pageIndex, long dataPointer) throws IOException {
    removeColdestPageIfNeeded();

    if (dataPointer == ODirectMemory.NULL_POINTER) {
      dataPointer = cacheFileContent(fileId, pageIndex);
    }

    System.out.println("q4a " + dataPointer);
    OLogSequenceNumber lsn = OLSNHelper.getLogSequenceNumberFromPage(dataPointer, directMemory);
    System.out.println("q4b " + dataPointer);

    return a1in.putToMRU(fileId, pageIndex, dataPointer, lsn);
  }

  private long cacheFileContent(long fileId, long pageIndex) throws IOException {
    final OFileClassic fileClassic = files.get(fileId);
    final long startPosition = pageIndex * pageSize;
    final long endPosition = startPosition + pageSize;

    byte[] content = new byte[pageSize];
    long dataPointer;
    if (fileClassic.getFilledUpTo() >= endPosition) {
      fileClassic.read(startPosition, content, content.length);
      System.out.println("q1");
      dataPointer = directMemory.allocate(content);
    } else {
      fileClassic.allocateSpace((int) (endPosition - fileClassic.getFilledUpTo()));
      System.out.println("q2");
      dataPointer = directMemory.allocate(content);
    }

    return dataPointer;
  }

  private void removeColdestPageIfNeeded() throws IOException {
    if (am.size() + a1in.size() >= maxSize) {
      if (a1in.size() > K_IN) {
        OCacheEntry removedFromAInEntry = a1in.removeLRU();
        if (removedFromAInEntry == null) {
          increaseCacheSize();
          return;
        } else {
          if (!removedFromAInEntry.inWriteCache) {
            assert removedFromAInEntry.usageCounter == 0;
            System.out.println("q5");
            directMemory.free(removedFromAInEntry.dataPointer);
          }

          a1out.putToMRU(removedFromAInEntry.fileId, removedFromAInEntry.pageIndex, ODirectMemory.NULL_POINTER, null);
        }
        if (a1out.size() > K_OUT) {
          OCacheEntry removedEntry = a1out.removeLRU();
          assert removedEntry.usageCounter == 0;
          removedEntry.inReadCache = false;
          Set<Long> pageEntries = filePages.get(removedEntry.fileId);
          pageEntries.remove(removedEntry.pageIndex);
        }
      } else {
        OCacheEntry removedEntry = am.removeLRU();
        if (removedEntry == null) {
          increaseCacheSize();
        } else {

          if (!removedEntry.inWriteCache) {
            assert removedEntry.usageCounter == 0;
            System.out.println("q6");
            directMemory.free(removedEntry.dataPointer);
            Set<Long> pageEntries = filePages.get(removedEntry.fileId);
            pageEntries.remove(removedEntry.pageIndex);
          }
          removedEntry.inReadCache = false;
        }
      }
    }
  }

  public OCacheEntry get(long fileId, long pageIndex) {
    OCacheEntry lruEntry = am.get(fileId, pageIndex);

    if (lruEntry != null) {
      return lruEntry;
    }

    lruEntry = a1in.get(fileId, pageIndex);
    return lruEntry;
  }

  private void increaseCacheSize() {
    String message = "All records in aIn queue in 2q cache are used!";
    OLogManager.instance().warn(this, message);
    if (OGlobalConfiguration.SERVER_CACHE_2Q_INCREASE_ON_DEMAND.getValueAsBoolean()) {
      OLogManager.instance().warn(this, "Cache size will be increased.");
      maxSize = (int) Math.ceil(maxSize * (1 + OGlobalConfiguration.SERVER_CACHE_2Q_INCREASE_STEP.getValueAsFloat()));
      K_IN = maxSize >> 2;
      K_OUT = maxSize >> 1;
    } else {
      throw new OAllLRUListEntriesAreUsedException(message);
    }
  }

  LRUList getAm() {
    return am;
  }

  LRUList getA1out() {
    return a1out;
  }

  LRUList getA1in() {
    return a1in;
  }

  public void closeFile(long fileId, Set<Long> filePages) {
    for (Long pageIndex : filePages) {
      remove(fileId, pageIndex);
    }
  }

  private OCacheEntry remove(long fileId, long pageIndex) {
    OCacheEntry lruEntry = am.remove(fileId, pageIndex);
    if (lruEntry != null) {
      if (lruEntry.usageCounter > 1)
        throw new IllegalStateException("Record cannot be removed because it is used!");
      return lruEntry;
    }
    lruEntry = a1out.remove(fileId, pageIndex);
    if (lruEntry != null) {
      return lruEntry;
    }
    lruEntry = a1in.remove(fileId, pageIndex);
    if (lruEntry != null && lruEntry.usageCounter > 1)
      throw new IllegalStateException("Record cannot be removed because it is used!");
    return lruEntry;
  }

  public void deleteFile(long fileId) {
    // todo
    // check if this method is necessary
  }

  public int getSize() {
    return a1in.size() + a1out.size() + am.size();
  }
}
