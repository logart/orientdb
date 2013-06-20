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

  private int                           K_IN;
  private int                           K_OUT;

  private LRUList                       am;
  private LRUList                       a1out;
  private LRUList                       a1in;

  private int                           maxSize;

  private final int                     pageSize;

  private final Map<Long, OFileClassic> files;

  /**
   * Contains all pages in cache for given file, not only dirty onces.
   */
  private final Map<Long, Set<Long>>    filePages;

  private ODirectMemory                 directMemory;

  O2QCache(Map<Long, OFileClassic> files, Map<Long, Set<Long>> filePages, int pageSize, ODirectMemory directMemory) {
    am = new LRUList();
    a1out = new LRUList();
    a1in = new LRUList();

    this.filePages = filePages;
    this.files = files;

    this.pageSize = pageSize;

    this.directMemory = directMemory;

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

  private OCacheEntry updateCache(long fileId, long pageIndex) throws IOException {
    OCacheEntry lruEntry = am.get(fileId, pageIndex);
    if (lruEntry != null) {
      lruEntry = am.putToMRU(fileId, pageIndex, lruEntry.dataPointer, lruEntry.isDirty, lruEntry.loadedLSN);

      return lruEntry;
    }

    lruEntry = a1out.remove(fileId, pageIndex);
    if (lruEntry != null) {
      removeColdestPageIfNeeded();

      CacheResult cacheResult = cacheFileContent(fileId, pageIndex);
      lruEntry.dataPointer = cacheResult.dataPointer;
      lruEntry.isDirty = cacheResult.isDirty;

      OLogSequenceNumber lsn;
      // if (cacheResult.isDirty)
      // TODO
      // lsn = dirtyPages.get(fileId).get(pageIndex);
      // else
      // lsn = getLogSequenceNumberFromPage(cacheResult.dataPointer);

      lruEntry = am.putToMRU(fileId, pageIndex, lruEntry.dataPointer, lruEntry.isDirty, lsn);
      return lruEntry;
    }

    lruEntry = a1in.get(fileId, pageIndex);
    if (lruEntry != null)
      return lruEntry;

    removeColdestPageIfNeeded();

    CacheResult cacheResult = cacheFileContent(fileId, pageIndex);
    OLogSequenceNumber lsn;
    // if (cacheResult.isDirty)
    // lsn = dirtyPages.get(fileId).get(pageIndex);
    // else
    // todo
    // lsn = getLogSequenceNumberFromPage(cacheResult.dataPointer);

    lruEntry = a1in.putToMRU(fileId, pageIndex, cacheResult.dataPointer, cacheResult.isDirty, lsn);

    filePages.get(fileId).add(pageIndex);

    return lruEntry;
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
            directMemory.free(removedFromAInEntry.dataPointer);
          }

          a1out.putToMRU(removedFromAInEntry.fileId, removedFromAInEntry.pageIndex, ODirectMemory.NULL_POINTER, false, null);
        }
        if (a1out.size() > K_OUT) {
          OCacheEntry removedEntry = a1out.removeLRU();
          assert removedEntry.usageCounter == 0;
          Set<Long> pageEntries = filePages.get(removedEntry.fileId);
          pageEntries.remove(removedEntry.pageIndex);
        }
      } else {
        OCacheEntry removedEntry = am.removeLRU();
        if (removedEntry == null) {
          increaseCacheSize();
          return;
        } else {
          if (!removedEntry.inWriteCache) {
            assert removedEntry.usageCounter == 0;
            directMemory.free(removedEntry.dataPointer);
          }

      return errors.toArray(new OPageDataVerificationError[errors.size()]);
    }
  }

  @Override
  public Set<ODirtyPage> logDirtyPagesTable() throws IOException {
    synchronized (syncObject) {
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

  public OCacheEntry load(OCacheEntry entry) {
    return null; // To change body of created methods use File | Settings | File Templates.
  }

  public OCacheEntry load(long fileId, long pageIndex) {
    return null; // To change body of created methods use File | Settings | File Templates.
  }

  private static class CacheResult {
    private final boolean isDirty;
    private final long    dataPointer;

    private CacheResult(boolean dirty, long dataPointer) {
      isDirty = dirty;
      this.dataPointer = dataPointer;
    }
  }
}
