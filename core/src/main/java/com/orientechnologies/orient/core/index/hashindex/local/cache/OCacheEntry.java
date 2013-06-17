package com.orientechnologies.orient.core.index.hashindex.local.cache;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

public class OCacheEntry {
  final long               fileId;
  final long               pageIndex;

  final OLogSequenceNumber loadedLSN;

  volatile long            dataPointer;

  volatile int             usageCounter = 0;

  volatile boolean         inReadCache;     // will be accessible from flush thread
  boolean                  inWriteCache;    // volatile is redundant because of access only from cache thread

  // TODO remove
  @Deprecated
  volatile boolean         isDirty;

  public OCacheEntry(long fileId, long pageIndex, OLogSequenceNumber loadedLSN) {
    this.fileId = fileId;
    this.pageIndex = pageIndex;
    this.loadedLSN = loadedLSN;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OCacheEntry that = (OCacheEntry) o;

    if (dataPointer != that.dataPointer)
      return false;
    if (fileId != that.fileId)
      return false;
    if (isDirty != that.isDirty)
      return false;
    if (pageIndex != that.pageIndex)
      return false;
    if (loadedLSN != null ? !loadedLSN.equals(that.loadedLSN) : that.loadedLSN != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (fileId ^ (fileId >>> 32));
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + (loadedLSN != null ? loadedLSN.hashCode() : 0);
    result = 31 * result + (int) (dataPointer ^ (dataPointer >>> 32));
    result = 31 * result + (isDirty ? 1 : 0);
    return result;
  }
}
