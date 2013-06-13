package com.orientechnologies.orient.core.index.hashindex.local.cache;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

public class OCacheEntry {
  long               fileId;
  long               pageIndex;

  OLogSequenceNumber loadedLSN;

  volatile long      dataPointer;

  volatile int       usageCounter = 0;

  volatile boolean   inReadCache;     // will be accessible from flush thread
  boolean            inWriteCache;    // volatile is redundant because of access only from cache thread

  // TODO remove
  @Deprecated
  boolean            isDirty;
}
