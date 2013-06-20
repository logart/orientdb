package com.orientechnologies.orient.core.index.hashindex.local.cache;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

public class OLSNHelper {
  public static OLogSequenceNumber getLogSequenceNumberFromPage(long dataPointer, ODirectMemory directMemory) {
    final long position = OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, dataPointer
        + OLongSerializer.LONG_SIZE + (2 * OIntegerSerializer.INT_SIZE));
    final int segment = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, dataPointer
        + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);

    return new OLogSequenceNumber(segment, position);
  }
}
