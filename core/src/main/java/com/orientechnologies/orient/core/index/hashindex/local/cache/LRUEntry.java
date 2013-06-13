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

/**
 * @author Andrey Lomakin
 * @since 25.02.13
 */
class LRUEntry {

  OCacheEntry value;

  long        hashCode;

  LRUEntry    next;

  LRUEntry    after;
  LRUEntry    before;

  LRUEntry() {
    this.value = new OCacheEntry();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    LRUEntry lruEntry = (LRUEntry) o;

    return value.equals(lruEntry.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public String toString() {
    return "LRUEntry{" + "value=" + value.toString() + '}';
  }
}
