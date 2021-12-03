/*
 * Copyright 2021 Andre Gebers
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package kvd.server.storage.timestamp;

import java.time.Instant;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import kvd.common.KvdException;
import kvd.server.Key;
import kvd.server.list.KvdLinkedList;
import kvd.server.storage.StorageBackend;

public class TimestampStore {

  private KvdLinkedList<Timestamp> created;

  private KvdLinkedList<Timestamp> accessed;

  public TimestampStore(StorageBackend backend) {
    created = new KvdLinkedList<Timestamp>(backend,
        "created",
        Timestamp::serialize,
        Timestamp::deserialize,
        Timestamp::getKey);
    accessed = new KvdLinkedList<Timestamp>(backend,
        "accessed",
        Timestamp::serialize,
        Timestamp::deserialize,
        Timestamp::getKey);
  }

  synchronized Set<Key> getExpired(Long expireAfterAccess, Long expireAfterWrite, int limit) {
    Instant now = Instant.now();
    Set<Key> expired = new LinkedHashSet<>();
    if(limit <= 0) {
      throw new KvdException("wrong limit (needs to be positive int), " + limit);
    }
    if(expireAfterAccess != null) {
      getExpired(accessed, now, expireAfterAccess, expired, limit);
    }
    if(expireAfterWrite != null) {
      getExpired(created, now, expireAfterWrite, expired, limit - expired.size());
    }
    return expired;
  }

  private synchronized void getExpired(KvdLinkedList<Timestamp> list,
      Instant now,
      long expiredDuration,
      Set<Key> expired,
      int limit) {
    Iterator<Timestamp> iter = list.iterator();
    for(int i=0;(i<limit)&&iter.hasNext();i++) {
      Timestamp ts = iter.next();
      Instant instant = Instant.ofEpochMilli(ts.getTimestamp());
      if(now.isAfter(instant.plusMillis(expiredDuration))) {
        expired.add(ts.getKey());
      } else {
        // list is ordered by timestamps
        break;
      }
    }
  }

  synchronized void recordChanges(Collection<TimestampRecord> c) {
    c.forEach(t -> {
      if(AccessType.CREATED.equals(t.getAccessType())) {
        created.lookupRemove(t.getKey());
        accessed.lookupRemove(t.getKey());
        created.add(t);
        accessed.add(t);
      } else if(AccessType.ACCESSED.equals(t.getAccessType())) {
        accessed.lookupRemove(t.getKey());
        accessed.add(t);
      } else if(AccessType.REMOVED.equals(t.getAccessType())) {
        created.lookupRemove(t.getKey());
        accessed.lookupRemove(t.getKey());
      } else {
        throw new KvdException("invalid/unknown access type: " + t.getAccessType());
      }
    });
  }

}
