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
package kvd.server.storage.mapdb.expire;

import java.io.File;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.server.Key;
import kvd.server.list.KvdLinkedList;
import kvd.server.list.MapByteArrayKvdListStore;
import kvd.server.storage.mapdb.Value;
import kvd.server.storage.mapdb.ValueType;
import kvd.server.util.FileUtils;
import kvd.server.util.HumanReadable;

public class ExpireDb {

  private static final Logger log = LoggerFactory.getLogger(ExpireDb.class);

  private DB db;

  private HTreeMap<byte[], byte[]> map;

  private KvdLinkedList<Timestamp> created;

  private KvdLinkedList<Timestamp> accessed;

  private ReentrantLock lock = new ReentrantLock(true);

  public ExpireDb(File base, boolean enableMmap) {
    super();
    File expiredb = new File(base, "expiredb");
    FileUtils.createDirIfMissing(expiredb);
    DBMaker.Maker dbBuilder = DBMaker
        .fileDB(new File(expiredb, "map"))
        .transactionEnable()
        .closeOnJvmShutdown();
    if(enableMmap) {
      dbBuilder.fileMmapEnable();
    }
    db = dbBuilder.make();
    var builder = db
        .hashMap("map")
        .keySerializer(Serializer.BYTE_ARRAY)
        .valueSerializer(Serializer.BYTE_ARRAY);
    map = builder.createOrOpen();
    created = new KvdLinkedList<Timestamp>(
        new MapByteArrayKvdListStore(map),
        "created",
        Timestamp::serialize,
        Timestamp::deserialize,
        Timestamp::getKey);
    accessed = new KvdLinkedList<Timestamp>(
        new MapByteArrayKvdListStore(map),
        "accessed",
        Timestamp::serialize,
        Timestamp::deserialize,
        Timestamp::getKey);
  }

  public void updateAll(Map<Key, Value> m) {
    lock.lock();
    try {
      m.forEach((k,v) -> {
        if(v != null) {
          if(ValueType.REMOVE.equals(v.getType())) {
            created.lookupRemove(k);
            accessed.lookupRemove(k);
          } else {
            created.lookupRemove(k);
            accessed.lookupRemove(k);
            Timestamp t = new Timestamp(k, v.getCreated().getEpochSecond());
            created.add(t);
            accessed.add(t);
          }
        }
      });
      db.commit();
    } finally {
      lock.unlock();
    }
  }

  public void accessed(Key key, Instant i) {
    lock.lock();
    try {
      if(key != null) {
        accessed.lookupRemove(key);
        Timestamp t = new Timestamp(key, i.getEpochSecond());
        accessed.add(t);
        db.commit();
      }
    } finally {
      lock.unlock();
    }
  }

  public Set<Key> getExpired(Long expireAfterAccessMs, Long expireAfterWriteMs, int limit) {
    lock.lock();
    try {
      Instant now = Value.now();
      Set<Key> expired = new LinkedHashSet<>();
      if(limit <= 0) {
        throw new KvdException("wrong limit (needs to be positive int), " + limit);
      }
      if(expireAfterAccessMs != null) {
        getExpired(accessed, now, expireAfterAccessMs, expired, limit, "access");
      }
      if(expireAfterWriteMs != null) {
        getExpired(created, now, expireAfterWriteMs, expired, limit - expired.size(), "write");
      }
      return expired;
    } finally {
      lock.unlock();
    }
  }

  private void getExpired(
      KvdLinkedList<Timestamp> list,
      Instant now,
      long expiredDuration,
      Set<Key> expired,
      int limit,
      String type) {
    Iterator<Timestamp> iter = list.iterator();
    for(int i=0;(i<limit)&&iter.hasNext();i++) {
      Timestamp ts = iter.next();
      Instant instant = Instant.ofEpochSecond(ts.getTimestamp());
      if(now.isAfter(instant.plusMillis(expiredDuration))) {
        log.trace("expire after {}, add key '{}' to expire list", type, ts.getKey());
        expired.add(ts.getKey());
      } else {
        // list is ordered by timestamps
        log.debug("expire after {}, break loop on key '{}', expires at '{}' (in '{}'), now '{}'",
            type,
            ts.getKey(),
            instant.plusMillis(expiredDuration),
            HumanReadable.formatDuration(instant.plusMillis(expiredDuration).getEpochSecond() - now.getEpochSecond()),
            now);
        break;
      }
    }
  }

  public long size() {
    lock.lock();
    try {
      return created.size();
    } finally {
      lock.unlock();
    }
  }

}
