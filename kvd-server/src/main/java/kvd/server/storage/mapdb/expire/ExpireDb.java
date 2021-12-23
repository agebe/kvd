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
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import kvd.server.Key;
import kvd.server.list.KvdLinkedList;
import kvd.server.list.MapByteArrayKvdListStore;
import kvd.server.storage.mapdb.Value;
import kvd.server.storage.mapdb.ValueType;
import kvd.server.util.FileUtils;

public class ExpireDb {

  private DB db;

  private HTreeMap<byte[], byte[]> map;

  private KvdLinkedList<Timestamp> created;

  private KvdLinkedList<Timestamp> accessed;

  private ReentrantLock lock = new ReentrantLock(true);

  public ExpireDb(File base) {
    super();
    File expiredb = new File(base, "expiredb");
    FileUtils.createDirIfMissing(expiredb);
    db = DBMaker
        .fileDB(new File(expiredb, "map"))
        .transactionEnable()
        .fileMmapEnable()
        .closeOnJvmShutdown()
        .make();
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

}
