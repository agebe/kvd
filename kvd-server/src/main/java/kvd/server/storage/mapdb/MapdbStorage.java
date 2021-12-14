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
package kvd.server.storage.mapdb;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.mapdb.DB;
import org.mapdb.DB.HashMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.server.Key;
import kvd.server.util.FileUtils;
import kvd.server.util.HumanReadable;

public class MapdbStorage {

  private static final Logger log = LoggerFactory.getLogger(MapdbStorage.class);

  private File mapdb;

  private File blobs;

  private HTreeMap<byte[], byte[]> map;

  private DB db;

  private Long expireAfterAccessMs;
  private Long expireAfterWriteMs;
  private Long expireIntervalMs;

  private List<Consumer<Key>> expireListeners = new ArrayList<>();

  public MapdbStorage(
      File base,
      Long expireAfterAccessMs,
      Long expireAfterWriteMs,
      Long expireIntervalMs) {
    super();
    this.mapdb = new File(base, "mapdb");
    this.blobs = new File(base, "blobs");
    this.expireAfterAccessMs = expireAfterAccessMs;
    this.expireAfterWriteMs = expireAfterWriteMs;
    this.expireIntervalMs = expireIntervalMs;
    FileUtils.createDirIfMissing(mapdb);
    FileUtils.createDirIfMissing(blobs);
    // TODO test corrupted database
    // maybe add try catch, if opening fails delete the folders and try again
    // data seems to be lost so it might be better to automatically start with a clean db
    db = DBMaker
        .fileDB(new File(mapdb, "map"))
        .transactionEnable()
        .fileMmapEnable()
        .closeOnJvmShutdown()
        .make();
    var builder = db
        .hashMap("map")
        .keySerializer(Serializer.BYTE_ARRAY)
        .valueSerializer(Serializer.BYTE_ARRAY);
    setupExpire(builder);
    map = builder.createOrOpen();
  }

  private void setupExpire(HashMapMaker<byte[],byte[]> builder) {
    if((expireAfterAccessMs == null) && (expireAfterWriteMs == null)) {
      log.info("keys never expire");
      return;
    }
    long intervalMs = expireIntervalMs!=null?expireIntervalMs:Math.max(100, minExpireMs() / 10);
    log.info("expire after access '{}', expire after write '{}', check interval '{}'",
        HumanReadable.formatDurationOrEmpty(expireAfterAccessMs, TimeUnit.MILLISECONDS),
        HumanReadable.formatDurationOrEmpty(expireAfterWriteMs, TimeUnit.MILLISECONDS),
        HumanReadable.formatDuration(intervalMs, TimeUnit.MILLISECONDS));
    if(expireAfterAccessMs != null) {
      builder.expireAfterGet(expireAfterAccessMs, TimeUnit.MILLISECONDS);
    }
    if(expireAfterWriteMs != null) {
      builder.expireAfterCreate(expireAfterWriteMs, TimeUnit.MILLISECONDS);
      builder.expireAfterUpdate(expireAfterWriteMs, TimeUnit.MILLISECONDS);
    }
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
    builder.modificationListener((k, nv, ov, triggered) -> {
      Key key = new Key(k);
      log.trace("map modification event on key '{}', triggered '{}'", key, triggered);
      if(triggered) {
        log.info("key expired '{}'", key);
        deleteExpiredBlob(key, Value.deserialize(ov));
        notifyExpireListeners(key);
      }
    });
    builder.expireExecutor(executor);
    builder.expireExecutorPeriod(intervalMs);
  }

  private void deleteExpiredBlob(Key k, Value v) {
    // TODO
  }

  public synchronized void registerExpireListener(Consumer<Key> listener) {
    expireListeners.add(listener);
  }

  private synchronized void notifyExpireListeners(Key key) {
    expireListeners.forEach(l -> l.accept(key));
  }

  private long minExpireMs() {
    if((expireAfterAccessMs == null) && (expireAfterWriteMs == null)) {
      return -1;
    } else if((expireAfterAccessMs != null) && (expireAfterWriteMs != null)) {
      return Math.min(expireAfterAccessMs, expireAfterWriteMs);
    } else if(expireAfterAccessMs != null) {
      return expireAfterAccessMs;
    } else {
      return expireAfterWriteMs;
    }
  }

  synchronized InputStream get(Key key) {
    return toInputStream(Value.deserialize(map.get(key.getBytes())));
  }

  private InputStream toInputStream(Value v) {
    if(v != null) {
      ValueType vt = v.getType();
      if(ValueType.INLINE.equals(vt)) {
        return new ByteArrayInputStream(v.inline());
      } else if(ValueType.BLOB.equals(vt)) {
        throw new KvdException("blob support not implemented yet"); 
      } else {
        throw new KvdException("invalid value type " + vt);
      }
    } else {
      return null;
    }
  }

  synchronized boolean contains(Key key) {
    return map.containsKey(key.getBytes());
  }

  synchronized void removeAll() {
    map.clear();
    db.commit();
  }

  synchronized void commit(Map<Key, Value> m) {
    try {
      m.forEach((k,v) -> {
        if(ValueType.REMOVE.equals(v.getType())) {
          map.remove(k.getBytes());
        } else {
          map.put(k.getBytes(), Value.serialize(v));
        }
      });
      db.commit();
    } catch(Throwable t) {
      log.error("failed to write key/values into mapdb, rollback transaction", t);
      db.rollback();
    }
  }

}
