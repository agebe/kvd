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

import java.io.File;
import java.io.IOException;
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
import kvd.server.storage.mapdb.expire.ExpireDb;
import kvd.server.util.FileUtils;
import kvd.server.util.HumanReadable;

public class MapdbStorage {

  private static final Logger log = LoggerFactory.getLogger(MapdbStorage.class);

  private File mapdb;

  private File blobs;

  private HTreeMap<byte[], byte[]> map;

  private DB db;

  private ExpireDb expireDb;

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
    // or try to rebuild the database from the blob files
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
    expireDb = new ExpireDb(base);
  }

  private void setupExpire(HashMapMaker<byte[],byte[]> builder) {
    final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    // when the HTreeMap calls into the modification listener it holds onto locks internally.
    // make sure that we only call into synchronized methods async to avoid deadlocks.
    builder.modificationListener((k, ov, nv, triggered) -> {
      Key key = new Key(k);
      Value oldValue = Value.deserialize(ov);
      Value newValue = Value.deserialize(nv);
      log.trace("map modification, key {}, triggered '{}', old-value {}, new-value {}",
          key, triggered, oldValue, newValue);
      deleteBlobs(oldValue, newValue);
      if(triggered) {
        // call notifyExpireListeners async to avoid deadlock
        executor.execute(() -> notifyExpireListeners(key));
      }
    });
//    if((expireAfterAccessMs == null) && (expireAfterWriteMs == null)) {
//      log.info("keys never expire");
//      return;
//    }
//    long intervalMs = expireIntervalMs!=null?expireIntervalMs:Math.max(100, minExpireMs() / 10);
//    log.info("expire after access '{}', expire after write '{}', check interval '{}'",
//        HumanReadable.formatDurationOrEmpty(expireAfterAccessMs, TimeUnit.MILLISECONDS),
//        HumanReadable.formatDurationOrEmpty(expireAfterWriteMs, TimeUnit.MILLISECONDS),
//        HumanReadable.formatDuration(intervalMs, TimeUnit.MILLISECONDS));
//    if(expireAfterAccessMs != null) {
//      builder.expireAfterGet(expireAfterAccessMs, TimeUnit.MILLISECONDS);
//    }
//    if(expireAfterWriteMs != null) {
//      builder.expireAfterCreate(expireAfterWriteMs, TimeUnit.MILLISECONDS);
//      builder.expireAfterUpdate(expireAfterWriteMs, TimeUnit.MILLISECONDS);
//    }
//    builder.expireExecutor(executor);
//    builder.expireExecutorPeriod(intervalMs);
//    // could work if there is a way to get values from the HTreeMap without changing the access time.
////    executor.scheduleWithFixedDelay(this::deleteOrphanedBlobs, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
  }

//  private void deleteOrphanedBlobs() {
//    try(Stream<Path> stream = Files.walk(blobs.toPath())) {
//      stream
//      .filter(Files::isRegularFile)
//      .map(Path::toFile)
//      .forEach(this::checkOrphanedBlob);
//    } catch(Throwable t) {
//      log.error("failed to check/delete orphaned blob files", t);
//    }
//  }
//
//  private void checkOrphanedBlob(File f) {
//    try(InputStream in = new BufferedInputStream(new FileInputStream(f))) {
//      BlobHeader header;
//      try {
//        header = BlobHeader.fromInputStream(in);
//      } catch(Exception e) {
//        // the blob directory should only contain blobs. if the header can't be loaded ignore the file
//        log.debug("failed to load header from blob file '{}', msg '{}'",
//            f.getAbsolutePath(), ExceptionUtils.getRootCauseMessage(e));
//        return;
//      }
//      if(header.getKey() == null) {
//        log.debug("no key stored in blob header '{}'", f.getAbsolutePath());
//        return;
//      }
//      Value v = getValue(header.getKey());
//      if((v == null) || (!checkIndex(f.getName(), header, v))) {
//        boolean deleted = f.delete();
//        if(deleted) {
//          log.info("deleted orphaned blob '{}'", f.getName());
//        }
//      }
//    } catch(Exception e) {
//      log.error("failed to check blob file '{}'", f.getAbsolutePath(), e);
//    }
//  }
//
//  private boolean checkIndex(String blobName, BlobHeader header, Value v) {
//    if(v.blobs() == null) {
//      return false;
//    }
//    if(v.blobs().size() <= header.getIndex()) {
//      return false;
//    }
//    return StringUtils.equals(blobName, v.blobs().get(header.getIndex()));
//  }

  private void deleteBlobs(Value ov, Value nv) {
    if(ov == null) {
      return;
    }
    if(!ov.isBlob()) {
      return;
    }
    if(nv == null) {
      deleteBlobs(ov.blobs());
    } else {
      if(!ov.blobs().equals(nv.blobs())) {
        var diff = new ArrayList<>(ov.blobs());
        diff.removeAll(nv.blobs());
        deleteBlobs(diff);
      }
    }
  }

  private void deleteBlobs(List<String> blobs) {
    for(String s : blobs) {
      File f = new File(getBlobs(), s);
      if(!f.exists()) {
        continue;
      }
      if(!f.delete()) {
        // TODO try to delete again later
      }
    }
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

  private synchronized Value getValue(Key key) {
    return Value.deserialize(map.get(key.getBytes()));
  }

  synchronized InputStream get(Key key) {
    Value v = getValue(key);
    if(v!=null) {
      v.setAccessed(Value.now());
      map.put(key.getBytes(), v.serialize());
      expireDb.accessed(key, v.getAccessed());
      return toInputStream(v);
    } else {
      return null;
    }
  }

  private InputStream toInputStream(Value v) {
    if(v != null) {
      if(v.isInline() || v.isBlob()) {
        try {
          return new BinaryLargeObjectInputStream(getBlobs(), v);
        } catch (IOException e) {
          throw new KvdException("failed to open blob stream", e);
        }
      } else {
        throw new KvdException("invalid value type " + v.getType());
      }
    } else {
      return null;
    }
  }

  synchronized boolean contains(Key key) {
    Value v = getValue(key);
    if(v!=null) {
      v.setAccessed(Value.now());
      map.put(key.getBytes(), v.serialize());
      expireDb.accessed(key, v.getAccessed());
      return true;
    } else {
      return false;
    }
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
    try {
      expireDb.updateAll(m);
    } catch(Exception e) {
      log.error("failed to update expire database", e);
    }
  }

  File getBlobs() {
    return blobs;
  }

}
