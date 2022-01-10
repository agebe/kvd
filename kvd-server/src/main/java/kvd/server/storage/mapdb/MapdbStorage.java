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

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.server.Key;
import kvd.server.storage.mapdb.expire.ExpireDb;
import kvd.server.util.FileUtils;

public class MapdbStorage {

  private static final Logger log = LoggerFactory.getLogger(MapdbStorage.class);

  private File mapdb;

  private File blobs;

  private HTreeMap<byte[], byte[]> map;

  private DB db;

  private ExpireDb expireDb;

  public MapdbStorage(File base, boolean enableMmap) {
    super();
    this.mapdb = new File(base, "mapdb");
    this.blobs = new File(base, "blobs");
    FileUtils.createDirIfMissing(mapdb);
    FileUtils.createDirIfMissing(blobs);
    // TODO test corrupted database
    // maybe add try catch, if opening fails delete the folders and try again
    // data seems to be lost so it might be better to automatically start with a clean db
    // or try to rebuild the database from the blob files
    DBMaker.Maker dbBuilder = DBMaker
        .fileDB(new File(mapdb, "map"))
        .transactionEnable()
        .closeOnJvmShutdown();
    if(enableMmap) {
      dbBuilder.fileMmapEnable();
      log.info("mapdb file mmap enabled");
    }
    db = dbBuilder.make();
    map = db
        .hashMap("map")
        .keySerializer(Serializer.BYTE_ARRAY)
        .valueSerializer(Serializer.BYTE_ARRAY)
        .modificationListener((k, ov, nv, triggered) -> {
          Key key = new Key(k);
          Value oldValue = Value.deserialize(ov);
          Value newValue = Value.deserialize(nv);
          log.trace("map modification, key {}, triggered '{}', old-value {}, new-value {}",
              key, triggered, oldValue, newValue);
          deleteBlobs(oldValue, newValue);
        })
        .createOrOpen();
    expireDb = new ExpireDb(base, enableMmap);
  }

//  private void setupExpire(HashMapMaker<byte[],byte[]> builder) {
////    executor.scheduleWithFixedDelay(this::deleteOrphanedBlobs, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
//  }

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

  public ExpireDb getExpireDb() {
    return expireDb;
  }

}
