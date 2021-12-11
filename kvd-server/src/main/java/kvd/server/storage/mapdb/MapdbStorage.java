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
import java.nio.ByteBuffer;
import java.util.Map;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.server.Key;
import kvd.server.util.FileUtils;

public class MapdbStorage {

  private static final Logger log = LoggerFactory.getLogger(MapdbStorage.class);

  private File mapdb;

  private File blobs;

  private HTreeMap<byte[], byte[]> map;

  private DB db;

  public MapdbStorage(File base) {
    super();
    this.mapdb = new File(base, "mapdb");
    this.blobs = new File(base, "blobs");
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
    map = db
        .hashMap("map")
        .keySerializer(Serializer.BYTE_ARRAY)
        .valueSerializer(Serializer.BYTE_ARRAY)
        .createOrOpen();
  }

  synchronized InputStream get(Key key) {
    return toInputStream(deserialize(map.get(key.getBytes())));
  }

  private InputStream toInputStream(Value v) {
    if(v != null) {
      ValueType vt = v.getType();
      if(ValueType.INLINE.equals(vt)) {
        return new ByteArrayInputStream(v.getValue());
      } else if(ValueType.BLOB.equals(vt)) {
        throw new KvdException("blob support not implemented yet"); 
      } else {
        throw new KvdException("invalid value type " + vt);
      }
    } else {
      return null;
    }
  }

  private Value deserialize(byte[] buf) {
    if(buf != null) {
      try {
        ByteBuffer b = ByteBuffer.wrap(buf);
        int type = b.getInt();
        ValueType vt = ValueType.values()[type];
        int len = b.getInt();
        byte[] val = new byte[len];
        b.get(val);
        return new Value(vt, val);
      } catch(Exception e) {
        throw new KvdException("failed to deserialize", e);
      }
    } else {
      return null;
    }
  }

  private byte[] serialize(Value v) {
    if(v != null) {
      ByteBuffer b = ByteBuffer.allocate(4+4+v.getValue().length);
      b.putInt(v.getType().ordinal());
      b.putInt(v.getValue().length);
      b.put(v.getValue());
      return b.array();
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
          map.put(k.getBytes(), serialize(v));
        }
      });
      db.commit();
    } catch(Throwable t) {
      log.error("failed to write key/values into mapdb, rollback transaction", t);
      db.rollback();
    }
  }

}
