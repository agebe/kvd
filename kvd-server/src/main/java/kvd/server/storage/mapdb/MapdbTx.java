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
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.server.Key;
import kvd.server.storage.AbortableOutputStream;
import kvd.server.storage.AbstractTransaction;
import kvd.server.storage.CompletableOutputStream;

public class MapdbTx extends AbstractTransaction {

  private static final Logger log = LoggerFactory.getLogger(MapdbTx.class);

  private MapdbStorage store;

  private Map<Key, Value> map = new HashMap<>();

  private List<CompletableOutputStream> staging = new ArrayList<>();

  public MapdbTx(int handle, MapdbStorage store) {
    super(handle);
    this.store = store;
  }

  @Override
  public synchronized AbortableOutputStream put(Key key) {
    checkClosed();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    CompletableOutputStream out = new CompletableOutputStream(
        stream,
        o -> putComplete(o, key),
        this::putAbort);
    staging.add(out);
    return out;
  }

  private synchronized void putComplete(CompletableOutputStream out, Key k) {
    staging.remove(out);
    // TODO also support BLOBS
    ByteArrayOutputStream b = (ByteArrayOutputStream)out.getWrapped();
    Value v = new Value(ValueType.INLINE, b.toByteArray());
    map.put(k, v);
  }

  private synchronized void putAbort(CompletableOutputStream out) {
    staging.remove(out);
  }

  @Override
  public synchronized InputStream get(Key key) {
    checkClosed();
    Value v = map.get(key);
    if(v != null) {
      ValueType vt = v.getType();
      if(ValueType.INLINE.equals(vt)) {
        return new ByteArrayInputStream(v.getValue());
      } else if(ValueType.BLOB.equals(vt)) {
        // FIXME
        throw new KvdException("blob support not implemented yet");
      } else if(ValueType.REMOVE.equals(vt)) {
        return null;
      } else {
        throw new KvdException("unknown value type " + vt);
      }
    } else {
      return store.get(key);
    }
  }

  @Override
  public synchronized boolean contains(Key key) {
    checkClosed();
    Value v = map.get(key);
    if(v != null) {
      ValueType vt = v.getType();
      return !ValueType.REMOVE.equals(vt);
    } else {
      return store.contains(key);
    }
  }

  @Override
  public synchronized boolean remove(Key key) {
    checkClosed();
    boolean contains = contains(key);
    map.put(key, new Value(ValueType.REMOVE, null));
    return contains;
  }

  @Override
  public synchronized void removeAll() {
    checkClosed();
    store.removeAll();
  }

  @Override
  protected synchronized void commitInternal() {
    abortUnfinishedPuts();
    store.commit(map);
  }

  @Override
  protected synchronized void rollbackInternal() {
    abortUnfinishedPuts();
  }

  private void abortUnfinishedPuts() {
    new ArrayList<>(staging).forEach(stream -> {
      try {
        stream.abort();
      } catch(Exception e) {
        log.warn("exception on abort unfinished put, ignoring", e);
      }
    });
    staging.clear();
  }

}
