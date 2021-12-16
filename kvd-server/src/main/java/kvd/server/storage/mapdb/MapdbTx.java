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

import java.io.IOException;
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

  private int blobThreshold;

  private long blobSplitThreshold;

  public MapdbTx(int handle, MapdbStorage store, int blobThreshold, long blobSplitThreshold) {
    super(handle);
    this.store = store;
    this.blobThreshold = blobThreshold;
    this.blobSplitThreshold = blobSplitThreshold;
  }

  @Override
  public synchronized AbortableOutputStream put(Key key) {
    checkClosed();
    BinaryLargeObjectOutputStream stream = new BinaryLargeObjectOutputStream(
        key,
        store.getBlobs(),
        blobThreshold,
        blobSplitThreshold);
    CompletableOutputStream out = new CompletableOutputStream(
        stream,
        this::putComplete,
        this::putAbort);
    staging.add(out);
    return out;
  }

  private synchronized void putComplete(CompletableOutputStream out) {
    staging.remove(out);
    BinaryLargeObjectOutputStream b = (BinaryLargeObjectOutputStream)out.getWrapped();
    map.put(b.getKey(), b.toValue());
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
      if(v.isInline() || v.isBlob()) {
        try {
          return new BinaryLargeObjectInputStream(store.getBlobs(), v);
        } catch (IOException e) {
          throw new KvdException("failed to open blob input stream", e);
        }
      } else if(ValueType.REMOVE.equals(vt)) {
        return null;
      } else {
        throw new KvdException("unknown value type " + vt);
      }
    } else {
      // TODO maybe store value in tx map, expire?
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
   // TODO maybe store value in tx map, expire?
      return store.contains(key);
    }
  }

  @Override
  public synchronized boolean remove(Key key) {
    checkClosed();
    boolean contains = contains(key);
    map.put(key, Value.remove());
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
