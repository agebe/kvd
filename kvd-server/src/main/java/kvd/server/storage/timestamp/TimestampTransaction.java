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

import java.io.InputStream;
import java.util.LinkedHashMap;

import kvd.server.Key;
import kvd.server.storage.AbortableOutputStream;
import kvd.server.storage.AbortableOutputStream2;
import kvd.server.storage.AbstractTransaction;
import kvd.server.storage.Transaction;

public class TimestampTransaction extends AbstractTransaction {

  private Transaction backendTx;

  private TimestampStore store;

  private LinkedHashMap<Key, TimestampRecord> tmap = new LinkedHashMap<>();

  public TimestampTransaction(Transaction backendTx, TimestampStore store) {
    super(backendTx.handle());
    this.backendTx = backendTx;
    this.store = store;
  }

  private synchronized void updateMap(TimestampRecord record) {
    // in case it already existed get rid of the map entry first so the put below is added last to the ordered map
    tmap.remove(record.getKey());
    tmap.put(record.getKey(), record);
  }

  @Override
  public AbortableOutputStream put(Key key) {
    AbortableOutputStream downstreamOut = backendTx.put(key);
    return new AbortableOutputStream2<Key>(
        downstreamOut,
        key,
        k -> updateMap(TimestampRecord.putTimestamps(k)),
        k -> downstreamOut.abort());
  }

  @Override
  public InputStream get(Key key) {
    InputStream in = backendTx.get(key);
    if(in != null) {
      updateMap(TimestampRecord.readTimestamps(key));
    }
    return in;
  }

  @Override
  public boolean contains(Key key) {
    boolean contains = backendTx.contains(key);
    if(contains) {
      updateMap(TimestampRecord.readTimestamps(key));
    }
    return contains;
  }

  @Override
  public boolean remove(Key key) {
    boolean removed = backendTx.remove(key);
    if(removed) {
      updateMap(TimestampRecord.removeTimestamps(key));
    }
    return removed; 
  }

  @Override
  public void removeAll() {
    backendTx.removeAll();
    tmap.clear();
  }

  @Override
  protected synchronized void commitInternal() {
    store.recordChanges(tmap.values());
    backendTx.commit();
    tmap.clear();
  }

  @Override
  protected void rollbackInternal() {
    backendTx.rollback();
    tmap.clear();
  }

}
