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
package kvd.server.storage.mem;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.common.TransactionClosedException;
import kvd.server.storage.AbortableOutputStream;
import kvd.server.storage.AbstractTransaction;

class MemTx extends AbstractTransaction {

  private static final Logger log = LoggerFactory.getLogger(MemTx.class);

  private MemStorage store;

  private Runnable closeListener;

  private Map<String, Staging> staging = new HashMap<>();

  private Map<String, Object> txStore = new HashMap<>();

  private ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
  private Lock rlock = rwlock.readLock();
  private Lock wlock = rwlock.writeLock();

  public MemTx(int handle, MemStorage store, Runnable closeListener) {
    super(handle);
    this.store = store;
    this.closeListener = closeListener;
  }

  @Override
  protected void commitInternal() {
    wlock.lock();
    try {
      if(!isClosed()) {
        closeListener.run();
        store.commit(txStore);
        abortUnfinishedPuts();
      } else {
        log.warn("commit ignored, already closed");
      }
    } finally {
      wlock.unlock();
    }
  }

  @Override
  protected void rollbackInternal() {
    wlock.lock();
    try {
      if(!isClosed()) {
        closeListener.run();
        abortUnfinishedPuts();
      }
    } finally {
      wlock.unlock();
    }
  }

  private void checkClosed() {
    if(isClosed()) {
      throw new TransactionClosedException();
    }
  }

  private void abortUnfinishedPuts() {
    new ArrayList<Staging>(staging.values()).forEach(staging -> {
      log.warn("aborting unfinished put '{}'", staging.getKey());
      staging.abort();
    });
    staging.clear();
  }

  AbortableOutputStream put(String key) {
    checkClosed();
    wlock.lock();
    try {
      String txId = UUID.randomUUID().toString();
      BinaryLargeObjectOutputStream blobStream = new BinaryLargeObjectOutputStream(new BinaryLargeObject(64*1024), false);
      AbortableOutputStream out = new AbortableOutputStream(
          blobStream,
          txId,
          this::putCommit,
          this::putRollback);
      Staging staging = new Staging(key, blobStream, out);
      this.staging.put(txId, staging);
      log.debug("starting put, key '{}', tx '{}'", StringUtils.substring(key, 0, 200), txId);
      return out;
    } finally {
      wlock.unlock();
    }
  }

  private void putCommit(String txId) {
    wlock.lock();
    try {
      Staging s = staging.remove(txId);
      if(s != null) {
        BinaryLargeObject blob = s.getBlobStream().toBinaryLargeObject();
        blob.compact();
        txStore.put(s.getKey(), blob);
      } else {
        log.warn("unknown tx '{}'", txId);
      }
    } finally {
      wlock.unlock();
    }
  }

  private void putRollback(String txId) {
    wlock.lock();
    try {
      staging.remove(txId);
    } finally {
      wlock.unlock();
    }
  }

  InputStream get(String key) {
    checkClosed();
    rlock.lock();
    try {
      Object o = txStore.get(key);
      if(o == null) {
        return store.get(key);
      } else if(o instanceof BinaryLargeObject) {
        BinaryLargeObject blob = (BinaryLargeObject)o;
        return new BinaryLargeObjectInputStream(blob);
      } else {
        return null;
      }
    } finally {
      rlock.unlock();
    }
  }

  boolean contains(String key) {
    checkClosed();
    rlock.lock();
    try {
      Object o = txStore.get(key);
      if(o == null) {
        return store.contains(key);
      } else if(o instanceof BinaryLargeObject) {
        return true;
      } else if(o instanceof MemStorageRemove) {
        return false;
      } else {
        throw new KvdException("unexpected case " + o);
      }
    } finally {
      rlock.unlock();
    }
  }

  public boolean remove(String key) {
    checkClosed();
    wlock.lock();
    try {
      boolean contains = contains(key);
      txStore.put(key, new MemStorageRemove(key));
      return contains;
    } finally {
      wlock.unlock();
    }
  }

}
