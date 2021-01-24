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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.server.storage.AbortableOutputStream;
import kvd.server.storage.StorageBackend;

public class MemStorage implements StorageBackend {

  private static final Logger log = LoggerFactory.getLogger(MemStorage.class);

  private Map<String, BinaryLargeObject> store = new HashMap<>();

  private Map<String, Staging> staging = new HashMap<>();

  private ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
  private Lock rlock = rwlock.readLock();
  private Lock wlock = rwlock.writeLock();

  @Override
  public AbortableOutputStream put(String key) {
    wlock.lock();
    try {
      String txId = UUID.randomUUID().toString();
      BinaryLargeObjectOutputStream blobStream = new BinaryLargeObjectOutputStream();
      AbortableOutputStream out = new AbortableOutputStream(
          blobStream,
          txId,
          this::commit,
          this::rollback);
      Staging staging = new Staging(key, blobStream);
      this.staging.put(txId, staging);
      log.debug("starting put, key '{}', tx '{}'", StringUtils.substring(key, 0, 200), txId);
      return out;
    } finally {
      wlock.unlock();
    }
  }

  private void commit(String txId) {
    wlock.lock();
    try {
      Staging s = staging.remove(txId);
      if(s != null) {
        BinaryLargeObject blob = s.getBlobStream().toBinaryLargeObject();
        blob.compact();
        store.put(s.getKey(), blob);
      } else {
        log.warn("unknown tx '{}'", txId);
      }
    } finally {
      wlock.unlock();
    }
  }

  private void rollback(String txId) {
    wlock.lock();
    try {
      staging.remove(txId);
    } finally {
      wlock.unlock();
    }
  }

  @Override
  public InputStream get(String key) {
    rlock.lock();
    try {
      BinaryLargeObject blob = store.get(key);
      return blob!=null?new BinaryLargeObjectInputStream(blob):null;
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public boolean contains(String key) {
    rlock.lock();
    try {
      return store.containsKey(key);
    } finally {
      rlock.unlock();
    }
  }

  @Override
  public boolean remove(String key) {
    wlock.lock();
    try {
      if(store.containsKey(key)) {
        store.remove(key);
        return true;
      } else {
        return false;
      }
    } finally {
      wlock.unlock();
    }
  }

}
