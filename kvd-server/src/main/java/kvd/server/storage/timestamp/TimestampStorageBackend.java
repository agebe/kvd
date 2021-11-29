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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.server.Key;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.Transaction;
import kvd.server.util.HumanReadable;

public class TimestampStorageBackend implements StorageBackend {

  private static final Logger log = LoggerFactory.getLogger(TimestampStorageBackend.class);

  private static final int EXPIRE_LIMIT_PER_TX = 100;

  private StorageBackend backend;

  private TimestampStore store;

  private Long expireAfterAccessMs;

  private Long expireAfterWriteMs;

  private Thread removeExpiredThread;

  public TimestampStorageBackend(StorageBackend backend, Long expireAfterAccess, Long expireAfterWrite) {
    this.backend = backend;
    this.expireAfterAccessMs = expireAfterAccess;
    this.expireAfterWriteMs = expireAfterWrite;
    store = new TimestampStore(backend);
    setupRemoveExpiredThread();
  }

  @Override
  public Transaction begin() {
    return new TimestampTransaction(backend.begin(), store);
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

  private void setupRemoveExpiredThread() {
    if((expireAfterAccessMs == null) && (expireAfterWriteMs == null)) {
      log.info("keys never expire");
      return;
    }
    // do the expire check every hour...
    // but shorter interval if expire timeouts are lower (e.g. for junit tests)
    long hourMs = TimeUnit.HOURS.toMillis(1);
    long sleepMs = minExpireMs() < hourMs?1000:hourMs;
    log.info("expire after access '{}', expire after write '{}', check interval '{}'",
        HumanReadable.formatDurationOrEmpty(expireAfterAccessMs, TimeUnit.MILLISECONDS),
        HumanReadable.formatDurationOrEmpty(expireAfterWriteMs, TimeUnit.MILLISECONDS),
        HumanReadable.formatDuration(sleepMs, TimeUnit.MILLISECONDS));
    Runnable r = () -> {
      log.debug("start");
      try {
        for(;;) {
          try {
            Thread.sleep(sleepMs);
          } catch(InterruptedException e) {
            log.debug("interrupted, exiting...");
            break;
          }
          try {
            log.debug("check expired");
            invalidateExpired();
          } catch(Throwable t) {
            log.error("failed in remove expired thread", t);
          }
        }
      } finally {
        log.debug("exit");
      }
    };
    removeExpiredThread = new Thread(r, "remove-expired");
    removeExpiredThread.start();
  }

  private void invalidateExpired() {
    while(invalidateExpiredTx());
  }

  private boolean invalidateExpiredTx() {
    return this.withTransaction(tx -> {
      Set<Key> expired = store.getExpired(expireAfterAccessMs, expireAfterWriteMs, EXPIRE_LIMIT_PER_TX);
      expired.forEach(key -> {
        log.info("remove expired key '{}'", key);
        tx.remove(key);
      });
      return expired.size() >= EXPIRE_LIMIT_PER_TX;
    });
  }

}
