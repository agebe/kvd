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
import kvd.server.util.HumanReadable;

public class ExpiredKeysRemover {

  private static final Logger log = LoggerFactory.getLogger(ExpiredKeysRemover.class);

  private static final int EXPIRE_LIMIT_PER_TX = 100;

  private Long expireAfterAccessMs;

  private Long expireAfterWriteMs;

  private Long expireCheckInterval;

  private StorageBackend storage;

  private TimestampStore timestampStore;

  private Thread removeExpiredThread;

  public ExpiredKeysRemover(
      Long expireAfterAccessMs,
      Long expireAfterWriteMs,
      Long expireCheckInterval,
      StorageBackend storage,
      TimestampStore timestampStore) {
    super();
    this.expireAfterAccessMs = expireAfterAccessMs;
    this.expireAfterWriteMs = expireAfterWriteMs;
    this.expireCheckInterval = expireCheckInterval;
    this.storage = storage;
    this.timestampStore = timestampStore;
  }

  public synchronized void start() {
    if(removeExpiredThread == null) {
      removeExpiredThread = setupRemoveExpiredThread();
      removeExpiredThread.start();
    }
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

  private Thread setupRemoveExpiredThread() {
    Runnable r = () -> {
      log.debug("start");
      if((expireAfterAccessMs == null) && (expireAfterWriteMs == null)) {
        log.info("keys never expire");
        return;
      }
      long sleepMs = expireCheckInterval!=null?expireCheckInterval:Math.max(100, minExpireMs() / 10);
      log.info("expire after access '{}', expire after write '{}', check interval '{}'",
          HumanReadable.formatDurationOrEmpty(expireAfterAccessMs, TimeUnit.MILLISECONDS),
          HumanReadable.formatDurationOrEmpty(expireAfterWriteMs, TimeUnit.MILLISECONDS),
          HumanReadable.formatDuration(sleepMs, TimeUnit.MILLISECONDS));
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
    return new Thread(r, "remove-expired");
  }

  private void invalidateExpired() {
    int i = 0;
    for(;;) {
      log.trace("invalidateExpired '{}'", i);
      if(!invalidateExpiredTx()) {
        log.trace("break");
        break;
      } else {
        log.trace("loop, '{}'", i);
      }
      i++;
    }
  }

  private boolean invalidateExpiredTx() {
    return storage.withTransaction(tx -> {
      Set<Key> expired = timestampStore.getExpired(expireAfterAccessMs, expireAfterWriteMs, EXPIRE_LIMIT_PER_TX);
      expired.forEach(key -> {
        try {
          tx.writeLockNowOrFail(key);
          tx.remove(key);
          log.info("removed expired key '{}'", key);
        } catch(Exception e) {
          // writeLockNowOrFail throws exception if key is already locked, simply skip the key and try again later
          log.debug("remove expired key '{}' failed", key, e);
        }
      });
      log.trace("expired.size '{}'", expired.size());
      return expired.size() >= EXPIRE_LIMIT_PER_TX;
    });
  }

}
