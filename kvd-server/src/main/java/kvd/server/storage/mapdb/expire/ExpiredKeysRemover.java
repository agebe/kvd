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
package kvd.server.storage.mapdb.expire;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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

  private Long expireCheckIntervalMs;

  private StorageBackend storage;

  private ExpireDb expireDb;

  private Thread removeExpiredThread;

  private List<Consumer<List<Key>>> listeners = new ArrayList<>();

  private AtomicBoolean stop = new AtomicBoolean();

  public ExpiredKeysRemover(
      Long expireAfterAccessMs,
      Long expireAfterWriteMs,
      Long expireCheckIntervalMs,
      StorageBackend storage,
      ExpireDb expireDb) {
    super();
    this.expireAfterAccessMs = expireAfterAccessMs;
    this.expireAfterWriteMs = expireAfterWriteMs;
    this.expireCheckIntervalMs = expireCheckIntervalMs;
    this.storage = storage;
    this.expireDb = expireDb;
  }

  public synchronized void start(boolean logExpired) {
    if(removeExpiredThread == null) {
      if(logExpired) {
        this.registerRemovalListener(l -> l.forEach(k -> log.info("key '{}' expired", k)));
      }
      removeExpiredThread = setupRemoveExpiredThread();
      removeExpiredThread.setDaemon(true);
      removeExpiredThread.start();
    }
  }

  public synchronized void stop() {
    if(removeExpiredThread != null) {
      stop.set(true);
      listeners.clear();
      removeExpiredThread.interrupt();
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
      long sleepMs = expireCheckIntervalMs!=null?expireCheckIntervalMs:Math.max(100, minExpireMs() / 10);
      log.info("expire after access '{}', expire after write '{}', check interval '{}'",
          HumanReadable.formatDurationOrEmpty(expireAfterAccessMs, TimeUnit.MILLISECONDS),
          HumanReadable.formatDurationOrEmpty(expireAfterWriteMs, TimeUnit.MILLISECONDS),
          HumanReadable.formatDuration(sleepMs, TimeUnit.MILLISECONDS));
      log.info("db has '{}' key/value(s)", expireDb.size());
      try {
        while(!stop.get()) {
          try {
            Thread.sleep(sleepMs);
          } catch(InterruptedException e) {
            log.info("interrupted, exiting...");
            break;
          }
          try {
            invalidateExpired();
          } catch(Throwable t) {
            log.error("failed in remove expired thread", t);
          }
        }
      } finally {
        log.info("exit");
      }
    };
    return new Thread(r, "remove-expired");
  }

  private void invalidateExpired() {
    log.trace("invalidate expired start");
    int i = 0;
    long expired = 0;
    long startNs = System.nanoTime();
    for(;;) {
      i++;
      int exp = invalidateExpiredTx();
      expired += exp;
      if(exp < EXPIRE_LIMIT_PER_TX) {
        break;
      }
    }
    String msg = "invalidate expired '{}' key/value(s), in '{}' / '{}' iteration(s), db has '{}' key/value(s)";
    if(expired > 0) {
      log.info(
          msg,
          expired,
          HumanReadable.formatDuration(System.nanoTime() - startNs, TimeUnit.NANOSECONDS),
          i,
          expireDb.size());
    } else {
      if(log.isDebugEnabled()) {
        log.debug(
            msg,
            expired,
            HumanReadable.formatDuration(System.nanoTime() - startNs, TimeUnit.NANOSECONDS),
            i,
            expireDb.size());
      }
    }
  }

  private int invalidateExpiredTx() {
    long startNs = System.nanoTime();
    List<Key> removed = storage.withTransaction(tx -> {
      Set<Key> expired = expireDb.getExpired(expireAfterAccessMs, expireAfterWriteMs, EXPIRE_LIMIT_PER_TX);
      List<Key> l = new ArrayList<>();
      expired.forEach(key -> {
        try {
          tx.writeLockNowOrFail(key);
          tx.remove(key);
          l.add(key);
        } catch(Exception e) {
          // writeLockNowOrFail throws exception if key is already locked, simply skip the key and try again later
          log.debug("remove expired key '{}' failed, try again later...", key, e);
        }
      });
      return l;
    });
    if(!removed.isEmpty()) {
      getCopyOfListeners().forEach(l -> {
        try {
          l.accept(removed);
        } catch(Exception e) {
          log.warn("exception on expire listener '{}', '{}'", l, removed, e);
        }
      });
    }
    log.trace("invalidate expired '{}' key/value(s), in '{}', continue '{}'",
        removed.size(),
        HumanReadable.formatDuration(System.nanoTime() - startNs, TimeUnit.NANOSECONDS),
        removed.size() >= EXPIRE_LIMIT_PER_TX);
    return removed.size();
  }

  public synchronized void registerRemovalListener(Consumer<List<Key>> listener) {
    listeners.add(listener);
  }

  public synchronized void unregisterRemovalListener(Consumer<List<Key>> listener) {
    listeners.remove(listener);
  }

  private synchronized List<Consumer<List<Key>>> getCopyOfListeners() {
    return new ArrayList<>(listeners);
  }

}
