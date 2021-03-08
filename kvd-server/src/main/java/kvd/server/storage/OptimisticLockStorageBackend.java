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
package kvd.server.storage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;

/**
 * This lock manager fails a operations (put, get, remove, contains) immediately
 * if it can not acquire the lock for the key. Read locks (shared) are used on get and contains operations
 * and write locks (exclusive) on put and remove operations.
 * 
 * This locking system also supports a write lock only mode so no read locks are recorded. The difference to
 * read/write mode is a lower isolation level (non-repeatable reads) but better chances for the transaction to succeed
 * and better performance.
 *
 * The downstream storage manager must also support read/write isolation.
 */
public class OptimisticLockStorageBackend implements StorageBackend {

  private static final Logger log = LoggerFactory.getLogger(OptimisticLockStorageBackend.class);

  public static enum Mode {
    READWRITE, WRITEONLY;
  }

  private StorageBackend backend;

  private Mode mode;

  private Map<Integer, OptimisticLockTransaction> transactions = new HashMap<>();

  private Map<String, Set<OptimisticLockTransaction>> locks = new HashMap<>();

  private AtomicInteger handles = new AtomicInteger(1);

  public OptimisticLockStorageBackend(StorageBackend backend, Mode mode) {
    super();
    this.backend = backend;
    this.mode = mode;
  }

  @Override
  public synchronized Transaction begin() {
    OptimisticLockTransaction tx = new OptimisticLockTransaction(
        handles.getAndIncrement(),
        backend.begin(),
        this::closeTransaction,
        new OptimisticLockStore() {

          @Override
          public void acquireWriteLock(OptimisticLockTransaction tx, String key) {
            OptimisticLockStorageBackend.this.acquireWriteLock(tx, key);
          }

          @Override
          public void acquireReadLock(OptimisticLockTransaction tx, String key) {
            OptimisticLockStorageBackend.this.acquireReadLock(tx, key);
          }});
    // slight race here but the close listener is not called before we put the transaction into the map
    // closeTransaction also checks that the transaction is in the map
    transactions.put(tx.handle(), tx);
    return tx;
  }

  private synchronized void acquireWriteLock(OptimisticLockTransaction tx, String key) {
    LockType hasLock = tx.getLock(key);
    if(hasLock == null) {
      // transaction has no lock on this key yet
      Set<OptimisticLockTransaction> set = locks.computeIfAbsent(key, k -> new HashSet<>());
      if(set.isEmpty()) {
        set.add(tx);
        tx.putLock(key, LockType.WRITE);
      } else {
        throw new OptimisticLockException("failed to acquire write lock (already locked) on " + key);
      }
    } else if(LockType.READ.equals(hasLock)) {
      // transaction requires a lock upgrade
      Set<OptimisticLockTransaction> set = locks.computeIfAbsent(key, k -> new HashSet<>());
      if(set.isEmpty()) {
        throw new OptimisticLockException("internal error, no read locks recorded"
            + " but expected read lock for this transaction");
      }
      if(set.size() > 1) {
        throw new OptimisticLockException("failed to upgrade to write lock, key already locked " + key);
      }
      if(set.contains(tx)) {
        tx.putLock(key, LockType.WRITE);
      } else {
        throw new OptimisticLockException("internal error, recorded lock from other transaction"
            + " but should be from current transaction");
      }
    } else if(LockType.WRITE.equals(hasLock)) {
      // transaction already has write lock on the key, all good
      return;
    } else {
      throw new KvdException("unexpected lock type " + hasLock);
    }
  }

  private void acquireReadLock(OptimisticLockTransaction tx, String key) {
    if(Mode.READWRITE.equals(mode)) {
      acquireReadLockSync(tx, key);
    }
  }

  private synchronized  void acquireReadLockSync(OptimisticLockTransaction tx, String key) {
    LockType hasLock = tx.getLock(key);
    if(hasLock == null) {
      // transaction has no lock on this key yet
      Set<OptimisticLockTransaction> set = locks.computeIfAbsent(key, k -> new HashSet<>());
      if(set.isEmpty()) {
        set.add(tx);
        tx.putLock(key, LockType.READ);
      } else {
        // check any other transaction that holds a key. only one is sufficient to check if read lock can be acquired
        OptimisticLockTransaction other = set.iterator().next();
        if(other.getLock(key).equals(LockType.READ)) {
          set.add(tx);
          tx.putLock(key, LockType.READ);
        } else {
          throw new OptimisticLockException("failed to acquire read lock (already write locked) on " + key);
        }
      }
    } else if(LockType.READ.equals(hasLock)) {
      // transaction already has a read lock on the key, all good
      return;
    } else if(LockType.WRITE.equals(hasLock)) {
      // transaction already has write lock on the key, all good
      return;
    } else {
      throw new KvdException("unexpected lock type " + hasLock);
    }
  }

  private synchronized void closeTransaction(OptimisticLockTransaction tx) {
    // clear the locks first
    removeAllLocks(tx);
    OptimisticLockTransaction t = transactions.remove(tx.handle());
    if(t == null) {
      throw new KvdException(String.format("missing transaction '%s'", tx.handle()));
    } else if(!tx.equals(t)) {
      throw new KvdException(String.format("handle '%s' belongs to other transaction", tx.handle()));
    }
  }

  private synchronized void removeAllLocks(OptimisticLockTransaction tx) {
    tx.locks().keySet().forEach(key -> {
      Set<OptimisticLockTransaction> s = locks.get(key);
      if(s != null) {
        if(!s.remove(tx)) {
          log.warn("lock from tx '{}'/'{}' disappeared on key '{}',"
              + " remove all locks", tx.handle(), tx.locks().get(key), key);
        }
        if(s.isEmpty()) {
          locks.remove(key);
        }
      } else {
        log.warn("lock set disappeared, remove all locks");
      }
    });
  }

  synchronized int transactions() {
    return transactions.size();
  }

  synchronized int lockedKeys() {
    return locks.size();
  }

}
