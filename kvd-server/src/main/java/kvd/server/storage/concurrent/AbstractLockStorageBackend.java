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
package kvd.server.storage.concurrent;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.common.KvdException;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.Transaction;

public abstract class AbstractLockStorageBackend implements StorageBackend {

  private static final Logger log = LoggerFactory.getLogger(AbstractLockStorageBackend.class);

  private StorageBackend backend;

  private LockMode mode;

  private Map<Integer, LockTransaction> transactions = new HashMap<>();

  private AtomicInteger handles = new AtomicInteger(1);

  private Map<String, Set<LockTransaction>> locks = new HashMap<>();

  public AbstractLockStorageBackend(StorageBackend backend, LockMode mode) {
    super();
    this.backend = backend;
    this.mode = mode;
  }

  synchronized int transactions() {
    return transactions.size();
  }

  @Override
  public synchronized Transaction begin() {
    LockTransaction tx = new LockTransaction(
        handles.getAndIncrement(),
        backend.begin(),
        this::closeTransaction,
        new LockStore() {

          @Override
          public void acquireWriteLock(LockTransaction tx, String key) {
            AbstractLockStorageBackend.this.acquireWriteLock(tx, key);
          }

          @Override
          public void acquireReadLock(LockTransaction tx, String key) {
            if(LockMode.READWRITE.equals(mode)) {
              AbstractLockStorageBackend.this.acquireReadLock(tx, key);
            }
          }},
        mode);
    // slight race here but the close listener is not called before we put the transaction into the map
    // closeTransaction also checks that the transaction is in the map
    transactions.put(tx.handle(), tx);
    return tx;
  }

  private synchronized void closeTransaction(LockTransaction tx) {
    // clear the locks first
    releaseAllLocks(tx);
    LockTransaction t = transactions.remove(tx.handle());
    if(t == null) {
      throw new KvdException(String.format("missing transaction '%s'", tx.handle()));
    } else if(!tx.equals(t)) {
      throw new KvdException(String.format("handle '%s' belongs to other transaction", tx.handle()));
    }
  }

  protected synchronized void acquireWriteLock(LockTransaction tx, String key) {
    LockType hasLock = tx.getLock(key);
    if(hasLock == null) {
      // transaction has no lock on this key yet
      while(!tx.isClosed()) {
        Set<LockTransaction> lockHolders = locks.computeIfAbsent(key, k -> new HashSet<>());
        if(canWriteLockNow(tx, key, lockHolders)) {
          lockHolders.add(tx);
          tx.putLock(key, LockType.WRITE);
          break;
        } else {
          try {
            wait();
          } catch(InterruptedException e) {
            break;
          }
        }
      }
    } else if(LockType.READ.equals(hasLock)) {
      // transaction requires a lock upgrade
      while(!tx.isClosed()) {
        Set<LockTransaction> lockHolders = locks.computeIfAbsent(key, k -> new HashSet<>());
        if(canWriteLockUpgradeNow(tx, key, lockHolders)) {
          tx.putLock(key, LockType.WRITE);
          break;
        } else {
          // TODO check deadlock before wait!
          try {
            wait();
          } catch(InterruptedException e) {
            break;
          }
        }
      }
    } else if(LockType.WRITE.equals(hasLock)) {
      // transaction already has write lock on the key, all good
    } else {
      throw new KvdException("unexpected lock type " + hasLock);
    }
  }

  protected synchronized void acquireReadLock(LockTransaction tx, String key) {
    LockType hasLock = tx.getLock(key);
    if(hasLock == null) {
      // transaction has no lock on this key yet
      while(!tx.isClosed()) {
        Set<LockTransaction> lockHolders = locks.computeIfAbsent(key, k -> new HashSet<>());
        if(canReadLockNow(tx, key, lockHolders)) {
          lockHolders.add(tx);
          tx.putLock(key, LockType.READ);
          break;
        } else {
          try {
            // TODO check deadlock before wait!
            this.wait();
          } catch(InterruptedException e) {
            break;
          }
        }
      }
    } else if(LockType.READ.equals(hasLock)) {
      // transaction already has a read lock on the key, all good
    } else if(LockType.WRITE.equals(hasLock)) {
      // transaction already has write lock on the key, all good
    } else {
      throw new KvdException("unexpected lock type " + hasLock);
    }
  }

  private synchronized void releaseAllLocks(LockTransaction tx) {
    tx.locks().keySet().forEach(key -> {
      Set<LockTransaction> s = locks.get(key);
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
    notifyAll();
  }

  synchronized int lockedKeys() {
    return locks.size();
  }

  protected abstract boolean canReadLockNow(LockTransaction tx, String key, Set<LockTransaction> lockHolders);

  protected abstract boolean canWriteLockNow(LockTransaction tx, String key, Set<LockTransaction> lockHolders);

  protected abstract boolean canWriteLockUpgradeNow(LockTransaction tx, String key, Set<LockTransaction> lockHolders);

}
