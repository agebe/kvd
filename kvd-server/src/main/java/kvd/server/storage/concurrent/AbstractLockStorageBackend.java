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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import kvd.common.KvdException;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.Transaction;

public abstract class AbstractLockStorageBackend implements StorageBackend {

  private StorageBackend backend;

  private LockMode mode;

  private Map<Integer, LockTransaction> transactions = new HashMap<>();

  private AtomicInteger handles = new AtomicInteger(1);

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
          }});
    // slight race here but the close listener is not called before we put the transaction into the map
    // closeTransaction also checks that the transaction is in the map
    transactions.put(tx.handle(), tx);
    return tx;
  }

  private synchronized void closeTransaction(LockTransaction tx) {
    // clear the locks first
    removeAllLocks(tx);
    LockTransaction t = transactions.remove(tx.handle());
    if(t == null) {
      throw new KvdException(String.format("missing transaction '%s'", tx.handle()));
    } else if(!tx.equals(t)) {
      throw new KvdException(String.format("handle '%s' belongs to other transaction", tx.handle()));
    }
  }

  protected abstract void removeAllLocks(LockTransaction tx);

  protected abstract void acquireWriteLock(LockTransaction tx, String key);

  protected abstract void acquireReadLock(LockTransaction tx, String key);

}
