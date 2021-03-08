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

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

class OptimisticLockTransaction extends AbstractTransaction {

  private Transaction backendTx;

  private Consumer<OptimisticLockTransaction> closeListener;

  private Map<String, LockType> locks = new HashMap<>();

  private OptimisticLockStore lockStore;

  OptimisticLockTransaction(int handle,
      Transaction backendTx,
      Consumer<OptimisticLockTransaction> closeListener,
      OptimisticLockStore lockStore) {
    super(handle);
    this.backendTx = backendTx;
    this.closeListener = closeListener;
    this.lockStore = lockStore;
  }

  LockType getLock(String key) {
    return locks.get(key);
  }

  void putLock(String key, LockType lock) {
    locks.put(key, lock);
  }

  Transaction backendTx() {
    return backendTx;
  }

  Map<String, LockType> locks() {
    return locks;
  }

  @Override
  protected void commitInternal() {
    try {
      backendTx.commit();
    } finally {
      closeListener.accept(this);
    }
  }

  @Override
  protected void rollbackInternal() {
    try {
      backendTx.rollback();
    } finally {
      closeListener.accept(this);
    }
  }

  @Override
  public AbortableOutputStream put(String key) {
    lockStore.acquireWriteLock(this, key);
    return backendTx.put(key);
  }

  @Override
  public InputStream get(String key) {
    lockStore.acquireReadLock(this, key);
    return backendTx.get(key);
  }

  @Override
  public boolean contains(String key) {
    lockStore.acquireReadLock(this, key);
    return backendTx.contains(key);
  }

  @Override
  public boolean remove(String key) {
    lockStore.acquireWriteLock(this, key);
    return backendTx.remove(key);
  }

}
