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

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import kvd.server.Key;
import kvd.server.storage.AbortableOutputStream;
import kvd.server.storage.AbstractTransaction;
import kvd.server.storage.Transaction;

class LockTransaction extends AbstractTransaction {

  private Transaction backendTx;

  private Consumer<LockTransaction> closeListener;

  private Map<Key, LockType> locks = Collections.synchronizedMap(new HashMap<>());

  private LockStore lockStore;

  private LockMode lockMode;

  LockTransaction(int handle,
      Transaction backendTx,
      Consumer<LockTransaction> closeListener,
      LockStore lockStore,
      LockMode lockMode) {
    super(handle);
    this.backendTx = backendTx;
    this.closeListener = closeListener;
    this.lockStore = lockStore;
    this.lockMode = lockMode;
  }

  LockType getLock(Key key) {
    return locks.get(key);
  }

  void putLock(Key key, LockType lock) {
    locks.put(key, lock);
  }

  Transaction backendTx() {
    return backendTx;
  }

  Map<Key, LockType> locks() {
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

  private void checkHasWriteLock(Key key) {
    LockType l = getLock(key);
    if((l == null) || LockType.READ.equals(l)) {
      throw new LockException("check has write lock failed, " + l);
    }
  }

  private void checkHasReadLock(Key key) {
    if(LockMode.WRITEONLY.equals(lockMode)) {
      return;
    }
    LockType l = getLock(key);
    if(l == null) {
      throw new LockException("check has read lock failed");
    }
  }

  @Override
  public AbortableOutputStream<?> put(Key key) {
    checkClosed();
    lockStore.acquireWriteLock(this, key);
    checkClosed();
    checkHasWriteLock(key);
    return backendTx.put(key);
  }

  @Override
  public InputStream get(Key key) {
    checkClosed();
    lockStore.acquireReadLock(this, key);
    checkClosed();
    checkHasReadLock(key);
    return backendTx.get(key);
  }

  @Override
  public boolean contains(Key key) {
    checkClosed();
    lockStore.acquireReadLock(this, key);
    checkClosed();
    checkHasReadLock(key);
    return backendTx.contains(key);
  }

  @Override
  public boolean remove(Key key) {
    checkClosed();
    lockStore.acquireWriteLock(this, key);
    checkClosed();
    checkHasWriteLock(key);
    return backendTx.remove(key);
  }

  @Override
  public boolean lock(Key key) {
    checkClosed();
    lockStore.acquireWriteLock(this, key);
    checkClosed();
    checkHasWriteLock(key);
    return true;
  }

  public void writeLockNowOrFail(Key key) {
    checkClosed();
    lockStore.acquireWriteLockNowOrFail(this, key);
    checkClosed();
    checkHasWriteLock(key);
  }

  @Override
  public void removeAll() {
    backendTx.removeAll();
  }

  @Override
  public String toString() {
    return "tx:"+handle();
  }

}
