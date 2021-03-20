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

import java.util.Set;

import kvd.server.storage.StorageBackend;

/**
 * Manages read/write locks and stalls transactions that can't currently proceed waiting to acquire a lock.
 * This locking system also fails transactions that cause a deadlock immediately.
 */
// TODO maintain a graph of transactions waiting for each other and fail a transaction that creates a cycle in the graph
public class PessimisticLockStorageBackend extends AbstractLockStorageBackend {

  public PessimisticLockStorageBackend(StorageBackend backend, LockMode mode) {
    super(backend, mode);
  }

  @Override
  protected boolean canReadLockNow(LockTransaction tx, String key, Set<LockTransaction> lockHolders) {
    if(lockHolders.isEmpty()) {
      return true;
    } else {
      // check any other transaction that holds a key. only one is sufficient to check if read lock can be acquired
      LockTransaction other = lockHolders.iterator().next();
      return other.getLock(key).equals(LockType.READ);
    }
  }

  @Override
  protected boolean canWriteLockNow(LockTransaction tx, String key, Set<LockTransaction> lockHolders) {
    return lockHolders.isEmpty();
  }

  @Override
  protected boolean canWriteLockUpgradeNow(LockTransaction tx, String key, Set<LockTransaction> lockHolders) {
    if(lockHolders.contains(tx)) {
      return lockHolders.size() == 1;
    } else {
      throw new LockException("internal error on lock upgrade, current tx does not hold lock");
    }
  }

}