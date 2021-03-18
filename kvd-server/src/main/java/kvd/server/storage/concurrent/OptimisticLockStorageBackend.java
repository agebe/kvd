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
public class OptimisticLockStorageBackend extends AbstractLockStorageBackend {

  public OptimisticLockStorageBackend(StorageBackend backend, LockMode mode) {
    super(backend, mode);
  }

  @Override
  protected boolean canReadLockNow(LockTransaction tx, String key, Set<LockTransaction> lockHolders) {
    if(lockHolders.isEmpty()) {
      return true;
    } else {
      // check any other transaction that holds a key. only one is sufficient to check if read lock can be acquired
      LockTransaction other = lockHolders.iterator().next();
      if(other.getLock(key).equals(LockType.READ)) {
        return true;
      } else {
        throw new AcquireLockException("failed to acquire read lock (already write locked) on " + key);
      }
    }
  }

  @Override
  protected boolean canWriteLockNow(LockTransaction tx, String key, Set<LockTransaction> lockHolders) {
    if(lockHolders.isEmpty()) {
      return true;
    } else {
      throw new AcquireLockException("failed to acquire write lock (already locked) on " + key);
    }
  }

  @Override
  protected boolean canWriteLockUpgradeNow(LockTransaction tx, String key, Set<LockTransaction> lockHolders) {
    if(lockHolders.contains(tx)) {
      if(lockHolders.size() == 1) {
        return true;
      } else {
        throw new AcquireLockException("failed to upgrade to write lock, key already locked " + key);
      }
    } else {
      throw new AcquireLockException("internal error on lock upgrade, current tx does not hold lock");
    }
  }

}
