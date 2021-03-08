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

/**
 * Manages read/write locks and stalls transactions that can't currently proceed waiting to acquire a lock.
 * This locking system also fails transactions that cause a deadlock immediately.
 */
// maintain a graph of transactions waiting for each other and fail a transaction that creates a cycle in the graph
public class PessimisticLockStorageBackend implements StorageBackend {

  @Override
  public Transaction begin() {
    // TODO Auto-generated method stub
    return null;
  }

}
