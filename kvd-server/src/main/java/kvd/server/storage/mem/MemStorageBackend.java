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
package kvd.server.storage.mem;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import kvd.server.storage.StorageBackend;
import kvd.server.storage.Transaction;

public class MemStorageBackend implements StorageBackend {

  private AtomicInteger txCounter = new AtomicInteger(1);

  private Map<Integer, MemTx> transactions = Collections.synchronizedMap(new HashMap<>());

  private MemStorage store = new MemStorage();

  @Override
  public Transaction begin() {
    // TODO Read committed transaction support should be OK for now. Repeatable reads or above are harder to implement 
    //   see https://en.wikipedia.org/wiki/Isolation_(database_systems)#Read_committed
    // each transaction has its own transaction store (map)
    // put operation are visible in the transaction store once the input stream has been closed
    // put operations on the same key overwrite the value of the transaction store in the order they are closed
    // get operations check transaction store first and then fallback on the global store (check also for remove object, see below)
    // contains behaves the same has get (above)
    // remove: record remove in the transaction store as a special object that gets propagated to the global store
    //   when the transaction is committed. when there is another put operation on the same key the remove object is overwritten
    //   on put close
    int txHandle = txCounter.getAndIncrement();
    MemTx tx = new MemTx(txHandle, store, () -> transactions.remove(txHandle));
    transactions.put(txHandle, tx);
    return tx;
  }

}
