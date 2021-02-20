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
package kvd.client;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import kvd.common.TransactionClosedException;

public class KvdTransaction implements KvdOperations, AutoCloseable {

  private ClientBackend backend;

  private int handle;

  private AtomicBoolean closed = new AtomicBoolean();

  @Override
  public void close() throws Exception {
    rollback();
  }

  public boolean isClosed() {
    return closed.get();
  }

  public void checkClosed() {
    if(isClosed()) {
      throw new TransactionClosedException();
    }
  }

  public synchronized void commit() {
    if(!isClosed()) {
      
    }
  }

  public synchronized void rollback() {
    if(!isClosed()) {
      
    }
  }

  @Override
  public synchronized OutputStream put(String key) {
    checkClosed();
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized Future<InputStream> getAsync(String key) {
    checkClosed();
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized Future<Boolean> containsAsync(String key) {
    checkClosed();
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized Future<Boolean> removeAsync(String key) {
    checkClosed();
    // TODO Auto-generated method stub
    return null;
  }

}
