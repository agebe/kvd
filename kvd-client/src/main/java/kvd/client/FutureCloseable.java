/*
 * Copyright 2020 Andre Gebers
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

import java.util.concurrent.CompletableFuture;

import kvd.common.KvdException;

public class FutureCloseable implements AutoCloseable {

  private Thread thread;

  private CompletableFuture<?> future;

  public FutureCloseable(CompletableFuture<?> future) {
    super();
    this.future = future;
  }

  public Thread getThread() {
    return thread;
  }

  public void setThread(Thread thread) {
    this.thread = thread;
  }

  public CompletableFuture<?> getFuture() {
    return future;
  }

  public void setFuture(CompletableFuture<?> future) {
    this.future = future;
  }

  @Override
  public void close() throws Exception {
    future.completeExceptionally(new KvdException("closed"));
    ThreadCloseable tc = new ThreadCloseable();
    tc.setThread(thread);
    tc.close();
  }

}
