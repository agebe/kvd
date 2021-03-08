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

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTransaction implements Transaction {

  private static final Logger log = LoggerFactory.getLogger(AbstractTransaction.class);

  private int handle;

  private AtomicBoolean closed = new AtomicBoolean();

  public AbstractTransaction(int handle) {
    super();
    this.handle = handle;
  }

  @Override
  public synchronized void commit() {
    if(!closed.getAndSet(true)) {
      commitInternal();
    } else {
      log.debug("commit ignored, already closed");
    }
  }

  @Override
  public synchronized void rollback() {
    if(!closed.getAndSet(true)) {
      rollbackInternal();
    }
  }

  @Override
  public int handle() {
    return handle;
  }

  public boolean isClosed() {
    return closed.get();
  }

  protected abstract void commitInternal();

  protected abstract void rollbackInternal();

}
