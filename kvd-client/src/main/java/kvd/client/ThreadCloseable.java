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

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadCloseable implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ThreadCloseable.class);

  private AutoCloseable closeable;

  private Thread thread;

  public ThreadCloseable() {
    super();
  }

  public ThreadCloseable(AutoCloseable closeable) {
    super();
    this.closeable = closeable;
  }

  public AutoCloseable getCloseable() {
    return closeable;
  }

  public void setCloseable(AutoCloseable closeable) {
    this.closeable = closeable;
  }

  public Thread getThread() {
    return thread;
  }

  public void setThread(Thread thread) {
    this.thread = thread;
  }

  @Override
  public void close() throws Exception {
    try {
      if(closeable != null) {
        closeable.close();
      }
    } catch(Exception e) {
      //ignore
    }
    try {
      if(thread != null) {
        log.trace("joining with thread '{}'/'{}'", thread.getName(), thread.isAlive());
        thread.join(TimeUnit.SECONDS.toMillis(5));
        log.trace("joined with thread '{}'/'{}'", thread.getName(), thread.isAlive());
        if(thread.isAlive()) {
          thread.interrupt();
        }
      }
    } catch(Exception e) {
      // ignore
    }
  }

}
