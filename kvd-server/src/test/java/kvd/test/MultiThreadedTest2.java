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
package kvd.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kvd.client.KvdClient;
import kvd.server.ConcurrencyControl;
import kvd.server.Kvd;

public class MultiThreadedTest2 {

  private static final Logger log = LoggerFactory.getLogger(MultiThreadedTest2.class);

  private Kvd server = TestUtils.startMemServer("info", ConcurrencyControl.NONE);

  private KvdClient client() {
    return server.newLocalClient();
  }

  private void testRunner(KvdClient client, int threadId) {
    log.info("test runner '{}'", threadId);
    Random r = new Random(threadId);
    byte[] buf = new byte[(threadId+1)*10];
    IntStream.range(0,1000).forEachOrdered(i -> {
      r.nextBytes(buf);
      String key = "multithreadedtest_threadid_" + threadId + "_" + i;
      String val = TestUtils.bytesToHex(buf);
      assertFalse(client.contains(key));
      client.putString(key, val);
      assertTrue(client.contains(key));
      assertEquals(val, client.getString(key));
      assertTrue(client.remove(key));
      assertFalse(client.contains(key));
    });
    log.info("test runner done '{}'", threadId);
  }

  private void multiThreadedTest() throws Exception {
    ExecutorService exec = Executors.newFixedThreadPool(50);
    log.info("start");
    List<Future<Boolean>> futures = IntStream.range(0, 50).mapToObj(i -> exec.submit(() -> {
      try(KvdClient client = client()) {
        testRunner(client, i);
      } catch(Throwable t) {
        log.error("testrunner '{}' failed", i, t);
      }
      return true;
    })).collect(Collectors.toList());
    for(Future<Boolean> f : futures) {
      f.get();
    }
    exec.shutdown();
  }

  private void run() throws Exception {
    log.info("multi thread test");
    multiThreadedTest();
    log.info("multi thread test done, shutdown");
    server.shutdown();
  }

  public static void main(String[] args) throws Exception {
    new MultiThreadedTest2().run();
    log.info("exit");
  }
}
