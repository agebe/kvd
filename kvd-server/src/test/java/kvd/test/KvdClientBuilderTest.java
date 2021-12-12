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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import kvd.client.KvdClient;
import kvd.client.KvdClientBuilder;
import kvd.common.KvdException;
import kvd.server.Kvd;

public class KvdClientBuilderTest {

  @Test
  public void test() {
    Kvd server = TestUtils.startServer();
    try(KvdClient client = new KvdClientBuilder()
        .setTransactionDefaultTimeoutMs(2500)
        .create("localhost:"+server.getLocalPort())) {
      client.putString("test", "test");
      assertTrue(client.contains("test"));
      assertThrows(KvdException.class, () -> client.withTransactionVoid(tx -> {
        assertTrue(tx.contains("test"));
        try {
          Thread.sleep(2500);
        } catch (InterruptedException e) {
          throw new RuntimeException("interrupted", e);
        }
        assertThrows(KvdException.class, () -> tx.remove("test"));
      }));
      assertTrue(client.contains("test"));
    }
    server.shutdown();
  }

}
