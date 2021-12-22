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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import kvd.client.KvdClient;
import kvd.server.ConcurrencyControl;
import kvd.server.Kvd;

public class ByteArrayTest {

  private static Kvd server;

  @BeforeAll
  public static void setup() throws Exception {
    server = TestUtils.startServer("info", ConcurrencyControl.NONE);
  }

  @AfterAll
  public static void done() {
    server.shutdown();
  }

  @Test
  public void test() {
    byte[] key = new byte[] {0};
    byte[] val = new byte[2*64*1024];
    try(KvdClient client = server.newLocalClient()) {
      client.putBytes(key, val);
      assertArrayEquals(val, client.getBytes(key));
      assertArrayEquals(val, client.getBytes(key));
      client.putBytes(key, val);
      assertArrayEquals(val, client.getBytes(key));
      assertArrayEquals(val, client.getBytes(key));
      client.remove(key);
    }
  }
}
