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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import kvd.client.KvdClient;
import kvd.client.KvdTransaction;
import kvd.common.KvdException;
import kvd.server.Kvd;

public class TxClientTest {

  protected static Kvd server;

  private KvdClient client() {
    return server.newLocalClient();
  }

  @BeforeAll
  public static void setup() {
    server = TestUtils.startMemServer();
  }

  @AfterAll
  public static void done() {
    server.shutdown();
  }

  @Test
  public void test1() throws Exception {
    final String key = "test1";
    try(KvdClient client = client()) {
      KvdTransaction tx = client.beginTransaction();
      tx.putString(key, "test");
      assertTrue(tx.contains(key));
      assertFalse(client.contains(key));
    }
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
    }
//    Thread.sleep(2000);
  }

  @Test
  public void test2() throws Exception {
    final String key = "test2";
    try(KvdClient client = client()) {
      KvdTransaction tx = client.beginTransaction();
      tx.putString(key, "test");
      assertTrue(tx.contains(key));
      assertFalse(client.contains(key));
      tx.commit();
    }
    try(KvdClient client = client()) {
      assertTrue(client.contains(key));
    }
//    Thread.sleep(2000);
  }

  @Test
  public void test3() throws Exception {
    final String key = "test3";
    try(KvdClient client = client()) {
      KvdTransaction tx = client.beginTransaction();
      tx.putString(key, "test");
      assertTrue(tx.contains(key));
      assertFalse(client.contains(key));
      tx.commit();
    }
    try(KvdClient client = client()) {
      assertTrue(client.contains(key));
      try(KvdTransaction tx = client.beginTransaction()) {
        assertTrue(client.contains(key));
        assertTrue(tx.contains(key));
        tx.remove(key);
        assertTrue(client.contains(key));
        assertFalse(tx.contains(key));
        tx.commit();
        assertFalse(client.contains(key));
      }
    }
//    Thread.sleep(2000);
  }

  @Test
  public void timeoutTest() throws Exception {
    final String key = "timeoutTest";
    try(KvdClient client = client()) {
      try(KvdTransaction tx = client.beginTransaction(250)) {
        tx.putString(key, "test");
        assertTrue(tx.contains(key));
        Thread.sleep(300);
        assertThrows(KvdException.class, () -> {
          assertTrue(tx.contains(key));
        });
      }
    }
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
    }
  }

  @Test
  public void testWithTransaction1() {
    final String key = "testWithTransaction1";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      String val = client.withTransaction(tx -> {
        tx.putString(key, key);
        return tx.getString(key);
      });
      assertEquals(key, val);
    }
  }

  @Test
  public void testWithTransactionVoid() {
    final String key = "testWithTransactionVoid";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.withTransactionVoid(tx -> {
        tx.putString(key, key);
      });
      assertEquals(key, client.getString(key));
    }
  }

  @Test
  public void testWithTransaction2() {
    // test if inner transaction joins outer transactions
    final String key = "testWithTransaction2";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      String val = client.withTransaction(tx -> {
        tx.putString(key, key);
        return getStrVal(client, key);
      });
      assertEquals(key, val);
    }
  }

  private String getStrVal(KvdClient client, String key) {
    return client.withTransaction(tx -> tx.getString(key));
  }

}
