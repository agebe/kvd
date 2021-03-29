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
import kvd.server.ConcurrencyControl;
import kvd.server.Kvd;

public class ConcurrencyControlOptRWTest {

  protected static Kvd server;

  private KvdClient client() {
    return server.newLocalClient();
  }

  @BeforeAll
  public static void setup() {
    server = TestUtils.startMemServer("warn", ConcurrencyControl.OPTRW);
  }

  @AfterAll
  public static void done() {
    server.shutdown();
  }

  @Test
  public void concurrentRead1() {
    final String key = "concurrentRead1";
    final String value = key;
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, value);
      KvdTransaction tx1 = client.beginTransaction();
      KvdTransaction tx2 = client.beginTransaction();
      assertEquals(value, tx1.getString(key));
      assertEquals(value, tx2.getString(key));
      assertTrue(tx1.contains(key));
      assertTrue(tx2.contains(key));
      tx1.rollback();
      tx2.rollback();
      assertTrue(client.remove(key));
    }
  }

  @Test
  public void concurrentReadWrite1() throws Exception {
    final String key = "concurrentReadWrite1_optrw";
    final String value1 = key;
    final String value2 = value1+"value2";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, value1);
      KvdTransaction tx1 = client.beginTransaction();
      KvdTransaction tx2 = client.beginTransaction();
      assertEquals(value1, tx1.getString(key));
      tx1.putString(key, value2);
      assertEquals(value2, tx1.getString(key));
      assertTrue(tx1.contains(key));
      assertThrows(KvdException.class, () -> tx2.getString(key));
      assertThrows(KvdException.class, () -> tx2.contains(key));
      tx1.commit();
      assertEquals(value2, tx2.getString(key));
      tx2.commit();
      assertTrue(client.remove(key));
    }
  }

  @Test
  public void concurrentReadWrite2() throws Exception {
    final String key = "concurrentReadWrite2";
    final String value1 = key;
    final String value2 = value1+"value2";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, value1);
      KvdTransaction tx1 = client.beginTransaction();
      KvdTransaction tx2 = client.beginTransaction();
      assertEquals(value1, tx1.getString(key));
      assertEquals(value1, tx2.getString(key));
      assertEquals(value1, client.getString(key));
      assertThrows(KvdException.class, () -> tx1.putString(key, value2));
      assertThrows(KvdException.class, () -> tx1.remove(key));
      assertEquals(value1, tx1.getString(key));
      assertEquals(value1, tx2.getString(key));
      tx2.commit();
      assertEquals(value1, tx1.getString(key));
      assertTrue(tx1.contains(key));
      assertEquals(value1, client.getString(key));
      assertTrue(client.contains(key));
      tx1.putString(key, value2);
      assertEquals(value2, tx1.getString(key));
      assertThrows(KvdException.class, () -> client.getString(key));
      assertThrows(KvdException.class, () -> client.contains(key));
      tx1.commit();
      assertEquals(value2, client.getString(key));
      assertTrue(client.remove(key));
    }
  }

  @Test
  public void concurrentWrite1() throws Exception {
    final String key = "concurrentWrite1";
    final String value1 = key;
    final String value2 = value1+"value2";
    final String value3 = value1+"value3";
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, value1);
      KvdTransaction tx1 = client.beginTransaction();
      KvdTransaction tx2 = client.beginTransaction();
      tx1.putString(key, value2);
      assertThrows(KvdException.class, () -> tx2.putString(key, value3));
      assertThrows(KvdException.class, () -> tx2.getString(key));
      assertThrows(KvdException.class, () -> client.putString(key, "foo"));
      assertThrows(KvdException.class, () -> client.getString(key));
      tx1.commit();
      assertEquals(value2, tx2.getString(key));
      assertEquals(value2, client.getString(key));
      tx2.putString(key, value3);
      assertEquals(value3, tx2.getString(key));
      assertThrows(KvdException.class, () -> client.getString(key));
      assertThrows(KvdException.class, () -> client.contains(key));
      assertThrows(KvdException.class, () -> client.remove(key));
      assertThrows(KvdException.class, () -> client.putString(key, "bar"));
      tx2.commit();
      assertEquals(value3, client.getString(key));
      assertTrue(client.remove(key));
    }
  }

  @Test
  public void testWriteLock1() {
    final String key = "testWriteLock1";
    final String value = key;
    try(KvdClient client = client()) {
      assertFalse(client.contains(key));
      client.putString(key, value);
      KvdTransaction tx1 = client.beginTransaction();
      KvdTransaction tx2 = client.beginTransaction();
      assertTrue(tx1.lock(key));
      assertThrows(KvdException.class, () -> tx2.lock(key));
      assertThrows(KvdException.class, () -> client.contains(key));
      tx1.putString(key, value);
      assertEquals(value, tx1.getString(key));
      tx1.commit();
      assertTrue(tx2.lock(key));
      assertThrows(KvdException.class, () -> client.contains(key));
      tx2.rollback();
      assertTrue(client.contains(key));
    }
  }

}
