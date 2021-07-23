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
package kvd.server.storage.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;

import org.junit.jupiter.api.Test;

import kvd.server.Key;
import kvd.server.storage.Transaction;
import kvd.server.storage.mem.MemStorageBackend;

public class OptimisticLockStorageBackendTest {

  private static final Key key1 = Key.of("key1");
  private static final Key key2 = Key.of("key2");

  @Test
  public void test1() {
    OptimisticLockStorageBackend backend = new OptimisticLockStorageBackend(new MemStorageBackend(),
        LockMode.READWRITE);
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
    try(Transaction t1 = backend.begin()) {
      assertEquals(1, backend.transactions());
      assertEquals(0, backend.lockedKeys());
      assertFalse(t1.contains(key1));
      assertEquals(1, backend.transactions());
      assertEquals(1, backend.lockedKeys());
      assertEquals(LockType.READ, ((LockTransaction)t1).locks().get(key1));
      t1.putBytes(key1, "foo".getBytes());
      assertEquals(LockType.WRITE, ((LockTransaction)t1).locks().get(key1));
      assertTrue(t1.contains(key1));
      assertEquals(1, backend.transactions());
      assertEquals(1, backend.lockedKeys());
      t1.commit();
      assertEquals(0, backend.transactions());
      assertEquals(0, backend.lockedKeys());
    }
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
    try(Transaction t2 = backend.begin()) {
      assertTrue(t2.contains(key1));
      assertEquals("foo", new String(t2.getBytes(key1)));
      assertTrue(t2.remove(key1));
      assertNull(t2.getBytes(key1));
      // no commit, so auto rollback on close
    }
    try(Transaction t3 = backend.begin()) {
      assertTrue(t3.contains(key1));
      assertEquals("foo", new String(t3.getBytes(key1)));
      assertTrue(t3.remove(key1));
      assertNull(t3.getBytes(key1));
      t3.commit();
    }
    try(Transaction t4 = backend.begin()) {
      assertFalse(t4.contains(key1));
      t4.putBytes(key1, "foo".getBytes());
      assertTrue(t4.contains(key1));
      t4.commit();
    }
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
  }

  @Test
  public void test2() {
    OptimisticLockStorageBackend backend = new OptimisticLockStorageBackend(new MemStorageBackend(),
        LockMode.WRITEONLY);
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
    try(Transaction t1 = backend.begin()) {
      assertEquals(1, backend.transactions());
      assertEquals(0, backend.lockedKeys());
      assertFalse(t1.contains(key1));
      assertEquals(1, backend.transactions());
      assertEquals(0, backend.lockedKeys());
      t1.putBytes(key1, "foo".getBytes());
      assertEquals(LockType.WRITE, ((LockTransaction)t1).locks().get(key1));
      assertTrue(t1.contains(key1));
      assertEquals(1, backend.transactions());
      assertEquals(1, backend.lockedKeys());
      t1.commit();
      assertEquals(0, backend.transactions());
      assertEquals(0, backend.lockedKeys());
    }
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
    try(Transaction t2 = backend.begin()) {
      assertTrue(t2.contains(key1));
      assertEquals("foo", new String(t2.getBytes(key1)));
      assertTrue(t2.remove(key1));
      assertNull(t2.getBytes(key1));
      // no commit, so auto rollback on close
    }
    try(Transaction t3 = backend.begin()) {
      assertTrue(t3.contains(key1));
      assertEquals("foo", new String(t3.getBytes(key1)));
      assertTrue(t3.remove(key1));
      assertNull(t3.getBytes(key1));
      t3.commit();
    }
    try(Transaction t4 = backend.begin()) {
      assertFalse(t4.contains(key1));
      t4.putBytes(key1, "foo".getBytes());
      assertTrue(t4.contains(key1));
      t4.commit();
    }
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
  }

  @Test
  public void test3() {
    OptimisticLockStorageBackend backend = new OptimisticLockStorageBackend(new MemStorageBackend(),
        LockMode.READWRITE);
    Transaction t1 = backend.begin();
    Transaction t2 = backend.begin();
    t1.contains(key1);
    t2.putBytes(key2, "foo".getBytes());
    assertThrows(LockException.class, () -> t2.putBytes(key1, "bar".getBytes()));
    t2.contains(key1);
    assertThrows(LockException.class, () -> t1.putBytes(key1, "bar".getBytes()));
    assertEquals(2, backend.transactions());
    assertEquals(2, backend.lockedKeys());
    t2.commit();
    t1.putBytes(key1, "bar".getBytes());
    Transaction t3 = backend.begin();
    assertThrows(LockException.class, () -> t3.contains(key1));
    t1.commit();
    assertEquals(1, backend.transactions());
    assertEquals(0, backend.lockedKeys());
    assertTrue(t3.contains(key1));
    t3.commit();
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
  }

  @Test
  public void test4() {
    OptimisticLockStorageBackend backend = new OptimisticLockStorageBackend(new MemStorageBackend(),
        LockMode.WRITEONLY);
    Transaction t1 = backend.begin();
    Transaction t2 = backend.begin();
    t1.contains(key1);
    t2.putBytes(key2, "foo".getBytes());
    t2.putBytes(key1, "bar".getBytes());
    assertTrue(t2.contains(key1));
    assertFalse(t1.contains(key1));
    assertThrows(LockException.class, () -> t1.putBytes(key1, "bar".getBytes()));
    assertEquals(2, backend.transactions());
    assertEquals(2, backend.lockedKeys());
    t2.commit();
    assertTrue(t1.contains(key1));
    assertEquals("bar", new String(t1.getBytes(key1)));
    t1.putBytes(key1, "baz".getBytes());
    Transaction t3 = backend.begin();
    assertTrue(t3.contains(key1));
    assertEquals("bar", new String(t3.getBytes(key1)));
    t1.commit();
    assertEquals("baz", new String(t3.getBytes(key1)));
    t3.commit();
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
  }

  @Test
  public void test5() throws IOException {
    OptimisticLockStorageBackend backend = new OptimisticLockStorageBackend(new MemStorageBackend(),
        LockMode.WRITEONLY);
    Transaction t1 = backend.begin();
    OutputStream out = t1.put(key1);
    out.write("foo".getBytes());
    t1.commit();
    assertThrows(IOException.class, () -> out.write("bar".getBytes()));
    Transaction t2 = backend.begin();
    assertFalse(t2.contains(key1));
    t2.commit();
  }

}
