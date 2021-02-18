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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;

import org.junit.jupiter.api.Test;

import kvd.server.storage.mem.MemStorageBackend;

public class OptimisticLockStorageBackendTest {

  @Test
  public void test1() {
    OptimisticLockStorageBackend backend = new OptimisticLockStorageBackend(new MemStorageBackend(),
        OptimisticLockStorageBackend.Mode.READWRITE);
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
    try(Transaction t1 = backend.begin()) {
      assertEquals(1, backend.transactions());
      assertEquals(0, backend.lockedKeys());
      assertFalse(backend.contains(t1, "key1"));
      assertEquals(1, backend.transactions());
      assertEquals(1, backend.lockedKeys());
      assertEquals(LockType.READ, ((OptimisticLockTransaction)t1).locks().get("key1"));
      backend.putBytes(t1, "key1", "foo".getBytes());
      assertEquals(LockType.WRITE, ((OptimisticLockTransaction)t1).locks().get("key1"));
      assertTrue(backend.contains(t1, "key1"));
      assertEquals(1, backend.transactions());
      assertEquals(1, backend.lockedKeys());
      t1.commit();
      assertEquals(0, backend.transactions());
      assertEquals(0, backend.lockedKeys());
    }
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
    try(Transaction t2 = backend.begin()) {
      assertTrue(backend.contains(t2, "key1"));
      assertEquals("foo", new String(backend.getBytes(t2, "key1")));
      assertTrue(backend.remove(t2, "key1"));
      assertNull(backend.getBytes(t2, "key1"));
      // no commit, so auto rollback on close
    }
    try(Transaction t3 = backend.begin()) {
      assertTrue(backend.contains(t3, "key1"));
      assertEquals("foo", new String(backend.getBytes(t3, "key1")));
      assertTrue(backend.remove(t3, "key1"));
      assertNull(backend.getBytes(t3, "key1"));
      t3.commit();
    }
    try(Transaction t4 = backend.begin()) {
      assertFalse(backend.contains(t4, "key1"));
      backend.putBytes(t4, "key1", "foo".getBytes());
      assertTrue(backend.contains(t4, "key1"));
      t4.commit();
    }
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
  }

  @Test
  public void test2() {
    OptimisticLockStorageBackend backend = new OptimisticLockStorageBackend(new MemStorageBackend(),
        OptimisticLockStorageBackend.Mode.WRITEONLY);
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
    try(Transaction t1 = backend.begin()) {
      assertEquals(1, backend.transactions());
      assertEquals(0, backend.lockedKeys());
      assertFalse(backend.contains(t1, "key1"));
      assertEquals(1, backend.transactions());
      assertEquals(0, backend.lockedKeys());
      backend.putBytes(t1, "key1", "foo".getBytes());
      assertEquals(LockType.WRITE, ((OptimisticLockTransaction)t1).locks().get("key1"));
      assertTrue(backend.contains(t1, "key1"));
      assertEquals(1, backend.transactions());
      assertEquals(1, backend.lockedKeys());
      t1.commit();
      assertEquals(0, backend.transactions());
      assertEquals(0, backend.lockedKeys());
    }
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
    try(Transaction t2 = backend.begin()) {
      assertTrue(backend.contains(t2, "key1"));
      assertEquals("foo", new String(backend.getBytes(t2, "key1")));
      assertTrue(backend.remove(t2, "key1"));
      assertNull(backend.getBytes(t2, "key1"));
      // no commit, so auto rollback on close
    }
    try(Transaction t3 = backend.begin()) {
      assertTrue(backend.contains(t3, "key1"));
      assertEquals("foo", new String(backend.getBytes(t3, "key1")));
      assertTrue(backend.remove(t3, "key1"));
      assertNull(backend.getBytes(t3, "key1"));
      t3.commit();
    }
    try(Transaction t4 = backend.begin()) {
      assertFalse(backend.contains(t4, "key1"));
      backend.putBytes(t4, "key1", "foo".getBytes());
      assertTrue(backend.contains(t4, "key1"));
      t4.commit();
    }
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
  }

  @Test
  public void test3() {
    OptimisticLockStorageBackend backend = new OptimisticLockStorageBackend(new MemStorageBackend(),
        OptimisticLockStorageBackend.Mode.READWRITE);
    Transaction t1 = backend.begin();
    Transaction t2 = backend.begin();
    backend.contains(t1, "key1");
    backend.putBytes(t2, "key2", "foo".getBytes());
    assertThrows(OptimisticLockException.class, () -> backend.putBytes(t2, "key1", "bar".getBytes()));
    backend.contains(t2, "key1");
    assertThrows(OptimisticLockException.class, () -> backend.putBytes(t1, "key1", "bar".getBytes()));
    assertEquals(2, backend.transactions());
    assertEquals(2, backend.lockedKeys());
    t2.commit();
    backend.putBytes(t1, "key1", "bar".getBytes());
    Transaction t3 = backend.begin();
    assertThrows(OptimisticLockException.class, () -> backend.contains(t3, "key1"));
    t1.commit();
    assertEquals(1, backend.transactions());
    assertEquals(0, backend.lockedKeys());
    assertTrue(backend.contains(t3, "key1"));
    t3.commit();
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
  }

  @Test
  public void test4() {
    OptimisticLockStorageBackend backend = new OptimisticLockStorageBackend(new MemStorageBackend(),
        OptimisticLockStorageBackend.Mode.WRITEONLY);
    Transaction t1 = backend.begin();
    Transaction t2 = backend.begin();
    backend.contains(t1, "key1");
    backend.putBytes(t2, "key2", "foo".getBytes());
    backend.putBytes(t2, "key1", "bar".getBytes());
    assertTrue(backend.contains(t2, "key1"));
    assertFalse(backend.contains(t1, "key1"));
    assertThrows(OptimisticLockException.class, () -> backend.putBytes(t1, "key1", "bar".getBytes()));
    assertEquals(2, backend.transactions());
    assertEquals(2, backend.lockedKeys());
    t2.commit();
    assertTrue(backend.contains(t1, "key1"));
    assertEquals("bar", new String(backend.getBytes(t1, "key1")));
    backend.putBytes(t1, "key1", "baz".getBytes());
    Transaction t3 = backend.begin();
    assertTrue(backend.contains(t3, "key1"));
    assertEquals("bar", new String(backend.getBytes(t3, "key1")));
    t1.commit();
    assertEquals("baz", new String(backend.getBytes(t3, "key1")));
    t3.commit();
    assertEquals(0, backend.transactions());
    assertEquals(0, backend.lockedKeys());
  }

  @Test
  public void test5() throws IOException {
    OptimisticLockStorageBackend backend = new OptimisticLockStorageBackend(new MemStorageBackend(),
        OptimisticLockStorageBackend.Mode.WRITEONLY);
    Transaction t1 = backend.begin();
    OutputStream out = backend.put(t1, "key1");
    out.write("foo".getBytes());
    t1.commit();
    assertThrows(IOException.class, () -> out.write("bar".getBytes()));
    Transaction t2 = backend.begin();
    assertFalse(backend.contains(t2, "key1"));
    t2.commit();
  }

}
