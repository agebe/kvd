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
package kvd.server.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import kvd.common.Utils;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.fs.FileStorage;

public class KvdLinkedListTest {

  private static StorageBackend storage;

  @BeforeAll
  public static void setup() throws Exception {
    Path tempDirWithPrefix = Files.createTempDirectory("kvd");
    storage = new FileStorage(tempDirWithPrefix.toFile());
  }

  @AfterAll
  public static void done() {
  }

  @Test
  public void testKvdLinkedList() {
    List<String> l = new KvdLinkedList<>(storage, "test1", Utils::toUTF8, Utils::fromUTF8);
    listTest(l);
  }

  @Test
  public void testTheListTestArrayList() {
    listTest(new ArrayList<String>());
  }

  @Test
  public void testTheListTestLinkedList() {
    listTest(new LinkedList<String>());
  }


  public void listTest(List<String> l) {
    assertFalse(l.iterator().hasNext());
    assertThrows(NoSuchElementException.class, () -> l.iterator().next());
    assertEquals(0, l.size());
    l.add("s1");
    assertTrue(l.iterator().hasNext());
    assertEquals("s1", l.iterator().next());
    assertThrows(NoSuchElementException.class, () -> {
      Iterator<String> iter = l.iterator();
      iter.next();
      iter.next();
    });
    assertEquals(1, l.size());
    l.add("s2");
    assertEquals(2, l.size());
    l.add("s3");
    assertEquals(3, l.size());
    List<String> compare = Stream.of("s1", "s2", "s3").collect(Collectors.toList());
    for(String s : l) {
      String sCompare = compare.remove(0);
      assertEquals(sCompare, s);
    }
    assertEquals("s1", l.get(0));
    assertEquals("s2", l.get(1));
    assertEquals("s3", l.get(2));
    assertThrows(IndexOutOfBoundsException.class, () -> {
      l.get(-1);
    });
    assertThrows(IndexOutOfBoundsException.class, () -> {
      l.get(3);
    });
    assertThrows(IndexOutOfBoundsException.class, () -> {
      l.get(4);
    });
    {
      ListIterator<String> iter = l.listIterator();
      assertEquals(0, iter.nextIndex());
      assertEquals(-1, iter.previousIndex());
      assertEquals("s1", iter.next());
      assertEquals(1, iter.nextIndex());
      assertEquals(0, iter.previousIndex());
      assertEquals("s2", iter.next());
      assertEquals(2, iter.nextIndex());
      assertEquals(1, iter.previousIndex());
      assertEquals("s3", iter.next());
      assertEquals(3, iter.nextIndex());
      assertEquals(2, iter.previousIndex());
    }
    l.add("s4");
    l.add("s5");
    {
      ListIterator<String> iter = l.listIterator(3);
      assertEquals("s4", iter.next());
      assertEquals("s4", iter.previous());
      assertEquals("s4", iter.next());
      assertEquals(4, iter.nextIndex());
      assertEquals(3, iter.previousIndex());
    }
    {
      ListIterator<String> iter = l.listIterator(3);
      assertEquals("s3", iter.previous());
      assertEquals("s2", iter.previous());
      assertEquals("s1", iter.previous());
      assertEquals(0, iter.nextIndex());
      assertEquals(-1, iter.previousIndex());
      assertThrows(NoSuchElementException.class, () -> {
        iter.previous();
      });
      assertEquals("s1", iter.next());
    }
    assertEquals("s1", l.remove(0));
    l.set(0, "set2");
    assertEquals("set2", l.remove(0));
    assertArrayEquals(new String[] {"s3", "s4", "s5"}, l.toArray(new String[0]));
    {
      ListIterator<String> iter = l.listIterator();
      assertThrows(IllegalStateException.class, () -> {
        iter.set("foo");
      });
      assertEquals("s3", iter.next());
      iter.remove();
      assertThrows(IllegalStateException.class, () -> {
        iter.remove();
      });
      iter.next();
      iter.add("foo");
      assertThrows(IllegalStateException.class, () -> {
        iter.remove();
      });
      assertEquals("foo", iter.previous());
      iter.set("bar");
      iter.set("s45");
      iter.remove();
    }
    assertArrayEquals(new String[] {"s4", "s5"}, l.toArray(new String[0]));
    {
      ListIterator<String> iter = l.listIterator(2);
      assertEquals(2, iter.nextIndex());
      assertEquals(1, iter.previousIndex());
      iter.previous();
      assertEquals(1, iter.nextIndex());
      assertEquals(0, iter.previousIndex());
      iter.remove();
      assertEquals(1, iter.nextIndex());
      assertEquals(0, iter.previousIndex());
      iter.previous();
      assertEquals(0, iter.nextIndex());
      assertEquals(-1, iter.previousIndex());
      iter.remove();
      assertEquals(0, iter.nextIndex());
      assertEquals(-1, iter.previousIndex());
      assertTrue(l.isEmpty());
      assertArrayEquals(new String[] {}, l.toArray(new String[0]));
    }
    l.add("0");
    l.add("1");
    assertFalse(l.isEmpty());
    assertEquals(2, l.size());
    {
      ListIterator<String> iter = l.listIterator();
      assertEquals(0, iter.nextIndex());
      assertEquals(-1, iter.previousIndex());
      iter.next();
      assertEquals(1, iter.nextIndex());
      assertEquals(0, iter.previousIndex());
      iter.remove();
      assertEquals(0, iter.nextIndex());
      assertEquals(-1, iter.previousIndex());
      iter.next();
      assertEquals(1, iter.nextIndex());
      assertEquals(0, iter.previousIndex());
      iter.remove();
      assertTrue(l.isEmpty());
      assertArrayEquals(new String[] {}, l.toArray(new String[0]));
    }
    l.add("0");
    l.add("1");
    {
      ListIterator<String> iter = l.listIterator();
      assertEquals(0, iter.nextIndex());
      assertEquals(-1, iter.previousIndex());
      iter.next();
      assertEquals(1, iter.nextIndex());
      assertEquals(0, iter.previousIndex());
      iter.previous();
      assertEquals(0, iter.nextIndex());
      assertEquals(-1, iter.previousIndex());
      iter.remove();
      assertEquals(0, iter.nextIndex());
      assertEquals(-1, iter.previousIndex());
      iter.next();
      assertEquals(1, iter.nextIndex());
      assertEquals(0, iter.previousIndex());
      iter.remove();
      assertEquals(0, iter.nextIndex());
      assertEquals(-1, iter.previousIndex());
      assertTrue(l.isEmpty());
      assertArrayEquals(new String[] {}, l.toArray(new String[0]));
    }
  }

  @Test
  public void testQueueTest() {
    testQueue(new ArrayDeque<String>());
  }

  @Test
  public void testKvdLinkedListQueue() {
    Queue<String> q = new KvdLinkedList<>(storage, "queue1", Utils::toUTF8, Utils::fromUTF8);
    testQueue(q);
  }

  public void testQueue(Queue<String> q) {
    assertTrue(q.isEmpty());
    assertEquals(0, q.size());
    q.offer("0");
    q.offer("1");
    q.offer("2");
    q.add("3");
    assertFalse(q.isEmpty());
    assertEquals(4, q.size());
    assertEquals("0", q.peek());
    assertEquals("0", q.poll());
    assertEquals("1", q.element());
    assertEquals("1", q.remove());
    assertEquals("2", q.poll());
    assertEquals("3", q.poll());
    assertNull(q.peek());
    assertNull(q.poll());
    assertThrows(NoSuchElementException.class, () -> q.element());
    assertThrows(NoSuchElementException.class, () -> q.remove());
  }

}
