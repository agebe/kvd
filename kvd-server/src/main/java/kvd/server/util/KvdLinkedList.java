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

import java.util.AbstractSequentialList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.UUID;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import kvd.server.storage.StorageBackend;

/**
 * LinkedList implementation backed by the key/value store.
 */
// AbstractSequentialList is bases on ListIterator which is bound by positive int range (about 2 billion entries).
// This list implementation uses long size to get around the limitation on basic operations (e.g. add) but
// not all operations are working on large lists (> int range)
public class KvdLinkedList<E> extends AbstractSequentialList<E> implements List<E>, Queue<E>, Deque<E> {

  private StorageBackend storage;

  private String name;

  private Function<E, byte[]> serializer;

  private Function<byte[], E> deserializer;

  private Function<E, String> keyFunction;

  public KvdLinkedList(StorageBackend storage,
      String name,
      Function<E, byte[]> serializer,
      Function<byte[], E> deserializer) {
    this(storage,
        name,
        serializer,
        deserializer,
        element -> StringUtils.replaceChars(StringUtils.lowerCase(UUID.randomUUID().toString()), '-', '_'));
  }

  public KvdLinkedList(StorageBackend storage,
      String name,
      Function<E, byte[]> serializer,
      Function<byte[], E> deserializer,
      Function<E, String> keyFunction) {
    super();
    if(storage == null) {
      throw new NullPointerException("storage is null");
    }
    if(StringUtils.isBlank(name)) {
      throw new RuntimeException("name is blank");
    }
    if(serializer == null) {
      throw new NullPointerException("serializer is null");
    }
    if(deserializer == null) {
      throw new NullPointerException("deserializer is null");
    }
    if(keyFunction == null) {
      throw new NullPointerException("key function is null");
    }
    this.storage = storage;
    this.name = name;
    this.serializer = serializer;
    this.deserializer = deserializer;
    this.keyFunction = keyFunction;
  }

  private KvdLinkedListIterator<E> listIterator(long index) {
    return new KvdLinkedListIterator<>(
        storage,
        name,
        serializer,
        deserializer,
        keyFunction,
        index);
  }

  @Override
  public ListIterator<E> listIterator(int index) {
    return listIterator((long)index);
  }

  @Override
  public boolean add(E e) {
    listIterator(longSize()).add(e);
    return true;
  }

  @Override
  public void clear() {
    // TODO write a version that is not limited by int range?
    super.clear();
  }

  @Override
  public boolean offer(E e) {
    return add(e);
  }

  @Override
  public E remove() {
    E e = poll();
    if(e != null) {
      return e;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public E poll() {
    Iterator<E> i = iterator();
    if(i.hasNext()) {
      E head = i.next();
      i.remove();
      return head;
    } else {
      return null;
    }
  }

  @Override
  public E element() {
    E e = peek();
    if(e != null) {
      return e;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public E peek() {
    Iterator<E> i = iterator();
    return i.hasNext()?i.next():null;
  }

  @Override
  public void addFirst(E e) {
    add(0, e);
  }

  @Override
  public void addLast(E e) {
    add(e);
  }

  @Override
  public boolean offerFirst(E e) {
    add(0, e);
    return true;
  }

  @Override
  public boolean offerLast(E e) {
    addLast(e);
    return true;
  }

  @Override
  public E removeFirst() {
    return remove();
  }

  @Override
  public E removeLast() {
    E e = pollLast();
    if(e != null) {
      return e;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public E pollFirst() {
    return poll();
  }

  @Override
  public E pollLast() {
    ListIterator<E> i = listIterator(longSize());
    if(i.hasPrevious()) {
      E e = i.previous();
      i.remove();
      return e;
    } else {
      return null;
    }
  }

  @Override
  public E getFirst() {
    return element();
  }

  @Override
  public E getLast() {
    E e = peekLast();
    if(e != null) {
      return e;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public E peekFirst() {
    return peek();
  }

  @Override
  public E peekLast() {
    ListIterator<E> i = listIterator(longSize());
    return i.hasPrevious()?i.previous():null;
  }

  @Override
  public boolean removeFirstOccurrence(Object o) {
    return removeFirstIteratorOccurrence(iterator(), o);
  }

  @Override
  public boolean removeLastOccurrence(Object o) {
    return removeFirstIteratorOccurrence(descendingIterator(), o);
  }

  private boolean removeFirstIteratorOccurrence(Iterator<E> iter, Object o) {
    while(iter.hasNext()) {
      E e = iter.next();
      if(Objects.equals(o, e)) {
        iter.remove();
        return true;
      }
    }
    return false;
  }

  @Override
  public void push(E e) {
    addFirst(e);
  }

  @Override
  public E pop() {
    return removeFirst();
  }

  @Override
  public Iterator<E> descendingIterator() {
    ListIterator<E> i = listIterator(longSize());
    return new Iterator<E>() {
      @Override
      public boolean hasNext() {
        return i.hasPrevious();
      }

      @Override
      public E next() {
        return i.previous();
      }

      @Override
      public void remove() {
        i.remove();
      }
    };
  }

  public E lookup(String key) {
    return listIterator(0l).lookup(key);
  }

  public E lookupRemove(String key) {
    return listIterator(0l).lookupRemove(key);
  }

  @Override
  public int size() {
    return (int)Math.min(longSize(), Integer.MAX_VALUE);
  }

  public long longSize() {
    return listIterator(0l).size();
  }

}
