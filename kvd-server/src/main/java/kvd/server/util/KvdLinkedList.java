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
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addLast(E e) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean offerFirst(E e) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean offerLast(E e) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public E removeFirst() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public E removeLast() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public E pollFirst() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public E pollLast() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public E getFirst() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public E getLast() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public E peekFirst() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public E peekLast() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean removeFirstOccurrence(Object o) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean removeLastOccurrence(Object o) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void push(E e) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public E pop() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Iterator<E> descendingIterator() {
    // TODO Auto-generated method stub
    return null;
  }

  public E remove(String key) {
    // TODO lookup element by key, remove and return it (null if not found)
    return null;
  }

  public E get(String key) {
    // TODO lookup element by key and return it, null if not found
    return null;
  }

  @Override
  public int size() {
    return (int)Math.min(longSize(), Integer.MAX_VALUE);
  }

  public long longSize() {
    return listIterator(0l).size();
  }

}
