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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.server.storage.StorageBackend;

public class KvdLinkedList<E> implements List<E> {

  private StorageBackend storage;

  private String first;

  private String last;

  private Function<E, byte[]> serializer;

  private Function<byte[], E> deserializer;

  public KvdLinkedList(StorageBackend storage,
      String name,
      Function<E, byte[]> serializer,
      Function<byte[], E> deserializer) {
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
    this.storage = storage;
    this.first = "__kvd_list_" + name;
    this.serializer = serializer;
    this.deserializer = deserializer;
    last = first + "_last";
  }

  private ListNode getNode(String key) {
    try(InputStream in = storage.get(key)) {
      return ListNode.deserialize(in);
    } catch(IOException e) {
      throw new KvdException(String.format("list get node failed for '%s'", key), e);
    }
  }

  private void putNode(String key, ListNode node) {
    try(OutputStream out = storage.put(key)) {
      node.serialize(out);
    } catch(IOException e) {
      throw new KvdException(String.format("list put node failed for '%s'", key), e);
    }
  }

  private String newKey() {
    return first + '_' + StringUtils.replaceChars(StringUtils.lowerCase(UUID.randomUUID().toString()), '-', '_');
  }

  private void setListPointer(String key, String nodeKey) {
    if(StringUtils.isBlank(nodeKey)) {
      storage.remove(key);
    } else {
      storage.putBytes(key, Utils.toUTF8(nodeKey));
    }
  }

  private void setFirstPointer(String nodeKey) {
    setListPointer(first, nodeKey);
  }

  private void setLastPointer(String nodeKey) {
    setListPointer(last, nodeKey);
  }

  private String getListPointer(String key) {
    byte[] b = storage.getBytes(key);
    return b!=null?Utils.fromUTF8(b):null;
  }

  private String getFirstPointer() {
    return getListPointer(first);
  }

  private String getLastPointer() {
    return getListPointer(last);
  }

  @Override
  public int size() {
    int size = 0;
    Iterator<E> iter = iterator();
    while(iter.hasNext()) {
      size++;
      iter.next();
    }
    return size;
  }

  @Override
  public boolean isEmpty() {
    return !storage.contains(first);
  }

  @Override
  public boolean contains(Object o) {
    throw new KvdException("not supported");
  }

  @Override
  public Iterator<E> iterator() {
    return new Iterator<>() {
      private ListNode current = null;

      @Override
      public boolean hasNext() {
        if(current == null) {
          return !isEmpty();
        } else {
          return StringUtils.isNotBlank(current.getNext());
        }
      }

      @Override
      public E next() {
        if(current == null) {
          if(isEmpty()) {
            throw new NoSuchElementException();
          } else {
            current = getNode(getFirstPointer());
            return deserializer.apply(current.getData());
          }
        } else {
          String next = current.getNext();
          if(StringUtils.isBlank(next)) {
            throw new NoSuchElementException();
          } else {
            current = getNode(next);
            return deserializer.apply(current.getData());
          }
        }
      }};
  }

  @Override
  public Object[] toArray() {
    throw new KvdException("not supported");
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new KvdException("not supported");
  }

  @Override
  public boolean add(E e) {
    if(e == null) {
      throw new KvdException("null element not supported");
    }
    byte[] b = serializer.apply(e);
    String nodeKey = newKey();
    if(isEmpty()) {
      ListNode node = new ListNode("", "", b);
      putNode(nodeKey, node);
      setFirstPointer(nodeKey);
      setLastPointer(nodeKey);
    } else {
      String prevLastKey = getLastPointer();
      ListNode newNode = new ListNode(prevLastKey, "", b);
      putNode(nodeKey, newNode);
      setLastPointer(nodeKey);
      ListNode prevLast = getNode(prevLastKey);
      prevLast.setNext(nodeKey);
      putNode(prevLastKey, prevLast);
    }
    return true;
  }

  @Override
  public boolean remove(Object o) {
    throw new KvdException("not supported");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new KvdException("not supported");
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    throw new KvdException("not supported");
  }

  @Override
  public boolean addAll(int index, Collection<? extends E> c) {
    throw new KvdException("not supported");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new KvdException("not supported");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new KvdException("not supported");
  }

  @Override
  public void clear() {
    throw new KvdException("not supported");
  }

  @Override
  public E get(int index) {
    if(index < 0) {
      throw new IndexOutOfBoundsException();
    }
    int i = 0;
    Iterator<E> iter = iterator();
    while(iter.hasNext()) {
      E current = iter.next();
      if(i == index) {
        return current;
      }
      i++;
    }
    throw new IndexOutOfBoundsException();
  }

  @Override
  public E set(int index, E element) {
    throw new KvdException("not supported");
  }

  @Override
  public void add(int index, E element) {
    throw new KvdException("not supported");
  }

  @Override
  public E remove(int index) {
    throw new KvdException("not supported");
  }

  @Override
  public int indexOf(Object o) {
    throw new KvdException("not supported");
  }

  @Override
  public int lastIndexOf(Object o) {
    throw new KvdException("not supported");
  }

  @Override
  public ListIterator<E> listIterator() {
    throw new KvdException("not supported");
  }

  @Override
  public ListIterator<E> listIterator(int index) {
    throw new KvdException("not supported");
  }

  @Override
  public List<E> subList(int fromIndex, int toIndex) {
    throw new KvdException("not supported");
  }

}
