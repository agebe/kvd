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
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import kvd.common.KvdException;
import kvd.common.Utils;
import kvd.server.storage.StorageBackend;

public class KvdLinkedListIterator<E> implements ListIterator<E> {

  private StorageBackend storage;

  private String name;

  private Function<E, byte[]> serializer;

  private Function<byte[], E> deserializer;

  private Function<E, String> keyFunction;

  private long index;

  private long size;

  private ListNode prev;

  private ListNode next;

  private ListNode current;

  public KvdLinkedListIterator(StorageBackend storage,
      String name,
      Function<E, byte[]> serializer,
      Function<byte[], E> deserializer,
      Function<E, String> keyFunction,
      long index) {
    super();
    this.storage = storage;
    this.name = name;
    this.serializer = serializer;
    this.deserializer = deserializer;
    this.keyFunction = keyFunction;
    this.index = index;
    initSize();
    seek();
  }

  public long size() {
    return size;
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public E next() {
    if(next != null) {
      current = next;
      prev = current;
      next = getNode(current.getNext());
      index++;
      return deserializer.apply(current.getData());
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public boolean hasPrevious() {
    return prev != null;
  }

  @Override
  public E previous() {
    if(prev != null) {
      current = prev;
      next = current;
      prev = getNode(current.getPrev());
      index--;
      return deserializer.apply(current.getData());
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public int nextIndex() {
    return (int)Math.min(index, Integer.MAX_VALUE);
  }

  @Override
  public int previousIndex() {
    return (int)Math.min(index-1, Integer.MAX_VALUE);
  }

  @Override
  public void remove() {
    if(current == null) {
      throw new IllegalStateException();
    }
    removeCurrent();
  }

  @Override
  public void set(E e) {
    if(current == null) {
      throw new IllegalStateException();
    }
    if(e == null) {
      throw new NullPointerException("null element not supported");
    }
    byte[] b = serializer.apply(e);
    current.setData(b);
    putNode(current.getKey(), current);
  }

  @Override
  public void add(E e) {
    if(e == null) {
      throw new NullPointerException("null element not supported");
    }
    byte[] b = serializer.apply(e);
    String nodeKey = newNodeKey(e);
    if(storage.contains(nodeKey)) {
      throw new DuplicateKeyException(String.format("key '%s' already in store", nodeKey));
    }
    if(isEmpty()) {
      ListNode node = new ListNode(nodeKey, "", "", b);
      putNode(nodeKey, node);
      setFirstPointer(nodeKey);
      setLastPointer(nodeKey);
      prev = node;
    } else if((next != null) && (prev != null)) {
      next.setPrev(nodeKey);
      prev.setNext(nodeKey);
      ListNode node = new ListNode(nodeKey, prev.getKey(), next.getKey(), b);
      putNode(next.getKey(), next);
      putNode(prev.getKey(), prev);
      putNode(nodeKey, node);
      prev = node;
    } else if(next != null) {
      // add to front
      String first = getFirstPointer();
      next.setPrev(nodeKey);
      putNode(first, next);
      setFirstPointer(nodeKey);
      ListNode node = new ListNode(nodeKey, "", first, b);
      putNode(nodeKey, node);
    } else if(prev != null) {
      // add to end
      String last = getLastPointer();
      prev.setNext(nodeKey);
      putNode(last, prev);
      setLastPointer(nodeKey);
      ListNode node = new ListNode(nodeKey, last, "", b);
      putNode(nodeKey, node);
    } else {
      throw new KvdException("unexpected case");
    }
    current = null;
    index++;
    incSize();
  }

  public E lookup(String key) {
    String nodeKey = makeKey(key);
    ListNode node = getNode(nodeKey);
    return node!=null?deserializer.apply(node.getData()):null;
  }

  public E lookupRemove(String key) {
    String nodeKey = makeKey(key);
    ListNode node = getNode(nodeKey);
    if(node != null) {
      E e = deserializer.apply(node.getData());
      current = node;
      removeCurrent();
      return e;
    } else {
      return null;
    }
  }

  private String listKey() {
    return "__kvd_list_" + name;
  }

  private String makeKey(String key) {
    return listKey() + "_" + key;
  }

  private String firstKey() {
    return listKey();
  }

  private String lastKey() {
    return makeKey("last");
  }

  private String sizeKey() {
    return makeKey("size");
  }

  private String newNodeKey(E element) {
    return makeKey(keyFunction.apply(element));
  }

  private ListNode getNode(String key) {
    if(StringUtils.isBlank(key)) {
      return null;
    } else {
      try(InputStream in = storage.get(key)) {
        return in!=null?ListNode.deserialize(in):null;
      } catch(IOException e) {
        throw new KvdException(String.format("list get node failed for '%s'", key), e);
      }
    }
  }

  private void putNode(String key, ListNode node) {
    try(OutputStream out = storage.put(key)) {
      node.serialize(out);
    } catch(IOException e) {
      throw new KvdException(String.format("list put node failed for '%s'", key), e);
    }
  }

  private void removeNode(String key) {
    storage.remove(key);
  }

  private void setListPointer(String key, String value) {
    if(StringUtils.isBlank(value)) {
      storage.remove(key);
    } else {
      storage.putBytes(key, Utils.toUTF8(value));
    }
  }

  private void setFirstPointer(String nodeKey) {
    setListPointer(firstKey(), nodeKey);
  }

  private void setLastPointer(String nodeKey) {
    setListPointer(lastKey(), nodeKey);
  }

  private String getListPointer(String key) {
    byte[] b = storage.getBytes(key);
    return b!=null?Utils.fromUTF8(b):null;
  }

  private String getFirstPointer() {
    return getListPointer(firstKey());
  }

  private String getLastPointer() {
    return getListPointer(lastKey());
  }

  private void seek() {
    if(size == 0) {
      // nothing to do
    } else if(index == 0) {
      next = getNode(getFirstPointer());
    } else if(index == size) {
      prev = getNode(getLastPointer());
    } else {
      long distanceFromFront = index;
      long distanceFromEnd = (size - index);
      if(distanceFromFront <= distanceFromEnd) {
        seekFromFront();
      } else {
        seekFromEnd();
      }
    }
  }

  private void seekFromFront() {
    if((index < 0) || (index >= size)) {
      throw new IndexOutOfBoundsException(index+"/"+size);
    }
    next = getNode(getFirstPointer());
    prev = null;
    for(long i=0;i<size;i++) {
      if(i == index) {
        return;
      }
      if(next == null) {
        throw new IndexOutOfBoundsException(index+"/"+size);
      }
      prev = next;
      next = getNode(next.getNext());
    }
  }

  private void seekFromEnd() {
    if((index < 0) || (index >= size)) {
      throw new IndexOutOfBoundsException(index+"/"+size);
    }
    next = null;
    prev =  getNode(getLastPointer());
    for(long i=size;i>=0;i--) {
      if(i == index) {
        return;
      }
      if(prev == null) {
        throw new IndexOutOfBoundsException(index+"/"+size);
      }
      next = prev;
      prev = getNode(prev.getPrev());
    }
  }

  private void initSize() {
    String s = getListPointer(sizeKey());
    if(StringUtils.isBlank(s)) {
      size = 0;
      setListPointer(sizeKey(), Long.toString(size));
    } else {
      size = Long.parseLong(s);
    }
  }

  private void incSize() {
    size++;
    setListPointer(sizeKey(), Long.toString(size));
  }

  private void decSize() {
    size--;
    setListPointer(sizeKey(), Long.toString(size));
  }

  private boolean isEmpty() {
    return size == 0;
  }

  private void removeCurrent() {
    // if the last move was next then fix the index
    if(current == prev) {
      index = Math.max(index-1, 0);
    }
    unlink(current);
    current = null;
    decSize();
  }

  private void unlink(ListNode node) {
    if(node.isFirst() && node.isLast()) {
      if(size != 1) {
        throw new IllegalStateException("expected size to be 1 but was " + size);
      }
      setFirstPointer(null);
      setLastPointer(null);
      prev = null;
      next = null;
      index = 0;
    } else if(node.isFirst()) {
      ListNode n2 = getNode(node.getNext());
      n2.setPrev("");
      putNode(node.getNext(), n2);
      setFirstPointer(node.getNext());
      prev = null;
      next = n2;
    } else if(node.isLast()) {
      ListNode n2 = getNode(node.getPrev());
      n2.setNext("");
      putNode(node.getPrev(), n2);
      setLastPointer(node.getPrev());
      prev = n2;
      next = null;
    } else {
      ListNode p = getNode(node.getPrev());
      ListNode n = getNode(node.getNext());
      p.setNext(node.getNext());
      n.setPrev(node.getPrev());
      putNode(node.getPrev(), p);
      putNode(node.getNext(), n);
      prev = p;
      next = n;
    }
    removeNode(node.getKey());
  }

}
