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
package kvd.server.list;

import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import com.google.common.primitives.Longs;

import kvd.common.KvdException;
import kvd.server.Key;

public class KvdLinkedListIterator<E> implements ListIterator<E> {

  private KvdListStore storage;

  private String name;

  private Function<E, byte[]> serializer;

  private Function<byte[], E> deserializer;

  private Function<E, Key> keyFunction;

  private long index;

  private long size;

  private ListNode prev;

  private ListNode next;

  private ListNode current;

  public KvdLinkedListIterator(KvdListStore storage,
      String name,
      Function<E, byte[]> serializer,
      Function<byte[], E> deserializer,
      Function<E, Key> keyFunction,
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
    Key nodeKey = newNodeKey(e);
    if(storage.contains(nodeKey)) {
      throw new DuplicateKeyException(String.format("key '%s' already in store", nodeKey));
    }
    if(isEmpty()) {
      ListNode node = new ListNode(nodeKey, null, null, b);
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
      Key first = getFirstPointer();
      next.setPrev(nodeKey);
      putNode(first, next);
      setFirstPointer(nodeKey);
      ListNode node = new ListNode(nodeKey, null, first, b);
      putNode(nodeKey, node);
    } else if(prev != null) {
      // add to end
      Key last = getLastPointer();
      prev.setNext(nodeKey);
      putNode(last, prev);
      setLastPointer(nodeKey);
      ListNode node = new ListNode(nodeKey, last, null, b);
      putNode(nodeKey, node);
    } else {
      throw new KvdException("unexpected case");
    }
    current = null;
    index++;
    incSize();
  }

  public E lookup(Key key) {
    Key nodeKey = newNodeKey(key);
    ListNode node = getNode(nodeKey);
    return node!=null?deserializer.apply(node.getData()):null;
  }

  public E lookupRemove(Key key) {
    Key nodeKey = newNodeKey(key);
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

  private Key listKey() {
    return Key.of("__kvd_list_" + name);
  }

  private Key firstKey() {
    return listKey();
  }

  private Key lastKey() {
    return Key.of(listKey(), Key.of("_last"));
  }

  private Key sizeKey() {
    return Key.of(listKey(), Key.of("_size"));
  }

  private Key newNodeKey(Key elementKey) {
    return Key.of(Key.of(listKey(), Key.of("_node_")), elementKey);
  }

  private Key newNodeKey(E element) {
    return newNodeKey(keyFunction.apply(element));
  }

  private ListNode getNode(Key key) {
    if(key == null) {
      return null;
    } else {
      byte[] buf = storage.get(key);
      return buf!=null?ListNode.deserialize(buf):null;
    }
  }

  private void putNode(Key key, ListNode node) {
    try {
      storage.put(key, node.serialize());
    } catch(Exception e) {
      throw new KvdException(String.format("list put node failed for '%s'", key), e);
    }
  }

  private void removeNode(Key key) {
    storage.remove(key);
  }

  private void setListPointer(Key key, Key value) {
    if(value == null) {
      storage.remove(key);
    } else {
      storage.put(key, value.getBytes());
    }
  }

  private void setFirstPointer(Key nodeKey) {
    setListPointer(firstKey(), nodeKey);
  }

  private void setLastPointer(Key nodeKey) {
    setListPointer(lastKey(), nodeKey);
  }

  private Key getListPointer(Key key) {
    byte[] b = storage.get(key);
    return b!=null?new Key(b):null;
  }

  private Key getFirstPointer() {
    return getListPointer(firstKey());
  }

  private Key getLastPointer() {
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

  private void putSize(long size) {
    storage.put(sizeKey(), Longs.toByteArray(size));
  }

  private Long getSize() {
    byte[] b = storage.get(sizeKey());
    return b!=null?Longs.fromByteArray(b):null;
  }

  private void initSize() {
    Long l = getSize();
    if(l == null) {
      size = 0;
      putSize(size);
    } else {
      size = l;
    }
  }

  private void incSize() {
    size++;
    putSize(size);
  }

  private void decSize() {
    size--;
    putSize(size);
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
      n2.setPrev(null);
      putNode(node.getNext(), n2);
      setFirstPointer(node.getNext());
      prev = null;
      next = n2;
    } else if(node.isLast()) {
      ListNode n2 = getNode(node.getPrev());
      n2.setNext(null);
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
