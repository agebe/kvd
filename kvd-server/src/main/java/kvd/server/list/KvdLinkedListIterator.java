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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

import com.google.common.primitives.Longs;

import kvd.common.KvdException;
import kvd.server.Key;
import kvd.server.storage.StorageBackend;
import kvd.server.storage.Transaction;

public class KvdLinkedListIterator<E> implements ListIterator<E> {

  private StorageBackend storage;

  private String name;

  private Function<E, byte[]> serializer;

  private Function<byte[], E> deserializer;

  private Function<E, Key> keyFunction;

  private long index;

  private long size;

  private ListNode prev;

  private ListNode next;

  private ListNode current;

  public KvdLinkedListIterator(StorageBackend storage,
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
    storage.withTransactionVoid(tx -> {
      initSize(tx);
      seek(tx);
    });
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
      return storage.withTransaction(tx -> {
        next = getNode(tx, current.getNext());
        index++;
        return deserializer.apply(current.getData());
      });
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
      return storage.withTransaction(tx -> {
        prev = getNode(tx, current.getPrev());
        index--;
        return deserializer.apply(current.getData());
      });
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
    storage.withTransactionVoid(tx -> removeCurrent(tx));
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
    storage.withTransactionVoid(tx -> putNode(tx, current.getKey(), current));
  }

  @Override
  public void add(E e) {
    if(e == null) {
      throw new NullPointerException("null element not supported");
    }
    byte[] b = serializer.apply(e);
    Key nodeKey = newNodeKey(e);
    storage.withTransactionVoid(tx -> {
      if(tx.contains(nodeKey)) {
        throw new DuplicateKeyException(String.format("key '%s' already in store", nodeKey));
      }
      if(isEmpty()) {
        ListNode node = new ListNode(nodeKey, null, null, b);
        putNode(tx, nodeKey, node);
        setFirstPointer(tx, nodeKey);
        setLastPointer(tx, nodeKey);
        prev = node;
      } else if((next != null) && (prev != null)) {
        next.setPrev(nodeKey);
        prev.setNext(nodeKey);
        ListNode node = new ListNode(nodeKey, prev.getKey(), next.getKey(), b);
        putNode(tx, next.getKey(), next);
        putNode(tx, prev.getKey(), prev);
        putNode(tx, nodeKey, node);
        prev = node;
      } else if(next != null) {
        // add to front
        Key first = getFirstPointer(tx);
        next.setPrev(nodeKey);
        putNode(tx, first, next);
        setFirstPointer(tx, nodeKey);
        ListNode node = new ListNode(nodeKey, null, first, b);
        putNode(tx, nodeKey, node);
      } else if(prev != null) {
        // add to end
        Key last = getLastPointer(tx);
        prev.setNext(nodeKey);
        putNode(tx, last, prev);
        setLastPointer(tx, nodeKey);
        ListNode node = new ListNode(nodeKey, last, null, b);
        putNode(tx, nodeKey, node);
      } else {
        throw new KvdException("unexpected case");
      }
      current = null;
      index++;
      incSize(tx);
    });
  }

  public E lookup(Key key) {
    Key nodeKey = newNodeKey(key);
    return storage.withTransaction(tx -> {
      ListNode node = getNode(tx, nodeKey);
      return node!=null?deserializer.apply(node.getData()):null;
    });
  }

  public E lookupRemove(Key key) {
    Key nodeKey = newNodeKey(key);
    return storage.withTransaction(tx -> {
      ListNode node = getNode(tx, nodeKey);
      if(node != null) {
        E e = deserializer.apply(node.getData());
        current = node;
        removeCurrent(tx);
        return e;
      } else {
        return null;
      }
    });
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

  private ListNode getNode(Transaction tx, Key key) {
    if(key == null) {
      return null;
    } else {
      try(InputStream in = tx.get(key)) {
        return in!=null?ListNode.deserialize(in):null;
      } catch(IOException e) {
        throw new KvdException(String.format("list get node failed for '%s'", key), e);
      }
    }
  }

  private void putNode(Transaction tx, Key key, ListNode node) {
    try(OutputStream out = tx.put(key)) {
      node.serialize(out);
    } catch(IOException e) {
      throw new KvdException(String.format("list put node failed for '%s'", key), e);
    }
  }

  private void removeNode(Transaction tx, Key key) {
    tx.remove(key);
  }

  private void setListPointer(Transaction tx, Key key, Key value) {
    if(value == null) {
      tx.remove(key);
    } else {
      tx.putBytes(key, value.getBytes());
    }
  }

  private void setFirstPointer(Transaction tx, Key nodeKey) {
    setListPointer(tx, firstKey(), nodeKey);
  }

  private void setLastPointer(Transaction tx, Key nodeKey) {
    setListPointer(tx, lastKey(), nodeKey);
  }

  private Key getListPointer(Transaction tx, Key key) {
    byte[] b = tx.getBytes(key);
    return b!=null?new Key(b):null;
  }

  private Key getFirstPointer(Transaction tx) {
    return getListPointer(tx, firstKey());
  }

  private Key getLastPointer(Transaction tx) {
    return getListPointer(tx, lastKey());
  }

  private void seek(Transaction tx) {
    if(size == 0) {
      // nothing to do
    } else if(index == 0) {
      next = getNode(tx, getFirstPointer(tx));
    } else if(index == size) {
      prev = getNode(tx, getLastPointer(tx));
    } else {
      long distanceFromFront = index;
      long distanceFromEnd = (size - index);
      if(distanceFromFront <= distanceFromEnd) {
        seekFromFront(tx);
      } else {
        seekFromEnd(tx);
      }
    }
  }

  private void seekFromFront(Transaction tx) {
    if((index < 0) || (index >= size)) {
      throw new IndexOutOfBoundsException(index+"/"+size);
    }
    next = getNode(tx, getFirstPointer(tx));
    prev = null;
    for(long i=0;i<size;i++) {
      if(i == index) {
        return;
      }
      if(next == null) {
        throw new IndexOutOfBoundsException(index+"/"+size);
      }
      prev = next;
      next = getNode(tx, next.getNext());
    }
  }

  private void seekFromEnd(Transaction tx) {
    if((index < 0) || (index >= size)) {
      throw new IndexOutOfBoundsException(index+"/"+size);
    }
    next = null;
    prev =  getNode(tx, getLastPointer(tx));
    for(long i=size;i>=0;i--) {
      if(i == index) {
        return;
      }
      if(prev == null) {
        throw new IndexOutOfBoundsException(index+"/"+size);
      }
      next = prev;
      prev = getNode(tx, prev.getPrev());
    }
  }

  private void putSize(Transaction tx, long size) {
    tx.putBytes(sizeKey(), Longs.toByteArray(size));
  }

  private Long getSize(Transaction tx) {
    byte[] b = tx.getBytes(sizeKey());
    return b!=null?Longs.fromByteArray(b):null;
  }

  private void initSize(Transaction tx) {
    Long l = getSize(tx);
    if(l == null) {
      size = 0;
      putSize(tx, size);
    } else {
      size = l;
    }
  }

  private void incSize(Transaction tx) {
    size++;
    putSize(tx, size);
  }

  private void decSize(Transaction tx) {
    size--;
    putSize(tx, size);
  }

  private boolean isEmpty() {
    return size == 0;
  }

  private void removeCurrent(Transaction tx) {
    // if the last move was next then fix the index
    if(current == prev) {
      index = Math.max(index-1, 0);
    }
    unlink(tx, current);
    current = null;
    decSize(tx);
  }

  private void unlink(Transaction tx, ListNode node) {
    if(node.isFirst() && node.isLast()) {
      if(size != 1) {
        throw new IllegalStateException("expected size to be 1 but was " + size);
      }
      setFirstPointer(tx, null);
      setLastPointer(tx, null);
      prev = null;
      next = null;
      index = 0;
    } else if(node.isFirst()) {
      ListNode n2 = getNode(tx, node.getNext());
      n2.setPrev(null);
      putNode(tx, node.getNext(), n2);
      setFirstPointer(tx, node.getNext());
      prev = null;
      next = n2;
    } else if(node.isLast()) {
      ListNode n2 = getNode(tx, node.getPrev());
      n2.setNext(null);
      putNode(tx, node.getPrev(), n2);
      setLastPointer(tx, node.getPrev());
      prev = n2;
      next = null;
    } else {
      ListNode p = getNode(tx, node.getPrev());
      ListNode n = getNode(tx, node.getNext());
      p.setNext(node.getNext());
      n.setPrev(node.getPrev());
      putNode(tx, node.getPrev(), p);
      putNode(tx, node.getNext(), n);
      prev = p;
      next = n;
    }
    removeNode(tx, node.getKey());
  }

}