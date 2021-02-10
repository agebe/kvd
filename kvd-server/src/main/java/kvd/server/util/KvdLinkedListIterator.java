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
    throw new UnsupportedOperationException();
  }

  @Override
  public void set(E e) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(E e) {
    if(e == null) {
      throw new NullPointerException("null element not supported");
    }
    byte[] b = serializer.apply(e);
    String nodeKey = newNodeKey(e);
    // TODO blow up if the key already exists in the store!
    if(isEmpty()) {
      ListNode node = new ListNode("", "", b);
      putNode(nodeKey, node);
      setFirstPointer(nodeKey);
      setLastPointer(nodeKey);
      prev = node;
    } else if((next != null) && (prev != null)) {
        // add to middle
      throw new UnsupportedOperationException("add to middle");
    } else if(next != null) {
      // add to front
      String first = getFirstPointer();
      next.setPrev(nodeKey);
      putNode(first, next);
      setFirstPointer(nodeKey);
      ListNode node = new ListNode("", first, b);
      putNode(nodeKey, node);
    } else if(prev != null) {
      // add to end
      String last = getLastPointer();
      prev.setNext(nodeKey);
      putNode(last, prev);
      setLastPointer(nodeKey);
      ListNode node = new ListNode(last, "", b);
      putNode(nodeKey, node);
    } else {
      throw new KvdException("unexpected case");
    }
    index++;
    incSize();
  }

  private String listKey() {
    return "__kvd_list_" + name;
  }

  private String makeKey(String key) {
    return listKey() + name + "_" + key;
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
        return ListNode.deserialize(in);
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

  private boolean isEmpty() {
    return size == 0;
  }

}
