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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;

import kvd.common.KvdException;
import kvd.server.Key;

public class ListNode {

  private Key key;

  private Key prev;

  private Key next;

  private byte[] data;

  public ListNode(Key key, Key prev, Key next, byte[] data) {
    super();
    this.key = key;
    this.prev = prev;
    this.next = next;
    this.data = data;
  }

  public Key getKey() {
    return key;
  }

  public Key getPrev() {
    return prev;
  }

  public void setPrev(Key prev) {
    this.prev = prev;
  }

  public Key getNext() {
    return next;
  }

  public void setNext(Key next) {
    this.next = next;
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public boolean isFirst() {
    return prev == null;
  }

  public boolean isLast() {
    return next == null;
  }

  public void serialize(OutputStream out) {
    try(DataOutputStream d = new DataOutputStream(out)) {
      writeKey(d, key);
      writeKey(d, prev);
      writeKey(d, next);
      d.writeInt(data.length);
      d.write(data);
    } catch(Exception e) {
      throw new KvdException("failed to serialize list node", e);
    }
  }

  private static void writeKey(DataOutputStream out, Key key) throws IOException {
    if(key != null) {
      byte[] b = key.getBytes();
      out.writeInt(b.length);
      out.write(b);
    } else {
      out.writeInt(0);
    }
  }

  public static ListNode deserialize(InputStream in) {
    try(DataInputStream d = new DataInputStream(in)) {
      Key key = readKey(d);
      Key prev = readKey(d);
      Key next = readKey(d);
      int length = d.readInt();
      byte[] data = new byte[length];
      d.read(data);
      return new ListNode(key, prev, next, data);
    } catch(Exception e) {
      throw new KvdException("failed to deserialize list node", e);
    }
  }

  private static Key readKey(DataInputStream in) throws IOException {
    int l = in.readInt();
    if(l <= 0) {
      return null;
    } else {
      byte[] buf = new byte[l];
      IOUtils.readFully(in, buf);
      return new Key(buf);
    }
  }

}
