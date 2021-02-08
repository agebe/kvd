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
import java.io.InputStream;
import java.io.OutputStream;

import kvd.common.KvdException;

public class ListNode {

  private String prev;

  private String next;

  private byte[] data;

  public ListNode(String prev, String next, byte[] data) {
    super();
    this.prev = prev;
    this.next = next;
    this.data = data;
  }

  public String getPrev() {
    return prev;
  }

  public void setPrev(String prev) {
    this.prev = prev;
  }

  public String getNext() {
    return next;
  }

  public void setNext(String next) {
    this.next = next;
  }

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }

  public void serialize(OutputStream out) {
    try(DataOutputStream d = new DataOutputStream(out)) {
      d.writeUTF(prev);
      d.writeUTF(next);
      d.writeInt(data.length);
      d.write(data);
    } catch(Exception e) {
      throw new KvdException("failed to serialize list node", e);
    }
  }

  public static ListNode deserialize(InputStream in) {
    try(DataInputStream d = new DataInputStream(in)) {
      String prev = d.readUTF();
      String next = d.readUTF();
      int length = d.readInt();
      byte[] data = new byte[length];
      d.read(data);
      return new ListNode(prev, next, data);
    } catch(Exception e) {
      throw new KvdException("failed to deserialize list node", e);
    }
  }

}
