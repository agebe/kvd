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
package kvd.test;

import kvd.client.KvdClient;

public class LargeDataTest2 {

  private static void write(KvdClient client, String key, int bytes) {
    client.putBytes(key.getBytes(), new byte[bytes]);
  }

  private static void read(KvdClient client, String key, int bytes) {
    byte[] v = client.getBytes(key.getBytes());
    if(v.length != bytes) {
      throw new RuntimeException("size mismatch");
    }
  }

  public static void main(String[] args) {
    try(KvdClient client = new KvdClient("localhost")) {
      write(client, "1024", 1024);
      write(client, "1mib", 1024*1024);
      write(client, "10mib", 10*1024*1024);
      write(client, "100mib", 100*1024*1024);
      System.out.println("write done");
      read(client, "1024", 1024);
      read(client, "1mib", 1024*1024);
      read(client, "10mib",10*1024*1024);
      read(client, "100mib",100*1024*1024);
      System.out.println("read done");
    }
    System.out.println("done");
  }
}
