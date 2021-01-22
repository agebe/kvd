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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

import kvd.client.KvdClient;
import kvd.common.KvdException;

public class LargeDataTest {

  private static String key(int mb) {
    return mb + "mb";
  }

  private static void insertLarge(int mb, KvdClient client) {
    String key = key(mb);
    System.out.println("writing key " + key);
    try(OutputStream out = client.put(key)) {
      Random r = new Random(mb);
      byte[] buf = new byte[1024 * 64];
      IntStream.range(0, mb).forEachOrdered(i -> {
        IntStream.range(0, 16).forEachOrdered(block -> {
          r.nextBytes(buf);
          try {
            out.write(buf);
          } catch(IOException e) {
            throw new KvdException("write failed", e);
          }
        });
      });
    } catch(IOException e) {
      throw new KvdException("insert failed", e);
    }
  }

  private static int readFully(InputStream in, byte[] buf) throws IOException {
    int total = 0;
    while(true) {
      int read = in.read(buf, total, buf.length-total);
      if(read == -1) {
        if(total == 0) {
          return -1;
        } else {
          return total;
        }
      } else {
        total += read;
        if(total == buf.length) {
          return total;
        }
      }
    }
  }

  private static void read(int mb, KvdClient client) {
    String key = key(mb);
    System.out.println("reading key " + key);
    try(InputStream in = client.get(key)) {
      Random r = new Random(mb);
      byte[] expectedBuf = new byte[1024 * 64];
      byte[] buf = new byte[1024 * 64];
      while(true) {
        r.nextBytes(expectedBuf);
        int read = readFully(in, buf);
        if(read == -1) {
          break;
        }
        if(!Arrays.equals(expectedBuf, 0, read, buf, 0, read)) {
          throw new KvdException("read verification failed");
        }
      }
    } catch(IOException e) {
      throw new KvdException("get failed on " + key, e);
    }
  }

  public static void main(String[] args) {
    try(KvdClient client = new KvdClient("localhost")) {
      IntStream.rangeClosed(0, 3).forEachOrdered(i -> client.remove(key((1<<i)*1024)));
      System.out.println("remove done");
      IntStream.rangeClosed(0, 3).forEachOrdered(i -> insertLarge((1<<i)*1024, client));
      System.out.println("write done");
      IntStream.rangeClosed(0, 3).forEachOrdered(i -> read((1<<i)*1024, client));
      System.out.println("read done");
    } catch(Exception e) {
      e.printStackTrace();
    }
    System.out.println("done");
  }

}
