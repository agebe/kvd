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
package kvd.server.storage.mem;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

public class BinaryLargeObjectTest {

  AtomicLong l = new AtomicLong();

  private void reset() {
    l.set(0);
  }

  private void nextBytes(byte[] b) {
    IntStream.range(0, b.length).forEachOrdered(i -> {
      byte v = (byte)(l.get()%10);
      b[i] = v;
      l.getAndIncrement();
    });
  }

  @Test
  public void write1() {
    reset();
    BinaryLargeObject b = new BinaryLargeObject();
    byte[] buf = new byte[17];
    IntStream.range(0, 10_000).forEachOrdered(i -> {
      nextBytes(buf);
      b.append(buf);
    });
    assertEquals(17*10_000, b.size());
    byte[] rbuf = new byte[17_000];
    byte[] ebuf = new byte[17_000];
    reset();
    nextBytes(ebuf);
    b.read(0, rbuf);
    assertArrayEquals(ebuf, rbuf);
  }

  @Test
  public void write2() {
    reset();
    BinaryLargeObject b = new BinaryLargeObject();
    byte[] buf = new byte[50_000];
    nextBytes(buf);
    b.append(buf);
    assertEquals(50_000, b.size());
    byte[] rbuf = new byte[50_000];
    byte[] ebuf = new byte[50_000];
    reset();
    nextBytes(ebuf);
    b.read(0, rbuf);
    assertArrayEquals(ebuf, rbuf);
  }

  @Test
  public void write3() {
    reset();
    BinaryLargeObject b = new BinaryLargeObject();
    byte[] buf = new byte[10];
    nextBytes(buf);
    IntStream.range(0, 10_000).forEachOrdered(i -> {
      b.append(buf, 1, 8);
    });
    assertEquals(8*10_000, b.size());
    byte[] rbuf = new byte[80_000];
    byte[] ebuf = new byte[80_000];
    byte[] a = new byte[] {1,2,3,4,5,6,7,8};
    for(int i=0;i<80_000;i++) {
      ebuf[i] = a[i%8];
    }
    b.read(0, rbuf);
    assertArrayEquals(ebuf, rbuf);
  }

  @Test
  public void read1() {
    reset();
    BinaryLargeObject b = new BinaryLargeObject();
    byte[] buf = new byte[50_000];
    nextBytes(buf);
    b.append(buf);
    assertEquals(50_000, b.size());
    byte[] rbuf = new byte[7];
    long counter = 0;
    for(int i=0;i<7142;i++) {
      b.read(i*7, rbuf);
      for(int j=0;j<rbuf.length;j++) {
        assertEquals(((counter++)%10), rbuf[j]);
      }
    }
  }

  @Test
  public void read2() {
    reset();
    BinaryLargeObject b = new BinaryLargeObject();
    byte[] buf = new byte[50_000];
    nextBytes(buf);
    b.append(buf);
    assertEquals(50_000, b.size());
    byte[] rbuf = new byte[10];
    for(int i=0;i<5000;i++) {
      b.read(i*10+1, rbuf, 1, 8);
      assertArrayEquals(new byte[] {0,1,2,3,4,5,6,7,8,0}, rbuf);
    }
  }

}
