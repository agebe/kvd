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
package kvd.server.storage.mapdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

public class ByteBufferTest {

  @Test
  public void test() {
    ByteBuffer b = ByteBuffer.allocate(5);
    assertEquals(5, b.limit());
    assertEquals(0, b.position());
    b.put(new byte[] {1,2});
    assertEquals(5, b.limit());
    assertEquals(2, b.position());
    assertEquals(3, b.remaining());
    byte[] copy = new byte[b.position()];
    System.arraycopy(b.array(), 0, copy, 0, b.position());
    assertArrayEquals(new byte[] {1,2}, copy);
    b.flip();
    assertEquals(2, b.limit());
    assertEquals(0, b.position());
    byte[] bytes = new byte[b.limit()];
    b.get(bytes);
    assertArrayEquals(new byte[] {1,2}, bytes);
  }
}
